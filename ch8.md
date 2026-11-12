# 第8章 负载均衡：四种算法和它们的代价

**一句话**：选一台机器不难，难的是为"选错"付出多少运行时成本。

**为什么需要它**：三个 provider，A 是 32 核新机器 weight=300，B/C 是 8 核 weight=100。上线后 A 的 RT 反而最高、B 被打满；再过一天扩容出 D，D 一启动就一片超时。第一个问题的答案是"你的权重没生效"，第二个的答案是"warmup 生效了，但你以为它是故障"。这一章只回答一件事：一次 `select` 里到底算了什么，每种算法为了"算得准"在进程里留下了什么状态。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `AbstractLoadBalance.select` | 空返回 null、单元素直接返回，其余交给 `doSelect` |
| `getWeight` | 方法级 `weight` 参数 + 基于 `timestamp` 的预热降权 |
| `calculateWarmupWeight` | 把 uptime 按比例折算成 `[1, weight]` |
| `RpcStatus.active` | 进程内、方法级的在途调用数，LeastActive / ShortestResponse 的数据源 |
| `WeightedRoundRobin.current` | 平滑轮询的"当前权值"，选完扣掉 totalWeight |
| `SlideWindowData` | 两个基线偏移量 + `avg × (active+1)` 的预估响应时间 |
| `ConsistentHashSelector` | `TreeMap<Long, Invoker>` 组成的哈希环 |
| `hash.nodes` / `hash.arguments` | 虚拟节点数（默认 160）/ 参与哈希的参数下标（默认 "0"） |

**关键代码**：

```java
// AbstractLoadBalance.java:49-52  预热降权的全部数学
static int calculateWarmupWeight(int uptime, int warmup, int weight) {
    int ww = (int) (uptime / ((float) warmup / weight));   // = uptime * weight / warmup
    return ww < 1 ? 1 : (Math.min(ww, weight));            // 下限 1，上限原权重
}

// AbstractLoadBalance.java:75-102
protected int getWeight(Invoker<?> invoker, Invocation invocation) {
    int weight;
    URL url = invoker.getUrl();
    if (invoker instanceof ClusterInvoker) {
        url = ((ClusterInvoker<?>) invoker).getRegistryUrl();      // 多注册中心时权重是注册中心维度的
    }
    if (REGISTRY_SERVICE_REFERENCE_PATH.equals(url.getServiceInterface())) {
        weight = url.getParameter(WEIGHT_KEY, DEFAULT_WEIGHT);     // 注册中心自身被引用：权重在 url 上
    } else {
        weight = url.getMethodParameter(RpcUtils.getMethodName(invocation), WEIGHT_KEY, DEFAULT_WEIGHT);
        if (weight > 0) {
            long timestamp = invoker.getUrl().getParameter(TIMESTAMP_KEY, 0L);  // provider 注册时写入的启动时间
            if (timestamp > 0L) {
                long uptime = System.currentTimeMillis() - timestamp;
                if (uptime < 0) {
                    return 1;                                  // 时钟不同步：当成"最不可信"，直接给最低权重
                }
                int warmup = invoker.getUrl().getParameter(WARMUP_KEY, DEFAULT_WARMUP); // 默认 10 分钟
                if (uptime > 0 && uptime < warmup) {
                    weight = calculateWarmupWeight((int) uptime, warmup, weight);
                }
            }
        }
    }
    return Math.max(weight, 0);
}
```

```java
// RoundRobinLoadBalance.java:102-129  平滑加权轮询的全部状态机
for (Invoker<T> invoker : invokers) {
    String identifyString = invoker.getUrl().toIdentityString();
    int weight = getWeight(invoker, invocation);
    WeightedRoundRobin weightedRoundRobin = ConcurrentHashMapUtils.computeIfAbsent(map, identifyString, k -> {
        WeightedRoundRobin wrr = new WeightedRoundRobin();
        wrr.setWeight(weight);
        return wrr;
    });
    if (weight != weightedRoundRobin.getWeight()) {
        weightedRoundRobin.setWeight(weight);          // 权重变了会把 current 归零，见"容易误解"
    }
    long cur = weightedRoundRobin.increaseCurrent();   // current += weight
    weightedRoundRobin.setLastUpdate(now);
    if (cur > maxCurrent) {                            // 贪心取当前最大
        maxCurrent = cur;
        selectedInvoker = invoker;
        selectedWRR = weightedRoundRobin;
    }
    totalWeight += weight;
}
if (invokers.size() != map.size()) {                   // 地址数变了才做一次清理
    map.entrySet().removeIf(item -> now - item.getValue().getLastUpdate() > RECYCLE_PERIOD); // 60s
}
if (selectedInvoker != null) {
    selectedWRR.sel(totalWeight);                      // current -= totalWeight
    return selectedInvoker;
}
```

**拆解**：

### 1. 入口只有三行，权重却有一整套降级逻辑

`select` 本身短得没什么可讲：空返回 `null`、`size() == 1` 直接返回、其余交给 `doSelect`。单元素短路不是省事，是省算力——只有一个候选时不该为它建 `weights` 数组、建哈希环、或者读 `RpcStatus`。

真正有信息量的是 `getWeight` 的三个分支。第一，`invoker instanceof ClusterInvoker` 时取 `getRegistryUrl()`——多注册中心场景下，权重表达的是"这个注册中心整体该分多少流量"，不是某台 provider 的配置。第二，`REGISTRY_SERVICE_REFERENCE_PATH` 特判，此时权重挂在 url 上而非方法参数上。第三才是正常路径：`getMethodParameter(methodName, "weight", 100)`，注意是**方法级**参数，`@DubboReference(methods = @Method(name = "x", weight = 200))` 才能命中，配在接口级只是兜底。

预热降权是大多数人不知道的那段。`timestamp` 取自 `invoker.getUrl()`（provider 注册时写入的进程启动时间），`uptime = now - timestamp`，`warmup` 默认 `DEFAULT_WARMUP = 10 * 60 * 1000`。一个 weight=300 的实例，启动 1 分钟时权重只有 30，5 分钟 150，满 10 分钟才回到 300。理由很物理：JVM 刚起来，JIT 没编译、连接池是空的、本地缓存冷的，按满权重灌流量等于拿它做压力测试。

`calculateWarmupWeight` 就是一行比例折算 `uptime * weight / warmup`，再 clamp 到 `[1, weight]`——下限 1 保证新实例不会被彻底饿死，上限保证不会超过配置值。

有个反直觉分支值得单独记：`uptime < 0` 时直接 `return 1`。消费者时钟比提供者慢时算出来是负数，Dubbo 不认为"刚启动"，而是当成"时间戳不可信"处理，直接给最低权重。这是把时钟漂移当成未知状态，代价是——一台时钟快 20 分钟的机器，在别人眼里永远是权重 1。

还要注意它是**每次 select 都重算**的，不是启动时算一次存起来。所以权重是一条随时间上升的曲线，不是阶跃。

### 2. Random 与 RoundRobin：一个无状态，一个要记账

`RandomLoadBalance` 在 3.3 里多了一个 `needWeightLoadBalance` 短路：如果第一个 invoker 的 url 上既没有显式 `weight` 参数、也没有 `timestamp`，直接 `ThreadLocalRandom.current().nextInt(length)` 均匀随机，连 `weights` 数组都不建。这解释了为什么"没配权重的集群"里 Random 便宜得几乎免费。

为什么是 `ThreadLocalRandom` 而不是 `Random`：`java.util.Random.next()` 的 seed 存在一个 `AtomicLong` 里，每次取随机数都是一次 CAS，高并发下 CAS 失败重试，seed 变成全局竞争点。`ThreadLocalRandom` 把 seed 放在 `Thread` 对象字段里，`nextInt` 只读写本线程内存，无竞争、无伪共享。负载均衡是每次 RPC 都走的路径，这个差别在几万 QPS 下是能被 profiler 抓出来的。

权重区间法的实现细节比想象中讲究：`weights[i]` 存的不是第 i 个 invoker 的权重，而是**前缀和**（`totalWeight += weight; weights[i] = totalWeight;`），同时用 `totalWeight != weight * (i + 1)` 顺手判断"是不是全都一样重"。选点就变成"在 `[0, totalWeight)` 取一个 offset，找第一个前缀和大于 offset 的下标"。`length <= 4` 线性扫，`> 4` 走 `Arrays.binarySearch`。

二分那段有个只有读源码才会注意的点：命中时（`offset` 恰好等于某个前缀和）要 `while (weights[i + 1] == offset) i++;` 再 `i++`，因为区间是左闭右开、offset 落在相等前缀和的最后一个上。而这里不会数组越界，理由是 `nextInt(totalWeight)` 保证 `offset < totalWeight = weights[length - 1]`，所以精确命中点永远不可能是最后一个元素。一个看起来会崩的 `weights[i + 1]`，被上一行的取值范围救了。

`RoundRobinLoadBalance` 在 3.x 已经完全不是"取模"了。2.6 那套 `AtomicLong sequence` + `mod` 的问题不在正确性，在**突发**：A=5、B=1、C=1 时，如果按 `totalWeight` 取模再映射区间，序列是 A,A,A,A,A,B,C,...，前五个请求全砸在 A 上，B/C 要等到第六第七个才出现。按 `length` 取模则更糟——权重白配了。

3.3 用的是平滑加权轮询（和 nginx 的 smooth WRR 同构）：`methodWeightMap` 是 `Map<serviceKey.method, Map<identityString, WeightedRoundRobin>>`，每个节点一个 `current`。每轮所有节点 `current += weight`，取最大者，被选中的 `current -= totalWeight`。A=5、B=1、C=1 走一遍：

`(0,0,0) → (5,1,1) 选 A → (-2,1,1) → (3,2,2) 选 A → (-4,2,2) → (1,3,3) 选 B → (1,-4,3) → (6,-3,4) 选 A → (-1,-3,4) → (4,-2,5) 选 C → (4,-2,-2) → (9,-1,-1) 选 A → (2,-1,-1) → (7,0,0) 选 A → (0,0,0)`。

七次一轮，序列 A,A,B,A,C,A,A。A 拿 5 次，但被均匀插开，最大连续只有 2 次。这就是"平滑"的含义：比例对，且短窗口内也均匀。

两个实现细节要记牢。一是 `current` 虽然是 `AtomicLong`，但"遍历所有节点读 current → 比较 → `sel`"这整段不是原子的，并发下两个线程可能同时认为同一个节点最大。Dubbo 接受这个近似：`sel` 会扣减，下一轮自然纠正，代价是极端并发下比例不精确。二是 map 的回收：只有 `invokers.size() != map.size()` 时才 `removeIf` 掉 `lastUpdate` 超过 `RECYCLE_PERIOD`（60000ms）的条目，这是地址下线后 WRR 状态的清理路径，不加就是内存泄漏。

### 3. LeastActive 与 ShortestResponse：同一套骨架，两种"谁更空"

两者的代码结构几乎逐行对应：一个 `leastIndexes/shortestIndexes` 数组、一个 `leastCount/shortestCount`、一个 `sameWeight` 标志，结尾三步也一模一样——唯一候选直接返回；权重不同按权重随机；权重相同均匀随机。差别只在"用什么指标排序"。

LeastActive 的指标是 `RpcStatus.getStatus(invoker.getUrl(), methodName).getActive()`。这个 `active` 由 `beginCount` / `endCount` 维护：`beginCount` 对 method 级 status 做 CAS 自增（带 `max` 上限判断，超限返回 false，被 `ActiveLimitFilter` 用来拒绝请求），再对 service 级 status 无条件 `incrementAndGet`；`endCount` 对两级都 `decrementAndGet`，顺带累加 `total`、`totalElapsed`、`succeededMaxElapsed`。所以它是**本进程、本方法**此刻还没收到响应的调用数——你有 10 个消费者，每个看到的 active 都只是自己那一份，没有任何全局聚合。

`RpcStatus` 的 key 是 `url.toIdentityString()`，不是 `ip:port`。这意味着同一台机器上的同一个方法，如果因为 `version`、`group` 或某个参数不同被 refer 成两个 invoker，就会落到两个独立的 `RpcStatus` 上，各自的 active 互不相加。排查"活跃数明显偏低"时，先确认地址有没有被参数拆成多份。

为什么活跃数相同还要再随机？因为冷启动或低 QPS 时所有实例 active 都是 0。如果按列表顺序取第一个满足条件的，流量会全压在第一台，第二台永远拿不到请求、永远 active=0、永远排在后面——正反馈直接把均衡打死。代码里 `leastCount == 1` 直接返回是安全的（只有一个最少），但只要 `leastCount > 1` 就必须用权重或随机打散。这不是锦上添花，是算法正确性的一部分。

ShortestResponse 把指标换成"预估响应时间"：`SlideWindowData.getEstimateResponse() = 窗口内成功调用平均耗时 × (active + 1)`。`+1` 是把自己这次算进去：一台正在处理 10 个请求、平均 5ms 的机器预估 55ms，一台空闲但平均 20ms 的机器预估 20ms，选后者。这比 LeastActive 多了一层"这台机器是不是本来就很慢"的判断。

"滑动窗口"这个词容易骗人。它不是时间轮，也不是环形缓冲区，而是**两个基线偏移量**：`SlideWindowData` 存 `succeededOffset` 和 `succeededElapsedOffset`，`getSucceededAverageElapsed()` 用 `(当前累计 - 基线) / (当前成功数 - 基线成功数)`，`reset()` 把当前累计值写成新基线。触发条件是 `now - lastUpdateTime > slidePeriod`（默认 `30_000`，可由 `shortestResponseSlidePeriod` 配置），并用 `AtomicBoolean onResetSlideWindow` 的 CAS 保证同一时刻只有一个线程提交，任务丢到共享 executor 异步跑。所以窗口长度是"上次 reset 到这次 select"的近似值，而且是**惰性**的——没有请求进来就不会 reset。

它有两个真实缺陷，读代码就能看出来：一是只统计成功调用（`getSucceeded` / `getSucceededElapsed` 都是 total 减 failed），一台疯狂失败但"成功的那些很快"的机器会显得最优；二是冷启动时平均耗时是 0，所有实例 estimate 都退化成由 active 主导，等价于 LeastActive。

### 4. ConsistentHash：160 个点，一次 MD5

构造 `ConsistentHashSelector` 时：`replicaNumber = hash.nodes`（默认 160），循环 `replicaNumber / 4` 次，每次对 `address + i` 做 MD5，一次 MD5 出 16 字节，`hash(digest, h)` 取 4 个 32 位值，所以 40 × 4 = 160 个环上点。**160 的来历就在这里**：不是拍脑袋的经验数，是"一次 MD5 能切出 4 个节点"这个实现细节乘以 40 次循环。改 `hash.nodes=100` 就是 25 次循环、100 个点。

虚拟节点的作用是压方差。10 台真实机器只放 10 个点时，环上的间距由哈希运气决定，某台可能独占 30% 的环；加到 160 个点后，每台的平均份额趋近 1/N，加一台机器时迁移量才接近理论的 1/(N+1)。这是 Karger 那套一致性哈希的原始论证，memcached 客户端用同样的 160。

`select` 路径：`toKey(args)` 按 `argumentIndex` 拼接参数（`hash.arguments` 默认 `"0"`，只取第一个参数），MD5 后取 `hash(digest, 0)`，`TreeMap.ceilingEntry` 顺时针找第一个 `>=` 该 hash 的节点，找不到就 `firstEntry` 回绕。环上 key 用的是 `invoker.getUrl().getAddress()`（ip:port），不是完整 url——同一台机器换端口会被当成新节点。

`hash` 本身也值得看一眼：每个字节 `& 0xFF`，最后 `& 0xFFFFFFFFL`，全程无符号扩展。Java 的 `byte` 是有符号的，直接移位会把高位置 1，环上所有点挤到负数区间，`TreeMap` 的分布就废了。这行掩码不是装饰。

selector 缓存在 `selectors` 里，key 是 `serviceKey.method`，失效判据是 `invokers.hashCode() != selector.identityHashCode`。`List.hashCode` 只看元素和顺序，所以地址列表一增删或重排就重建整个环，成本是 N × 40 次 MD5。地址频繁抖动的集群里，这是 ConsistentHash 最贵的地方——比每次 select 一次 MD5 + 一次 TreeMap 查找贵得多。

它适合"同一参数打到同一台"：本地缓存命中、有状态计算、批量任务分片。代价同样明确：热点 key 无法分散（同一个 userId 永远同一台，哪怕那台已经满了），以及节点变化时**部分** key 会迁移——"一致性"的意思是只迁移 1/N，不是不迁移。

把五种算法的单次成本摊开看，选型就没那么玄了：Random 在无权重配置时是一次 `ThreadLocalRandom`，有权重时是 n 次 `getWeight`（每次都在 url 上做参数查找）+ 一次二分；RoundRobin 是两次 map 查找（外层按 `serviceKey.method`、内层按 `identityString`）+ n 次 `AtomicLong` 读改写；LeastActive 是 n 次 `RpcStatus` 两级 map 查找 + 一次 `AtomicInteger` 读；ShortestResponse 在它之上再加一次 `SlideWindowData` 查找，以及每 30 秒一次异步全量 reset；ConsistentHash 是一次 MD5 + 一次 `TreeMap` 查找，但地址一变就要重建整个环。

3.3 还多了一个 `AdaptiveLoadBalance`（`NAME = "adaptive"`），走的是 P2C：随机取两个 invoker（`pos1 = nextInt(length)`、`pos2 = nextInt(length - 1)`，`pos2 >= pos1` 时加一避免重合），比它们的 load 取小者。它的指标不是 `RpcStatus`，而是 `AdaptiveMetrics` 按 `address:protocolServiceKey` 聚合的负载，还带超时权重。它不是"第六种经典算法"，而是 LeastActive 思路在"看全局指标"方向上的延伸——代价是要维护一套指标采集，所以默认仍然是 `random`（`@SPI(RandomLoadBalance.NAME)`）。

**容易误解的地方**：

1. **"预热降权是 bug，新机器权重被砍了"**。不是。`timestamp` 来自 provider 注册时写入的启动时间，`getWeight` 每次 select 都重算，所以权重是一条 10 分钟爬升的曲线。真要关掉，配 `warmup=0` 或让 provider 不写 `timestamp`；别去改 `calculateWarmupWeight`。
2. **"LeastActive 统计的是全集群并发"**。是 `RpcStatus` 里本进程、本方法的在途计数，粒度到 method。10 个消费者各自看到 1/10 的真相，所以它只能反映"我这边压了多少"，不能反映 provider 真实负载。
3. **"RoundRobin 就是按权重取模"**。3.x 是平滑加权轮询，状态在 `methodWeightMap` 里，且并发下读 `current` 不是原子的，比例是近似值。另外 `setWeight` 会把 `current` 归零——权重一变，节奏就抖一下，这是设计取舍不是缺陷。
4. **"`consistenthash` 保证同一个 key 永远同一台"**。只在 invoker 列表不变时成立；而且默认只看第一个参数，参数对象靠 `toString()` 拼接（`toKey` 里就是 `buf.append(args[i])`），自定义对象没重写 `toString` 等于所有请求哈希到同一个字符串。

**记住这一句**：负载均衡只解决"这一批 invoker 里选谁"，它看不见下游真实压力——除非你愿意为维护 `RpcStatus` 和 WRR 状态付内存与竞争的钱。
