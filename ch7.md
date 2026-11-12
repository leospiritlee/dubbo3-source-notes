# 第7章 集群容错：失败之后怎么办

**一句话**：invoke 定四步模板，doInvoke 决定失败后做什么。

**为什么需要它**：最典型的现场是"重试之后订单重复创建了"：一个创建订单的接口超时，消费端默认 `failover` 把这次超时判成失败又发了一遍，服务端其实已经写库成功，于是下了两单。顺着往下全是问题：`retries=2` 到底是 2 次还是 3 次？为什么业务异常不重试、超时却重试？`check=false` 配了之后，服务明明没起来，为什么启动不报错、一点调用就炸？`broadcast` 到底有几种"模式"？这章把 `dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/support/` 下的源码拆开：`AbstractClusterInvoker.invoke` 的四步模板、`select` / `doSelect` / `reselect` 怎么用 LoadBalance 又怎么避开重复选到同一台、八种策略在 `doInvoke` 里的真实差别、以及 `check=false` 把错误推迟到了哪一层。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `Cluster` | `@SPI(Cluster.DEFAULT)`，`DEFAULT = "failover"`（`Cluster.java:37`）；`@Adaptive join(directory, buildFilterChain)` 把 Directory 合成一个 Invoker |
| `ClusterInvoker` | 消费端代理最终持有的 Invoker，持 `Directory`；`getRegistryUrl/getDirectory/isDestroyed/hasProxyInvokers/isServiceDiscovery` |
| `AbstractClusterInvoker` | 模板类：`invoke()` 做 `list` → `checkInvokers` → `initLoadBalance` → `doInvoke`，后半段留给子类 |
| `list()` / `select()` / `reselect()` | `getDirectory().list(invocation)` 取候选；`doSelect` 走 LoadBalance；`reselect` 在"未选过且可用"子集里重选 |
| `initLoadBalance` | 从 `invokers.get(0).getUrl()` 的 `loadbalance` 方法参数取扩展名，默认 `DEFAULT_LOADBALANCE` |
| `sticky` / `availableCheck` | `CLUSTER_STICKY_KEY` 默认 false；`CLUSTER_AVAILABLE_CHECK_KEY` 默认 true（`Constants.java:61`） |
| 八种 `doInvoke` | failover / failfast / failsafe / failback / forking / broadcast / available / mergeable |
| `ReferenceConfig.checkInvokerAvailable` | 启动期按 `check` 校验 `invoker.isAvailable()`，抛 `IllegalStateException` |
| `AbstractClusterInvoker.checkInvokers` | 调用期空候选抛 `RpcException(NO_INVOKER_AVAILABLE_AFTER_FILTER)` |

**关键代码**：

```java
// AbstractClusterInvoker.java:345
@Override
public Result invoke(final Invocation invocation) throws RpcException {
    checkWhetherDestroyed();
    // ... 绑定 attachments（3.3 已注释掉，交给 ClusterFilter 链）
    List<Invoker<T>> invokers = list(invocation);      // ① 获取 Directory → 候选 Invoker 列表（含路由）
    checkInvokers(invokers, invocation);              // 为什么：空列表立刻抛 NO_INVOKER_AVAILABLE_AFTER_FILTER —— check=false 的报错点
    LoadBalance loadbalance = initLoadBalance(invokers, invocation);  // ② 选 LoadBalance（从 invokers[0] 的 URL 取，默认 random）
    RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);
    try {
        return doInvoke(invocation, invokers, loadbalance);  // ③ 选 Invoker + 调用 + ④ 处理结果，全部由子类决定
    } finally {
        InvocationProfilerUtils.releaseDetailProfiler(invocation);
    }
}

// AbstractClusterInvoker.java:187（doSelect 骨架节选）
private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation,
                            List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
    if (CollectionUtils.isEmpty(invokers)) { return null; }
    if (invokers.size() == 1) {
        Invoker<T> tInvoker = invokers.get(0);
        checkShouldInvalidateInvoker(tInvoker);
        return tInvoker;                              // 为什么：只有一个 provider 时没必要走 LB
    }
    Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);
    boolean isSelected = selected != null && selected.contains(invoker);   // 为什么：重试时不能又选到刚才失败的那台
    boolean isUnavailable = availableCheck && !invoker.isAvailable() && getUrl() != null;
    if (isUnavailable) { invalidateInvoker(invoker); }
    if (isSelected || isUnavailable) {
        Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availableCheck);
        if (rInvoker != null) {
            invoker = rInvoker;
        } else {
            int index = invokers.indexOf(invoker);
            // Avoid collision：兜底取下一台，避免与 LB 结果碰撞
            invoker = invokers.get((index + 1) % invokers.size());   // AbstractClusterInvoker.java:218
        }
    }
    return invoker;
}
```

```java
// FailoverClusterInvoker.java:59（骨架节选）
public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance)
        throws RpcException {
    List<Invoker<T>> copyInvokers = invokers;
    String methodName = RpcUtils.getMethodName(invocation);
    int len = calculateInvokeTimes(methodName);              // 为什么：retries=2 → len=3，最多 3 次而不是 2 次
    RpcException le = null;                                  // last exception
    List<Invoker<T>> invoked = new ArrayList<>(copyInvokers.size());  // 已调用过的，传给 select 做去重
    Set<String> providers = new HashSet<>(len);
    for (int i = 0; i < len; i++) {
        if (i > 0) {
            checkWhetherDestroyed();
            copyInvokers = list(invocation);                 // 为什么：重试前重新拉 Directory，感知 provider 上下线
            checkInvokers(copyInvokers, invocation);
        }
        Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);
        invoked.add(invoker);
        RpcContext.getServiceContext().setInvokers((List) invoked);
        try {
            Result result = invokeWithContext(invoker, invocation);
            // ... 之前失败过则打 CLUSTER_FAILED_MULTIPLE_RETRIES warn
            return result;
        } catch (RpcException e) {
            if (e.isBiz()) { throw e; }                     // 为什么：业务异常是服务端明确拒绝，重试没有意义
            le = e;
        } catch (Throwable e) {
            le = new RpcException(e.getMessage(), e);
        } finally {
            // ... 未成功则 providers.add(invoker.getUrl().getAddress())
        }
    }
    // ... 抛出带 "Tried N times of the providers" 的 RpcException（第 116-126 行）
    throw new RpcException(le.getCode(), "Failed to invoke the method " + methodName + " ... ",
                           le.getCause() != null ? le.getCause() : le);
}

// FailoverClusterInvoker.java:129
private int calculateInvokeTimes(String methodName) {
    int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;  // DEFAULT_RETRIES = 2（CommonConstants.java:417）
    RpcContext rpcContext = RpcContext.getClientAttachment();
    Object retry = rpcContext.getObjectAttachment(RETRIES_KEY);
    if (retry instanceof Number) {                        // 为什么：允许单次调用在 RpcContext 里临时覆盖 retries
        len = ((Number) retry).intValue() + 1;
        rpcContext.removeAttachment(RETRIES_KEY);
    }
    if (len <= 0) { len = 1; }                            // 为什么：retries=-1 也不是"零次"，至少调一次
    return len;
}
```

**拆解**：

### 7.1 四步模板：invoke 只做前半段，doInvoke 决定一切

在进 `invoke()` 之前，先搞清楚这个 `ClusterInvoker` 是怎么来的，否则容易把第 6 章的两条链和第 7 章的策略混成一层。`Cluster` 是 `@SPI(Cluster.DEFAULT)`（默认 `"failover"`，`Cluster.java:34-37`），`@Adaptive <T> Invoker<T> join(Directory<T> directory, boolean buildFilterChain)`（第 47-48 行）。具体策略类极薄，比如 `FailoverCluster extends AbstractCluster`，`doJoin()`（`FailoverCluster.java:32-33`）只有 `return new FailoverClusterInvoker<>(directory);`。真正的外壳在 `AbstractCluster`（`support/wrapper/AbstractCluster.java:43`）：`join(directory, buildFilterChain)`（第 58 行）在 `buildFilterChain` 为 true 时返回 `buildClusterInterceptors(doJoin(directory))`，否则直接 `doJoin(directory)`。`buildClusterInterceptors`（第 45 行）先 `new ClusterFilterInvoker<>(clusterInvoker)`——这个内部类的构造器（第 82-97 行）遍历所有激活的 `FilterChainBuilder`，逐个 `builder.buildClusterInvokerChain(tmpInvoker, REFERENCE_FILTER_KEY, CommonConstants.CONSUMER)`；再叠 `InvocationInterceptorBuilder` 生成的 `InvocationInterceptorInvoker`（第 66-75 行），还可以通过 `CLUSTER_INTERCEPTOR_COMPATIBLE_KEY` 打开 2.7.x 兼容分支（第 48-53 行）。于是消费端代理最终持有的 `Invoker` 是「ClusterFilter 链 → InvocationInterceptor → 具体 `AbstractClusterInvoker` → 实例级 Filter 链 → 真实 Invoker」这么一串；`join()` 负责前半段，`ProtocolFilterWrapper.refer()` 负责后半段。`Cluster.getCluster(scopeModel, name, wrap)`（`Cluster.java:54`）则通过 `getExtension(name, wrap)` 决定是否套 wrapper。多注册中心场景还有 `ZoneAwareClusterInvoker` 这类实现，`ClusterInvoker` 的 Javadoc（第 28-30 行）说明它会"先选一个子 ClusterInvoker，再在里面做 LB"。

`AbstractClusterInvoker.invoke()`（第 345 行）是固定的四步骨架。① **获取 Directory**：`list(invocation)`（`list()` 在第 451-453 行，就是 `return getDirectory().list(invocation)`），这一步里已经跑完路由链。紧接着 `checkInvokers(invokers, invocation)`（第 386-400 行）：空列表直接抛 `RpcException(RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER, "Failed to invoke the method ... No provider available ...")`。② **选 LoadBalance**：`initLoadBalance(invokers, invocation)`（第 466-478 行）用 `applicationModel.getExtensionLoader(LoadBalance.class).getExtension(invokers.get(0).getUrl().getMethodParameter(methodName, LOADBALANCE_KEY, DEFAULT_LOADBALANCE))`；注意参数取自**候选列表第一个 invoker 的 URL**而不是 consumer URL，空列表时退化到 `DEFAULT_LOADBALANCE`。`RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation)` 补异步调用 ID。③④ **选 Invoker、调用、处理结果**：全部塞进抽象方法 `doInvoke(invocation, invokers, loadbalance)`（第 448-449 行声明），八种策略只覆写这一个方法。这个切分很重要：`checkInvokers` 在 `doInvoke` **之前**，所以"没有 provider"这种错误根本进不了任何容错循环。

`select()`（第 155 行）是子类统一入口：先读 `CLUSTER_STICKY_KEY`（默认 `DEFAULT_CLUSTER_STICKY = false`，`Constants.java:73`），若 `stickyInvoker` 仍在当前 `invokers` 里且 `availableCheck && stickyInvoker.isAvailable()`，直接复用（第 164-176 行）；否则 `doSelect(...)`，成功后记 `stickyInvoker = invoker`（第 180-182 行）。`availableCheck` 来自 `url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK)`（第 92 行，默认 true，`Constants.java:61`），不可用会 `invalidateInvoker(invoker)` → `getDirectory().addInvalidateInvoker(invoker)`（第 336-342 行）。真正发起调用的是 `invokeWithContext(invoker, invocation)`（第 402-418 行）：`setContext(invoker)` 把当前 Invoker 放进 `RpcContext.getServiceContext()`，`setRemote(invoker, invocation)`（第 424-429 行）设 `remoteAddress` / `remoteApplicationName`，`finally` 里 `clearContext(originInvoker)` 还原——第 6 章 Filter 里读到的 `RpcContext.getServiceContext().getRemoteHost()` 就是这里塞进去的。`invoke()` 第一行 `checkWhetherDestroyed()`（第 346、372-379 行）检查 `AtomicBoolean destroyed`，已销毁就直接抛 "is now destroyed! Can not invoke any more"；`destroy()`（第 128-132 行）用 CAS 置位后 `getDirectory().destroy()`。这两个方法配合 `isAvailable()`（第 114-120 行，优先看 `stickyInvoker`，否则 `getDirectory().isAvailable()`）构成了 `ClusterInvoker` 的生命周期契约——`ReferenceConfig.checkInvokerAvailable()` 里轮询的那个 `invoker.isAvailable()`，最终落到的就是这里。另有一个专供异步场景的 `invokeWithContextAsync(invoker, invocation, consumerUrl)`（第 437-446 行），Javadoc（第 431-434 行）说明"用线程池 fork 子线程时 ThreadLocal 传不过去"，所以要显式传 `consumerUrl`；Forking 和 Failback 用的都是它。

### 7.2 select 与 reselect：怎么不重复选到同一台

`doSelect()`（第 187 行）先处理两个短路：`invokers` 空返回 `null`；`invokers.size() == 1` 时 `checkShouldInvalidateInvoker` 后直接返回唯一那台，不走 LB。正常路径是 `loadbalance.select(invokers, getUrl(), invocation)`（第 199 行），然后判断两个触发 `reselect` 的条件（第 201-208 行）：`isSelected = selected != null && selected.contains(invoker)`（这次选中的正是之前失败过的那台），或 `isUnavailable = availableCheck && !invoker.isAvailable() && getUrl() != null`。`selected` 由各策略自己传：Failover 传累积的 `invoked`（第 77 行），Failfast / Failsafe / Failback 传 `null`（不重试，无需去重），Forking 传自己的 `selected` 列表来凑够不重复的 forks 个（第 83-87 行）。

`reselect()`（第 254 行）不是"随便换一台"，而是四级降级。第 1 级：从 `invokers` 里挑"可用且不在 `selected` 中"的（第 274-302 行）；规模由 `reselectCount` 控制，默认 `DEFAULT_RESELECT_COUNT = 10`（`CommonConstants.java:527`，第 71、94 行从全局配置读），当 `reselectCount < invokers.size()` 时改成 `ThreadLocalRandom` 随机取 `reselectCount` 个候选并去重——源码注释（第 270-273 行）直说这是为了"reduce retry times if invokers' size is huge, which may lead to long time hang up"，即大集群别为了重选把全表扫一遍。第 2 级：`reselectInvokers` 非空就 `loadbalance.select(reselectInvokers, getUrl(), invocation)`（第 305-307 行）——**在剩余候选上重新走一遍 LoadBalance**，所以权重、一致性哈希依旧生效。第 3 级：若为空，回头把 `selected` 里 `invoker.isAvailable()` 的加回来再选一次（第 309-324 行），注释写 "available first"。第 4 级：全都不行返回 `null`，`doSelect` 兜底 `invokers.get((index + 1) % invokers.size())`（第 214-218 行，注释 "Avoid collision"），且整段包在 `try/catch(Throwable)` 里，异常只记 `CLUSTER_FAILED_RESELECT_INVOKERS` 日志并提示"you can set cluster.availablecheck=false in url"（第 228-236 行）。

三个字段值得单独记住，因为它们都是"实例状态"，会跨调用影响选路：`stickyInvoker`（第 77 行，`volatile`）配合 `CLUSTER_STICKY_KEY` 实现粘滞连接——开启后只要上次的 Invoker 仍在候选里、`isAvailable()` 为真、且不在 `selected` 中，就跳过 LB 直接复用（第 164-176 行），`doSelect` 成功后回写（第 180-182 行）；`availableCheck`（第 69 行）决定 `doSelect` 里 `isUnavailable` 是否成立；`reselectCount`（第 71 行，来自 `ConfigurationUtils.getGlobalConfiguration(...).getInt(RESELECT_COUNT, DEFAULT_RESELECT_COUNT)`，第 94 行）决定重选时最多扫多少候选。注意 `invalidateInvoker()`（第 336 行）并不销毁 Invoker，只是 `getDirectory().addInvalidateInvoker(invoker)`，把"这台连不上"反馈给 Directory，让后续路由/列表把它降权——这是集群层与注册发现层之间唯一的反向通道。另外 `RpcStatus`（`ActiveLimitFilter` / `ExecuteLimitFilter` 用的那个类）是按 URL + methodName 统计 `active` / `total` / `failed` 的全局状态，和这里的 `stickyInvoker` 一样是共享可变状态，所以限流与粘滞都必须在压测下一起验，别只盯着单线程逻辑读源码。

### 7.3 八种策略在源码里的真实差别

`Cluster.DEFAULT = "failover"`（`Cluster.java:37`），所以不配就是 Failover。还要注意 `Cluster.getCluster(scopeModel, name, wrap)`（`Cluster.java:54`）里 `getExtension(name, wrap)` 的 `wrap` 默认为 true，因此拿到的往往不是裸的 `FailoverCluster`，而是被 `support/wrapper/` 下的 wrapper 包过的：`MockClusterWrapper.join()`（`MockClusterWrapper.java:37-38`）返回 `new MockClusterInvoker<>(directory, this.cluster.join(directory, buildFilterChain))`，把 mock/降级能力套在最外层；同目录还有 `ScopeClusterWrapper` / `ScopeClusterInvoker`。所以"集群容错"最外层其实还有一层 mock 判断，读日志时看到 `MockClusterInvoker` 不要以为配错了策略。八个 `*Cluster` 类本身都极薄，唯一动作就是 `doJoin()` 里 new 对应的 `*ClusterInvoker`，所有差异都在 `doInvoke` 里。

**Failover**（`FailoverClusterInvoker.java:59`）：循环 `len` 次，每次 `select(loadbalance, invocation, copyInvokers, invoked)`；`i > 0` 时重新 `list(invocation)` + `checkInvokers` 以感知 provider 上下线（第 69-76 行，注释 "Reselect before retry to avoid a change of candidate invokers"）。`catch (RpcException e)` 里只有 `e.isBiz()` 立刻抛，其余全部赋给 `le` 继续循环（第 103-109 行）；循环耗尽抛带 "Tried N times of the providers" 的 `RpcException`（第 116-126 行）。**非幂等风险就在这里**：`invokeWithContext` 已经把请求发出，超时只代表"不知道服务端有没有执行完成"，重试可能让一次创建变成两次。`retries=2` 经 `calculateInvokeTimes` 的 `+1` 变成 3 次（`DEFAULT_RETRIES = 2`，`CommonConstants.java:417`），`retries=0` 才是不重试；`retries=-1` 被 `len <= 0 → len = 1`（第 137-139 行）兜成 1 次，不是 0 次。第 131-136 行还允许在 `RpcContext.getClientAttachment()` 里临时塞一个 `retries` attachment 单次覆盖，排查"为什么这次没重试"别漏看这里。

**Failfast**（`FailfastClusterInvoker.java:44`）：只 `select` 一次，`invokeWithContext` 失败就包一层 `RpcException` 抛出，`isBiz()` 直接抛原异常——非幂等写操作的常规选择。**Failsafe**（`FailsafeClusterInvoker.java:49`）：`catch (Throwable e)` 后记 `CLUSTER_ERROR_RESPONSE` 日志，`return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation)`（第 61 行），显式吞掉异常返回 null，用于审计、日志等可丢的调用。**Failback**（`FailbackClusterInvoker.java:118`）：失败后 `addFailed(...)`（第 85 行）用 `HashedWheelTimer`（`failback-cluster-timer`，1 秒 tick、32 ticks、容量 `failbackTasks` 默认 `DEFAULT_FAILBACK_TASKS = 100`，`Constants.java:23`）在 `RETRY_FAILED_PERIOD = 5` 秒后调度 `RetryTimerTask` 后台重试，`retries` 默认 `DEFAULT_FAILBACK_TIMES = 3`（`CommonConstants.java:419`）；主线程立刻返回 `AsyncRpcResult.newDefaultAsyncResult(null, null, invocation)`（第 139 行）。因为后台重试跨线程、ThreadLocal 传不过去，这里必须用 `invokeWithContextAsync(invoker, invocation, consumerUrl)`（第 126 行，注释第 124-125 行明说原因）。适合通知类。

**Forking**（`ForkingClusterInvoker.java:72`）：`forks` 默认 `DEFAULT_FORKS = 2`（`Constants.java:25`），`timeout` 默认 `DEFAULT_TIMEOUT = 1000`（`CommonConstants.java:147`）；`forks <= 0 || forks >= invokers.size()` 就打全部，否则用 `select` 凑 `forks` 个互不相同的 Invoker（第 78-89 行）。并发靠 `CompletableFuture.supplyAsync(..., executor)`（`executor` 是 `FrameworkExecutorRepository.getSharedExecutor()`，第 62-67 行）投到线程池，结果汇入 `LinkedBlockingQueue<Object> ref = new LinkedBlockingQueue<>(1)`（第 92 行）：任一路成功 `ref.offer(v)`，全失败时 `count.incrementAndGet() >= selected.size()` 才把异常 offer 进去（第 103-112 行）。主线程 `ref.poll(timeout, TimeUnit.MILLISECONDS)`（第 115 行）拿第一个结果。注意一个真实行为：**超时后 `poll` 返回 `null`，源码直接 `return (Result) ret`（第 126 行）返回 null，不抛异常**——别把 Forking 超时当成 `TimeoutException` 来 catch。另外 `invokeWithContextAsync` 在这里也是必需的（第 100 行），并因 `RpcContext` 跨线程问题在 `finally` 里 `RpcContext.getClientAttachment().clearAttachments()`（第 133-136 行）。

**Broadcast**（`BroadcastClusterInvoker.java:54`）：遍历全部 `invokers` 逐个调用，每个都 `new RpcInvocation(...)` 复制一份（第 80-91 行），并用 `RpcContext.RestoreContext` 在非最后一次调用后 `restore()`（第 78、121-124 行），避免上一个 provider 改的上下文串到下一个。失败计数 `failIndex`，阈值 `failThresholdIndex = invokers.size() * broadcastFailPercent / MAX_BROADCAST_FAIL_PERCENT`（第 74 行），`broadcast.fail.percent` 默认 100（第 64 行）；循环结束只要 `exception != null` 就 `throw exception`（第 142 行）。**关于"两种模式"要纠正**：3.3 源码里根本没有 `failover`/`failsafe` 两个模式开关，只有一个 0-100 的百分比旋钮。类注释（第 60-63 行）说 "100 means that an exception will be thrown last, and 0 means that as long as an exception occurs, it will be thrown"，对应的是"全部调完才抛"（宽松/容错）与"一出错就抛"（严格）两种**设计意图**。但实现上 `failThresholdIndex` 是整数、判断用 `==`（第 104、118 行），而 `failIndex` 从 0 起、`failIndex++` 后变成 1——`broadcast.fail.percent=0` 时阈值算出来是 0，`1 == 0` 永远不成立，实际不会提前 break，仍是遍历完再抛。percent 落在 1~99 时（如 5 台配 20 → 阈值 1）才会在第 1 台失败后停。读文档别当成精确实现。

**Available**（`AvailableClusterInvoker.java:39`）：完全不用 LoadBalance，顺序遍历找第一个 `invoker.isAvailable()` 就调，全不可用抛 `RpcException("No provider available in " + invokers)`。**Mergeable**（`MergeableClusterInvoker.java:62`）：`merger` 取自 `getUrl().getMethodParameter(methodName, MERGER_KEY)`；没配就只调第一个可用 group，并对 `e.isNoInvokerAvailableAfterFilter()` 记录后继续试下一个 group（第 66-82 行）；配了就对每个 invoker 发一个 `setAttachment(ASYNC_KEY, "true")` 的 `RpcInvocation`（第 96-99 行），`asyncResult.get(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)` 等齐（第 122 行），再按 `merger` 指定的 `Merger` SPI 或返回类型上的同名方法逐个 `method.invoke` 合并（第 148-171 行）。用于多 group 结果聚合。

### 7.4 check=false 在集群层的表现

`check` 不是 `AbstractClusterInvoker` 的字段，它属于 `ReferenceConfig`，只影响"什么时候发现没有 provider"。`ReferenceConfig.init(boolean check)`（`ReferenceConfig.java:332`）在 `createProxy` 之后有 `if (check) { checkInvokerAvailable(0); }`（第 392-394 行）；`checkInvokerAvailable(long timeout)`（第 716 行）开头 `if (!shouldCheck()) { return; }`（第 717-719 行），否则轮询 `invoker.isAvailable()` 到 `checkDeadline`，仍不可用就抛 `IllegalStateException("Failed to check the status of the service ... No provider available ...")`（第 747-756 行）。所以 `check=true`（默认）会让"没有 provider"在**启动/首次 `get()` 阶段**就变成 Spring 容器刷新失败。入口不止一个：`ReferenceConfig.get(boolean check)`（第 230 行）先判 `destroyed`，再 `init(check)`（第 244 行）；`get()` 那条路径还会在 `init` 之后额外 `checkInvokerAvailable(timeout)`（第 256 行），而 `get()` 的 catch 块（第 287-288 行）专门识别 `IllegalStateException` 做清理。`checkInvokerAvailable` 的 `do/while` 每 100ms 轮询一次 `invoker.isAvailable()`（第 727-735 行），超时后打 `REGISTRY_EMPTY_ADDRESS` warn 并给出 "After Nms reties, finally failed"——这也是排查"启动很慢"时要看的地方：`check` 会实打实地把线程阻塞在等待 provider 上。

`check=false` 只是跳过这次启动期校验，代理照常创建。错误被推迟到第一次真实调用：`AbstractClusterInvoker.invoke()` → `list(invocation)` 返回空 → `checkInvokers(invokers, invocation)`（第 358 行）抛 `RpcException(NO_INVOKER_AVAILABLE_AFTER_FILTER, ...)`（第 386-400 行）。三个后果值得记牢：异常类型从 `IllegalStateException` 变成 `RpcException`，可以被业务 `catch`；因为 `checkInvokers` 在 `doInvoke` **之前**，空 provider 时连 Failover 的循环都进不去，重试次数一次都用不上，日志里不会出现 "Tried 3 times"；而 `MergeableClusterInvoker` 对 `e.isNoInvokerAvailableAfterFilter()` 是特判的——记 warn、继续尝试下一个 group（第 101-108 行），不直接抛。

集群层自己确实有一个相近但无关的开关：`availableCheck`（`cluster.availablecheck`，默认 true，第 92 行），它决定 `doSelect` 里要不要用 `invoker.isAvailable()` 判定并把失效 Invoker 加进 `addInvalidateInvoker`（第 203-207 行）。`reselect` 失败时日志里那句 "you can set cluster.availablecheck=false in url"（第 234 行）指的就是它，和 `check=false` 完全不是一回事。

最后把视角拉回第 5 章的部署器：`DefaultModuleDeployer.startSync()`（`DefaultModuleDeployer.java:162`）在 `exportServices()`、`referServices()` 之后还有 `checkReferences()`（第 197 行），方法体（第 455-461 行）是 `Optional<ModuleConfig> module = configManager.getModule(); long timeout = module.map(ModuleConfig::getCheckReferenceTimeout).orElse(30000L); for (ReferenceConfigBase<?> rc : configManager.getReferences()) { referenceCache.check(rc, timeout); }`。也就是说除了 `ReferenceConfig` 自己那次启动期校验，`ModuleDeployer` 在模块启动完成时还会按 `checkReferenceTimeout`（默认 30000ms）对模块内所有 reference 做一次集中检查。这两道检查加上调用期 `AbstractClusterInvoker.checkInvokers` 的第三道，构成"启动期 / 模块启动完成 / 首次调用"三个可能暴露"没有 provider"的时点；`check=false` 只是绕过第一道，第二道仍可能把错误提前到容器刷新阶段。排查"明明配了 `check=false` 却还是启动失败"时，要往 `ModuleConfig.checkReferenceTimeout` 和 `checkReferences()` 这条线查，而不是只盯着 `@DubboReference(check = false)`。

**容易误解的地方**：

1. **`retries=2` 是最多 3 次**。`calculateInvokeTimes()` 里 `getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1`（第 130 行），且 `DEFAULT_RETRIES = 2`（`CommonConstants.java:417`），所以默认值就是 3 次。`retries=0` 才是"不重试"；`retries=-1` 被 `len <= 0 → len = 1`（第 137-139 行）兜成 1 次，不是 0 次。另外 `RpcContext` 里临时塞的 `retries` attachment 会单次覆盖配置（第 131-136 行）。
2. **Failover 只对幂等操作安全**。它只放过 `RpcException.isBiz()` 的业务异常（第 104-106 行），超时/网络异常一律重试；而超时不代表服务端没执行完。非幂等写要么 `retries=0`，要么换 `failfast`。别指望"服务端抛个异常就不会重试"——业务异常不重试，但包装成普通 `RuntimeException` 的框架异常会重试。
3. **`check=false` 不是"忽略错误"**，只是把报错从启动期（`IllegalStateException`，Spring 刷新失败）推迟到首次调用（`RpcException(NO_INVOKER_AVAILABLE_AFTER_FILTER)`）。由于 `checkInvokers` 位于 `doInvoke` 之前（第 358 行），此时任何集群策略都还没开始工作，Failover 帮不上忙。别把它和 `cluster.availablecheck` 混为一谈。
4. **3.3 的 Broadcast 没有 "failover/failsafe 两种模式"**，只有 `broadcast.fail.percent`（0-100，默认 100，第 44、64 行）；而且 `failIndex == failThresholdIndex` 的 `==` 比较加上 `failIndex` 从 1 起算（第 75、104、118 行），percent=0 时阈值是 0、不会提前 break，实际仍是遍历完再抛。源码注释描述的是设计意图，不是精确语义。

**记住这一句**：容错策略只决定 doInvoke 里"失败之后做什么"，而失败信号能不能传到这里，取决于异常有没有被 Filter 或 Directory 吞掉。
