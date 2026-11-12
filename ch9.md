# 第9章 服务目录与路由：地址从哪来、被谁筛掉

**一句话**：Directory 回答"有哪些地址"，RouterChain 回答"这次能用哪些"。

**为什么需要它**：灰度发布，给新版本实例打了 `tag=gray`，消费者也带了 `dubbo.tag=gray`，但请求还是打到老版本；查日志发现路由链返回了空，然后"自动"回退到了全部实例。这个"自动回退"就是本章的核心：`force` 的默认值、空列表保护、`cachedInvokerUrls` 兜底，三条机制叠在一起，才有了你看到的"路由规则不生效"。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `Directory` | 地址来源抽象，`list(invocation)` 返回已过滤的 invoker |
| `StaticDirectory` | 固定 invoker 列表的快照，用于 group 合并、直连 |
| `DynamicDirectory` | 订阅注册中心，实现 `NotifyListener`，`forbidden` 标记无可用地址 |
| `RegistryDirectory` | 接口级目录，按 category 拆 configurator/router/provider |
| `ServiceDiscoveryRegistryDirectory` | 应用级目录，收 instance 列表，`isServiceDiscovery()=true` |
| `RouterChain` / `SingleRouterChain` | 双链切换 + 读写锁，真正执行路由 |
| `StateRouter` | 3.0 起的主路由模型，`BitList` + `next` 链式调用 |
| `force` | 路由结果为空时，是返回空还是回退到输入 |
| `BitList` | `long[]` 位图 + `Object[]` 数组的 invoker 容器，支持位运算求交 |

**关键代码**：

```java
// RegistryDirectory.java:274-311  空地址的三种处理，全部在这二十行里
private void refreshInvoker(List<URL> invokerUrls) {
    if (invokerUrls.size() == 1
            && invokerUrls.get(0) != null
            && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {   // "empty://"：注册中心明确宣告"没提供者了"
        refreshRouter(BitList.emptyList(), () -> this.forbidden = true);    // 先清路由链，再置 forbidden
        destroyAllInvokers();
    } else {
        this.forbidden = false;
        Set<URL> localCachedInvokerUrls = this.cachedInvokerUrls;
        if (invokerUrls.isEmpty()) {
            if (CollectionUtils.isNotEmpty(localCachedInvokerUrls)) {
                invokerUrls.addAll(localCachedInvokerUrls);                  // 空保护：注册中心推了空列表，用上次的地址顶住
            }
        } else {
            localCachedInvokerUrls = new HashSet<>();
            localCachedInvokerUrls.addAll(invokerUrls);
            this.cachedInvokerUrls = localCachedInvokerUrls;
        }
        if (invokerUrls.isEmpty()) {
            return;                                                        // 没有缓存可回退，直接不动，保留旧 invokers
        }
        Map<URL, Invoker<T>> newUrlInvokerMap = toInvokers(oldUrlInvokerMap, invokerUrls);
        // ... newUrlInvokerMap 为空则 return（协议不匹配等）
        BitList<Invoker<T>> finalInvokers =
                multiGroup ? new BitList<>(toMergeInvokerList(newInvokers)) : new BitList<>(newInvokers);
        refreshRouter(finalInvokers.clone(), () -> this.setInvokers(finalInvokers));  // 双链先更新，再换引用
        this.urlInvokerMap = newUrlInvokerMap;
        destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
        this.invokersChanged();
    }
}
```

```java
// ConditionStateRouter.java:245-278  "至少保留一个"真正的落点
BitList<Invoker<T>> result = invokers.clone();
result.removeIf(invoker -> !matchThen(invoker.getUrl(), url));

if (!result.isEmpty()) {
    if (needToPrintMessage) { messageHolder.set("Match return."); }
    return result;
} else if (this.isForce()) {
    logger.warn(CLUSTER_CONDITIONAL_ROUTE_LIST_EMPTY, "execute condition state router result list is empty. and force=true",
            "", "The route result is empty and force execute. ...");
    return result;                       // force=true：空就是空，交给上层报 No provider
}
// 走到这里说明 force=false 且结果为空 —— 返回输入的 invokers，等于"这条规则没生效"
return invokers;
```

**拆解**：

### 1. 两类 Directory：静态是快照，动态是订阅

`Directory` 接口只有几个方法，但 `list(Invocation)` 是整条调用链的分水岭——`ClusterInvoker` 拿到的候选列表就来自这里。`AbstractDirectory` 把公共逻辑收得很干净：`list` 先拿 `routerChain.getLock().readLock()`，克隆 `validInvokers`（初始化前是 `invokers`），取 `SingleRouterChain`，再调子类 `doList`；结果为空就打一条 `CLUSTER_NO_VALID_PROVIDER` 的 warn。

`invokers` 和 `validInvokers` 的区分是 3.x 才有的。`invokers` 是注册中心给的全量，`validInvokers` 减掉了两类：`invokersToReconnect`（连接断了，`addInvalidateInvoker` 塞进去，后台 `checkConnectivity` 每 `reconnectTaskPeriod` 试一次）和 `disabledInvokers`（服务下线通知，`addDisabledInvoker` 塞进去，只有注册中心重新推送才恢复）。这个设计让"暂时不可用"和"已下线"走了两条不同的恢复路径，不用重建整个列表。

`checkConnectivity` 有个容易忽略的约束：一个 `Semaphore(1)` 的 `tryAcquire` 保证每个 directory 同一时刻最多只有一个重连任务在跑，任务末尾如果 `invokersToReconnect` 还不为空就再提交一次，形成自驱动的循环。这样即使有 500 个地址全挂了，也不会瞬间往调度线程池里塞 500 个任务——它按 `reconnectTaskTryCount` 分批随机抽着试。

还有一个反直觉的分支在 `DynamicDirectory.doList`：`if (multiGroup) { return this.getInvokers(); }`。多 group（`group = "*"` 或逗号分隔）时**直接跳过整条路由链**，返回全量 invoker。原因是多 group 的每个 group 在 `toMergeInvokerList` 里已经各自建过 `StaticDirectory` 并合并过了，再走一遍外层路由链没有意义，反而可能把已经合并好的 cluster invoker 再过滤一次。这条短路也是"配了 `group` 之后路由规则不生效"的一个隐藏原因。

`StaticDirectory` 是纯快照：构造时 `setInvokers(new BitList<>(invokers))`，`notify` 直接换列表，没有注册中心、没有订阅。它出现在两个地方——`RegistryDirectory.toMergeInvokerList` 里每个 group 建一个 `StaticDirectory` 再 `cluster.join`（多 group 合并），以及直连 `dubbo://ip:port`。`buildRouterChain()` 会为它建一条独立的路由链，因为 group 合并后的列表需要单独路由。

`DynamicDirectory` 实现 `NotifyListener`，`subscribe(url)` 就是 `registry.subscribe(url, this)`。它的 `doList` 有两道闸：`forbidden && shouldFailFast` 时直接抛 `RpcException(FORBIDDEN_EXCEPTION)`——这是"确定没有提供者"的快速失败；路由抛异常时 catch 住、记 `CLUSTER_FAILED_SITE_SELECTION`、返回空列表，而不是把异常往上冒。

`RegistryDirectory.notify` 收到的是原始 `List<URL>`，先按 category 分组（`configurators` / `routers` / `providers`）。configurator 不是过滤，是**改写**：`overrideWithConfigurator(getOriginalConsumerUrl())` 把 `timeout`、`weight` 这类参数覆盖进 `directoryUrl`，所以配置中心的动态配置最终体现在 URL 参数上，而不是另一套机制。router 走 `toRouters` 生成老式 `Router` 实例，`addRouters` 追加到链尾。

然后是 `refreshInvoker`，本章最关键的一段。三种"空"被区别对待：收到单条 `empty://`（`EMPTY_PROTOCOL = "empty"`）表示注册中心明确说"这个服务没有提供者了"，此时 `forbidden = true` 并销毁全部 invoker；收到**空列表**但有 `cachedInvokerUrls`，走空保护，把上次的地址塞回去；两者都没有，直接 `return`，保留现状。这三种情况在日志里长得完全不一样，排查"服务突然不可用"时先看是哪一种。

`toInvokers` 的缓存策略也值得看：`oldUrlInvokerMap.remove(url)` 命中就复用旧 invoker，不重新 `protocol.refer`——只有 URL 参数变了（key 是 merge 后的 url）才会重建连接。这是地址推送频繁时不会疯狂建连的原因。

`ServiceDiscoveryRegistryDirectory` 是应用级服务发现的目录，`isServiceDiscovery()` 返回 `true`。它收到的不是带 category 的 URL，而是实例列表，所以没有 `judgeCategory` 这一步，`toInvokers` 的 key 也变成 `ProtocolServiceKeyWithAddress`；一个实例暴露多个协议时，会用 `InstanceWrappedInvoker` 包一层。`isNotificationReceived()` 的判断也变了：要求 `serviceListener.getAllInstances().size() == getServiceNames().size()`，即所有相关应用都推过一轮才算"收到"。

### 2. RouterChain：双链切换，和"至少保留一个"的真正位置

`RouterChain.buildChain` 一次建**两条** `SingleRouterChain`（`mainChain` / `backupChain`）。`setInvokers` 的流程是刻意绕的：先把 `currentChain` 切成 `backupChain`（此时新调用走旧链、旧地址），再去改 `mainChain`（拿它的写锁，等所有正在用 main 的调用结束），改完把 `notifyingInvokers` 指向新列表、执行 `switchAction`（真正替换 directory 里的 invokers 引用），最后切回 `currentChain = mainChain` 并补更新 `backupChain`。

为什么要两条链：地址刷新期间，正在跑的调用不能看到"链里是旧地址、列表是新地址"这种半成品状态。`getSingleChain` 里那段判断就是在处理这个窗口——如果 `availableInvokers` 来自新列表而 main 已更新完，就用 main；否则用 backup。代价是每次刷新要改两次链、加两次写锁，换来的是地址变更期间不出现"路由到已下线实例"。

`simpleRoute` 的执行顺序：先跑 StateRouter 链（`headStateRouter.route(...)`），结果为空且 `(shouldFailFast || routers.isEmpty())` 就返回空；再逐个跑老式 `Router`，`CollectionUtils.isEmpty(commonRouterResult) && shouldFailFast` 就返回空，`!routeResult.isNeedContinueRoute()` 则提前返回。`shouldFailFast` 来自 `dubbo.router.should-fail-fast`，默认 `"true"`。

这里有个广泛流传的误解：**"RouterChain 保证至少返回一个 invoker"**。代码里没有任何"保底"逻辑，`RouterChain` 只在为空时短路返回。真正的"至少保留一个"发生在每个 router 内部——`ConditionStateRouter` 在 `force=false` 且结果为空时 `return invokers`（返回输入），等于"这条规则不生效"；`TagStateRouter` 在没有匹配 tag 时走 FAILOVER 分支，返回所有"没有 tag 的实例"。所以保护是**逐个 router 协商**出来的，不是链条层面的兜底。`force=true` 就是主动放弃这层保护：宁可报 `No provider available`，也不要打到不该打的机器上。

`AbstractStateRouter.route` 是 `final` 的模板方法，三个动作值得记：`doRoute` 返回后，如果结果不是同一个引用，就 `routeResult = invokers.and(routeResult)`——用位运算强制结果是输入的子集，任何 router 都别想凭空造出地址；`supportContinueRoute()` 为 false 时才由框架调 `continueRoute`（tag 这类需要看全局的 router 会重写它自己控制）；`shouldFailFast && routeResult.isEmpty()` 时不继续往下传。

顺序由 `@Activate(order)` 决定，`getActivateExtension` 排序后建链：Mock(-100) → Tag(100) → Service(140) → ProviderApp(145) → App(150) → Script(200)。Mock 排最前是因为它要把"该走 mock"的请求提前摘出去；Tag 在条件路由之前，是因为标签是更粗的圈选。

### 3. 条件路由与标签路由：语法、force，以及那个 `|`

条件路由的规则是 `when => then`，解析在 `ConditionStateRouter.init`：按 `=>` 切两半，左边空或 `true` 视为无条件，右边空或 `false` 时 `thenCondition = null`——这不是"没规则"，而是"黑名单"，`doRoute` 里 `thenCondition == null` 直接返回空列表。

匹配语法由 `ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)")` 切分，支持 `=`、`!=`、`,`（或）、`&`（与）。值侧交给 `ValuePattern` 扩展：通配 `*`、区间 `1~100`；键侧交给 `ConditionMatcherFactory`，默认三类——`param`（url 参数）、`argument`（`arguments[0]`）、`attachment`（`attachments[foo]`）。所以 `method = sayHello => region = hangzhou`、`arguments[0] = 123 => host != 10.20.153.10` 都是合法规则。

匹配方向要分清：`matchWhen` 拿 `consumerUrl` 的 `toOriginalMap()` 当样本，`matchThen` 拿每个 provider 的 url 当样本。`method` 和 `methods` 这两个键特殊处理，值取自 `RpcUtils.getMethodName(invocation)` 而不是 url——这就是"按方法路由"能生效的原因。

`force` 的语义前面说过，这里补一句实操：默认 `false`（`AbstractRouterRule.force = false`），意味着**任何写错的条件路由都会静默失效**，只在日志里留一条 `CLUSTER_CONDITIONAL_ROUTE_LIST_EMPTY`。灰度/隔离这类"宁可失败也不能打错"的场景，必须显式写 `force: true`。

标签路由有两套规则源。有动态规则（配置中心下发的 `TagRouterRule`）时，按 `tagnameToAddresses` 做地址匹配；规则无效或未启用时退化到 `filterUsingStaticTag`，直接比 provider url 上的 `dubbo.tag` 参数。

`tag` 的取值顺序是：`invocation.getAttachment("dubbo.tag")` 优先，其次 `url.getParameter("dubbo.tag")`。这意味着单个请求可以覆盖全局标签，灰度时按用户维度打标就靠这个。

三个 `force` 必须分清，混了就排查不出来：

- 规则里的 `force`（`TagRouterRule.force`，默认 false）：动态标签组匹配到空时，是返回空还是继续往下走。
- 请求级的 `dubbo.force.tag`（`FORCE_USE_TAG`）：没有匹配 tag 的实例时，是否允许回退到"无 tag 实例"。`isForceUseTag` 先看 attachment，再看 router url 参数。
- `ConditionStateRouter` 的 `force`：条件路由结果为空时是否强制生效。

`doRoute` 的分支顺序值得逐行看一遍：有 tag 且动态组里能找到地址 → 过滤，结果非空**或** `rule.force` 为真就返回；动态组里没有这个 tag → 退到静态 tag；静态 tag 也没有匹配 → 若 `!isForceUseTag` 则 FAILOVER，返回"所有不在动态组里、且自己也没打 tag"的实例。这条 FAILOVER 就是"灰度流量漏到老版本"的官方通道——它不是 bug，是默认行为。

tag 还支持层级，`selectAddressByTagLevel` 处理 `beta|team1|partner1` 这种写法：从长到短逐级回退（先找 `beta|team1|partner1`，空了找 `beta|team1`，再空了找 `beta`）。`force` 为真时不做层级回退，直接取完整 selector。这让"团队内灰度 → 团队全量 → 全局"能用一条 tag 表达。

### 4. StateRouter：3.x 到底换掉了什么

旧的 `Router` 接口吃 `List<Invoker>`、吐 `List<Invoker>`，每次过滤都要新建 ArrayList，链式执行靠 `RouterChain` 里的 for 循环。`StateRouter` 换了三样东西。

第一是容器：`BitList<Invoker>` 内部是 `long[]` 位图 + `Object[]` 数组，`removeIf` 是把对应 bit 置 0，`and(other)` 是按位与。10 个 router 串行过滤时，省掉的是 10 次数组拷贝——在几千个地址、每秒几万次调用的场景下，这是 GC 压力的主要来源之一。

第二是链式结构：`initWithStateRouters` 倒序遍历，`nextStateRouter.setNextRouter(stateRouter)`，把列表编成单链表，`headStateRouter` 是入口。`route` 是 `final` 模板方法，子类只实现 `doRoute`，无法绕过"结果必须是输入子集"和"是否继续"这两条约束。

第三是 `RouterResult.needContinueRoute`：老 `Router` 也能提前终止，`RouterResult` 带上 `needContinueRoute` 和 `message`（用于 QoS 的路由快照）。这是 3.x 排障能力的一部分——`RpcContext.getServiceContext().isNeedPrintRouterSnapshot()` 打开后，每个 router 的输入输出都会被记录成 `RouterSnapshotNode` 树，`No provider available` 时直接告诉你"是哪个 router 把列表清空的"。这比 2.7 时代靠猜强太多。

脚本路由（`ScriptStateRouter`）是这条链上唯一执行外部代码的节点，安全边界值得说清。它用 `javax.script`，默认引擎 `javascript`（`DEFAULT_SCRIPT_TYPE_KEY`），`Compilable.compile(rule)` 预编译一次，执行时 `AccessController.doPrivileged(..., accessControlContext)`，`AccessControlContext` 里只放了 `RuntimePermission("accessDeclaredMembers")` 一个权限。脚本里能拿到 `invokers`、`invocation`、`context` 三个绑定，返回值经 `getRoutedInvokers` 做 `retainAll`——只能做减法，不能新增地址。

但这层沙箱的边界要说实话：它限制的是 Java 权限模型里的反射能力，不是完整的进程隔离；规则来自配置中心，等于把"能改配置的人"提升到"能在消费者进程里执行任意脚本"。而且 JDK 15 起 Nashorn 被移除，JDK 17 上默认引擎直接 `null`，构造时就抛 `unsupported route engine type`。生产环境用它之前，先确认 JDK 版本和谁能改配置中心。

`MockStateRouter`（`MockInvokersSelector`）是另一个有意思的实现：`notify` 时就把 invoker 预分成 `mockedInvokers` 和 `normalInvokers` 两个 `BitList`，`doRoute` 只看 `invocation.need.mock` 这个 attachment，`invokers.and(...)` 一下就完事。它把"每次调用都做分类"变成了"地址变更时分类一次"，是 `StateRouter` 缓存思路的典型用法。

**容易误解的地方**：

1. **"`force=true` 就是强制打到指定机器"**。反了。`force=true` 是"允许返回空"，匹配不到就真的没有 provider，调用方直接报错。它的用途是"宁可失败也不打错"，不是"无论如何都路由过去"。
2. **"注册中心推空列表，消费者就清空地址"**。`refreshInvoker` 里有 `cachedInvokerUrls` 空保护，空列表会用上次的地址顶住；只有收到单条 `empty://` 才 `forbidden = true` 并销毁 invoker。这两条路径的日志完全不同，排查时先分清。
3. **"`RouterChain` 兜底保证至少一个 invoker"**。链条里没有任何保底逻辑，只有 `shouldFailFast` 的短路。保护在 router 内部：`force=false` 时返回输入列表。所以"路由规则不生效"和"路由规则生效了但没匹配到"在日志上都表现为"用了全部地址"，只能靠 `force` 和路由快照区分。
4. **"`tag` 路由的 `force` 和 `dubbo.force.tag` 是一回事"**。前者是规则级（动态标签组为空时是否返回空），后者是请求级（没有匹配 tag 时是否允许回退到无 tag 实例）。默认都 false，叠加起来的效果是"灰度实例没起来时，灰度流量会静默流到老版本"。

**记住这一句**：地址由 Directory 提供，路由由 RouterChain 裁剪，两者之间只传 `BitList`——任何 router 都只能做减法，减到零还是减到一，取决于 `force`。
