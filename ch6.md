# 第6章 Filter 链：一次调用的洋葱模型

**一句话**：Filter 逆序包成 Invoker 链，逐层递归。

**为什么需要它**：给接口加了个自定义鉴权 Filter，上线后发现超时统计没了；另一个 Filter 里 `catch` 住异常返回了个默认值，结果消费端不再重试、订单状态卡住；还有人想加个"每次调用只打一次日志"的埋点，却发现日志被打印了好几遍，因为逻辑挂在负载均衡之后的实例级链上。这些都不是业务 bug，而是没搞清一次 RPC 从 Proxy 到真实 Invoker 中间套了几层、顺序由谁定、回调在哪触发。这章解决三件事：链是怎么被拼出来的（3.3 已经不是 2.x 那个私有 `buildInvokerChain` 了）、`@Activate(group = CONSUMER/PROVIDER)` 的 `group / value / order / before / after` 到底怎么筛和排、`BaseFilter.Listener` 的 `onResponse` / `onError` 是给谁用的，以及为什么一个 Filter 绝不能吞异常。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `ProtocolFilterWrapper` | `Protocol` 的 SPI wrapper，`@Activate(order = 100)`（`dubbo-cluster` 模块）；`export` / `refer` 时调 `FilterChainBuilder.buildInvokerChain` |
| `FilterChainBuilder` / `DefaultFilterChainBuilder` | `@SPI(value="default", scope=APPLICATION)`；`getActivateExtension` 拿已排序 Filter，逆序 new 节点，最外层再套回调注册器 |
| `CopyOfFilterChainNode` | `@Experimental` 的链节点，`invoke()` 里 `filter.invoke(nextNode, invocation)` 构成递归；`getUrl/getInterface/isAvailable/destroy` 全委托 `originalInvoker` |
| `CallbackRegistrationInvoker` | 包在链最外层，`asyncResult.whenCompleteWithContext` 里逆序触发所有 Listener 的 `onResponse` / `onError` |
| `Filter` / `BaseFilter` | `Filter extends BaseFilter`，`@SPI(scope = MODULE)`；`BaseFilter.invoke` 约定"必须调 `invoker.invoke()`"，`BaseFilter.Listener` 提供两个回调 |
| `@Activate` | `group` 决定哪一端生效，`value` 是 URL 条件，`order` / `before` / `after` 决定顺序 |
| `ActivateComparator` | `order` 升序 → `before`/`after` → 类名字符串兜底；`getActivateExtension` 用 `TreeMap<>(activateComparator)` 产出有序列表 |
| `ClusterFilter` | 3.0 新增，跑在负载均衡**之前**；`ConsumerContextFilter` 等位于 `dubbo-cluster/.../filter/support/`，与实例级 `Filter` 是两条链 |
| `ListenableFilter` | 3.3 已 `@Deprecated`；旧式"每调用一个 Listener"容器，现在推荐直接 `implements Filter.Listener` |

**关键代码**：

```java
// ProtocolFilterWrapper.java:56（类上有 @Activate(order = 100)，本身是 Protocol 的 wrapper）
@Override
public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
    if (UrlUtils.isRegistry(invoker.getUrl())) {
        return protocol.export(invoker);                    // 为什么：registry:// 是注册中心包装，不套业务 Filter 链
    }
    FilterChainBuilder builder = getFilterChainBuilder(invoker.getUrl());
    return protocol.export(builder.buildInvokerChain(invoker, SERVICE_FILTER_KEY, CommonConstants.PROVIDER));
}

// ProtocolFilterWrapper.java:70
@Override
public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
    if (UrlUtils.isRegistry(url)) {
        return protocol.refer(type, url);
    }
    FilterChainBuilder builder = getFilterChainBuilder(url);
    return builder.buildInvokerChain(protocol.refer(type, url), REFERENCE_FILTER_KEY, CommonConstants.CONSUMER);
    // 为什么：同一套 SPI，靠 key(service.filter / reference.filter) + group(PROVIDER / CONSUMER) 取不同子集
}

// DefaultFilterChainBuilder.java:44（骨架节选）
@Override
public <T> Invoker<T> buildInvokerChain(final Invoker<T> originalInvoker, String key, String group) {
    Invoker<T> last = originalInvoker;
    URL url = originalInvoker.getUrl();
    List<ModuleModel> moduleModels = getModuleModelsFromUrl(url);
    List<Filter> filters;
    if (moduleModels != null && moduleModels.size() == 1) {
        filters = ScopeModelUtil.getExtensionLoader(Filter.class, moduleModels.get(0))
                .getActivateExtension(url, key, group);     // 为什么：group 在这里过滤，返回的 List 已经按 ActivateComparator 排好序
    } else if (moduleModels != null && moduleModels.size() > 1) {
        // ... 逐个 ModuleModel 取扩展、合并、sortingAndDeduplication(filters, directors)
        filters = new ArrayList<>();
    } else {
        filters = ScopeModelUtil.getExtensionLoader(Filter.class, null).getActivateExtension(url, key, group);
    }
    if (!CollectionUtils.isEmpty(filters)) {
        for (int i = filters.size() - 1; i >= 0; i--) {     // 为什么：逆序包装 → order 小的在最外层，"前半段"先执行、"后半段"后执行
            final Filter filter = filters.get(i);
            final Invoker<T> next = last;
            last = new CopyOfFilterChainNode<>(originalInvoker, next, filter);
        }
        return new CallbackRegistrationInvoker<>(last, filters);  // 为什么：回调统一在最外层收口，覆盖整条链
    }
    return last;
}
```

```java
// FilterChainBuilder.java:344（CopyOfFilterChainNode.invoke，骨架节选）
@Override
public Result invoke(Invocation invocation) throws RpcException {
    Result asyncResult;
    try {
        InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Filter " + filter.getClass().getName() + " invoke.");
        asyncResult = filter.invoke(nextNode, invocation);   // 为什么：nextNode 是下一层，Filter 自己决定何时"穿过"——洋葱的递归就在这里
        if (!(asyncResult instanceof AsyncRpcResult)) {       // 为什么：3.x 全异步，Filter 必须还 AsyncRpcResult，否则链断
            String msg = "The result of filter invocation must be AsyncRpcResult. ...";
            LOGGER.error(INTERNAL_ERROR, "", "", msg);
            throw new RpcException(msg);
        }
    } catch (Exception e) {
        InvocationProfilerUtils.releaseDetailProfiler(invocation);
        // ... listener(invocation) 非空则 listener.onError(e, originalInvoker, invocation)
        throw e;                                             // 为什么：异常必须原样往外抛，链才不会假装成功
    }
    return asyncResult;
}

// FilterChainBuilder.java:196（CallbackRegistrationInvoker.invoke，骨架节选）
@Override
public Result invoke(Invocation invocation) throws RpcException {
    Result asyncResult = filterInvoker.invoke(invocation);
    asyncResult.whenCompleteWithContext((r, t) -> {
        RuntimeException filterRuntimeException = null;
        for (int i = filters.size() - 1; i >= 0; i--) {      // 为什么：回调按"内层先、外层后"逆序触发，与 invoke 的进入顺序相反
            FILTER filter = filters.get(i);
            try {
                // ... 取 Listener：ListenableFilter.listener(invocation) 或 (FILTER.Listener) filter
                if (t == null) {
                    listener.onResponse(r, filterInvoker, invocation);  // 为什么：正常/业务异常都走这里，靠 appResponse.hasException() 区分
                } else {
                    listener.onError(t, filterInvoker, invocation);     // 为什么：框架异常（超时/网络/Filter 自身抛错）走这里
                }
                // ... listenableFilter.removeListener(invocation) 清理
            } catch (RuntimeException runtimeException) {
                // ... 记 CLUSTER_EXECUTE_FILTER_EXCEPTION，t = runtimeException
            }
        }
        if (filterRuntimeException != null) { throw filterRuntimeException; }
    });
    return asyncResult;
}
```

**拆解**：

### 6.1 两条链：ClusterFilter 在负载均衡之前，Filter 在实例之后

`Filter.java` 的 Javadoc（第 38-59 行）直接画了 3.x 与 2.x 的差别：3.x 消费端是 `Proxy -> ClusterFilter -> ClusterInvoker -> Filter -> Invoker`，2.x 是 `Proxy -> ClusterInvoker -> Filter -> Invoker`。注释写明 "Starting from 3.0, Filter on consumer side has been refactored"，并区分两类：`Filter` 工作在实例级，"each Filter is bond to one specific Provider instance(invoker)"（第 40 行）；`ClusterFilter` 是 3.0 新增的，"intercepts request before Loadbalancer picks one specific Filter(Invoker)"（第 41 行）。

对应到源码，`DefaultFilterChainBuilder` 有两个入口：`buildInvokerChain`（第 44 行，实例级）和 `buildClusterInvokerChain`（第 83 行，消费端集群级），后者用 `ClusterFilter.class` 取扩展，节点是 `CopyOfClusterFilterChainNode`，最外层是 `ClusterCallbackRegistrationInvoker`（第 112-114 行）。`dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/filter/support/` 下放的是 `ConsumerContextFilter`、`CallbackConsumerContextFilter`、`ConsumerClassLoaderFilter`、`MetricsConsumerFilter`——注意消费端的上下文设置在这里，而提供端的 `ContextFilter` 在 `dubbo-rpc-api/.../filter/`。`ProtocolFilterWrapper` 只负责实例级那条链（第 56、70 行），并且 `UrlUtils.isRegistry(...)` 时直接透传：注册中心的包装 Invoker 不套业务 Filter。

那 `ClusterFilter` 链到底在哪拼？不在 `ProtocolFilterWrapper`，而在 `AbstractCluster.join(directory, buildFilterChain)`（`dubbo-cluster/.../support/wrapper/AbstractCluster.java:58`）：`buildFilterChain` 为 true 时返回 `buildClusterInterceptors(doJoin(directory))`，否则直接 `doJoin`。`buildClusterInterceptors`（第 45 行）先 `new ClusterFilterInvoker<>(clusterInvoker)`，这个内部类的构造器（第 82-97 行）取所有 `FilterChainBuilder` 的激活扩展，逐个 `builder.buildClusterInvokerChain(tmpInvoker, REFERENCE_FILTER_KEY, CommonConstants.CONSUMER)` 串起来；外面再叠 `InvocationInterceptorBuilder` 生成的 `InvocationInterceptorInvoker`（第 66-75 行），并可通过 `CLUSTER_INTERCEPTOR_COMPATIBLE_KEY` 打开 2.7.x 兼容分支（第 48-53 行）。所以第 7 章里 `Cluster.join()` 产出的那个 `Invoker`，外包的是 `ClusterFilter` 链；`ProtocolFilterWrapper.refer()` 拼的是更里面、绑定单个实例的 `Filter` 链。两条链的构造点、扩展类型、作用域都不相同，读源码时按 `join()` 和 `refer()` 分开追才不会绕晕。

这个区分有实际后果：想做"每次 RPC 只执行一次"的事（埋点、上下文初始化、参数校验），应该写 `ClusterFilter`；挂在 `Filter` 上，在多实例、Failover 重试的场景里就会被执行多次。反过来，想针对某个具体 Provider 实例做统计/熔断，就得用 `Filter`。

### 6.2 拼链与递归：逆序包装、三个角色、AsyncRpcResult 约束

`buildInvokerChain` 的形状并不复杂，但三个角色各司其职。第一，`getActivateExtension(url, key, group)` 返回的 `List<Filter>` **已经排好序**（`ExtensionLoader` 里是 `TreeMap<>(activateComparator)`，`ExtensionLoader.java:346`）。第二，`for (int i = filters.size() - 1; i >= 0; i--)` 逆序包装：`last` 初始是 `originalInvoker`，第一次循环用 `filters[last]`（order 最大）包住真实 Invoker，最后一次用 `filters[0]`（order 最小）包在最外面。于是 `filters[0]` 的 `invoke` 前半段最先执行、后半段最后执行——这正是洋葱模型。第三，`CallbackRegistrationInvoker` 再套一层，专门负责回调。

节点 `CopyOfFilterChainNode` 的 `invoke`（`FilterChainBuilder.java:344`）就是递归本身：`filter.invoke(nextNode, invocation)`，`nextNode` 是下一层节点；每个 Filter 的 `invoke` 自己决定要不要 `invoker.invoke(invocation)`（`BaseFilter.java:21` 的约定原文是 "Always call invoker.invoke() in the implementation to hand over the request to the next filter node"）。不调就是短路：`TokenFilter`（`TokenFilter.java:42-56`）token 不匹配直接 `throw new RpcException`，根本没走到 `invoker.invoke(inv)`。节点对外伪装成同一个 Invoker：`getUrl()` / `getInterface()` / `isAvailable()` / `destroy()` 全部委托 `originalInvoker`（第 330-341、382-384 行），而不是 `nextNode`——这样链上任意一层拿到的 URL 都是原始 Invoker 的 URL，配置读取不会随层级漂移。

3.x 新增的硬约束在第 350-357 行：`asyncResult` 必须是 `AsyncRpcResult`，否则记 `INTERNAL_ERROR` 并抛 `RpcException`，提示语原文是 "The result of filter invocation must be AsyncRpcResult. (If you want to recreate a result, please use AsyncRpcResult.newDefaultAsyncResult.)"。所以自定义 Filter 里要造返回值只能用 `AsyncRpcResult.newDefaultAsyncResult(...)`，`FailsafeClusterInvoker`（第 61 行）、`FailbackClusterInvoker`（第 139 行）都是这么写的。

`FilterChainBuilder` 里其实并存两套节点实现，值得对照读：老的 `FilterChainNode`（第 61 行）在 `invoke` 里除了 `filter.invoke(nextNode, invocation)`，还直接 `return asyncResult.whenCompleteWithContext((r, t) -> ...)`（第 118-142 行），也就是**每个节点自己注册回调**——正常时 `listener.onResponse(r, originalInvoker, invocation)`，异常时 `listener.onError(t, ...)`，并在 `finally` 里 `listenableFilter.removeListener(invocation)`。新的 `CopyOfFilterChainNode`（第 311 行，标 `@Experimental`，注释说"replace FilterChainNode with this one when proved stable enough"）的 `invoke` 只返回 `asyncResult`（第 378 行），回调全部交给外层 `CallbackRegistrationInvoker`。这个改动的意义是：回调不再散落在每一层，异常路径与成功路径统一收口，且 `ListenableFilter` 的 `removeListener` 只在最外层执行一次。`DefaultFilterChainBuilder` 当前用的是 `CopyOf*` 这一套，但 `FilterChainNode` / `ClusterFilterChainNode` 仍留在源码里，读老文章里"每个 Filter 自己 whenComplete"的描述时别以为写错了，那是旧节点的行为。此外 `CallbackRegistrationInvoker` 的 catch 里（第 226-244 行）会把某个 Listener 抛出的 `RuntimeException` 记成 `CLUSTER_EXECUTE_FILTER_EXCEPTION`、`t = runtimeException` 并在循环结束 `throw filterRuntimeException`（第 246-248 行）——即回调里抛异常不会被静默吞掉，会替换掉原本的结果。

调用进入链之前，`AbstractClusterInvoker.invokeWithContext()`（`AbstractClusterInvoker.java:402`）会 `setContext(invoker)`、`setRemote(invoker, invocation)`，所以 `ExceptionFilter`（第 88 行）里的 `RpcContext.getServiceContext().getRemoteHost()` 才有值；`finally` 里 `clearContext(originInvoker)` 还原。这是第 5 章那条 Spring 链路和第 7 章集群链路共同喂给 Filter 的上下文。

### 6.3 @Activate 怎么决定"谁生效、谁先跑"

`ExtensionLoader.getActivateExtension(URL url, String[] values, String group)`（`ExtensionLoader.java:343`）是总入口。它用 `Map<Class<?>, T> activateExtensionsMap = new TreeMap<>(activateComparator)`（第 346 行）保证有序，把 `url.getParameter(key)` 按逗号拆成 `names`（第 328-330 行，`key` 就是 `SERVICE_FILTER_KEY` / `REFERENCE_FILTER_KEY`，即 `service.filter` / `reference.filter`）。筛选条件在第 394-402 行，四个 `&&`：`isMatchGroup(group, activateGroup)`（`group` 与 `@Activate.group()` 匹配）、`!namesSet.contains(name)`、`!namesSet.contains(REMOVE_VALUE_PREFIX + name)`（`-xxx` 剔除）、`isActive(cachedActivateValues.get(name), url)`（`value` 指定的 URL key 存在）。`value` 这一项就是"条件激活"：`@Activate(group = PROVIDER, value = TOKEN_KEY)` 的 `TokenFilter` 只有 URL 带 `token` 才生效，`ExecuteLimitFilter` 要 `executes`、`TpsLimitFilter` 要 `tps`（`org.apache.dubbo.rpc.Constants.TPS_LIMIT_RATE_KEY = "tps"`）、`ActiveLimitFilter` 要 `actives`、`DeprecatedFilter` 要 `deprecated`、`GenericImplFilter` 要 `generic`。此外 `-default`（第 351 行）一次性禁掉所有默认扩展，`default` 关键字（第 405-422 行）用来把自定义 Filter 插到默认链之前/之后——`ext1,default,ext2` 的语义源码注释写得很清楚。两个被 `getActivateExtension` 调用的判定也值得看一眼：`isMatchGroup(String group, Set<String> groups)`（`ExtensionLoader.java:460`）在 `group` 为空时直接放行，否则要求 `@Activate.group()` 里包含传入的 `PROVIDER` / `CONSUMER`；`isActive(String[][] keyPairs, URL url)` 解析 `value` 里 `key:value` 或纯 `key` 两种形式——纯 key 要求 URL 上存在该参数，`key:value` 还要求参数值匹配（第 375-387 行把 `TPS_LIMIT_RATE_KEY` 这类 value 拆成 `keyPairs` 缓存）。所以 `@Activate(value = ...)` 不是"打个标记"，而是运行时按 URL 逐条比对的开关：`token`、`executes`、`tps`、`actives` 这些参数没配，对应的 Filter 根本不会进链。重载链也顺一下：`getActivateExtension(URL, String key)`（第 303 行）→ `getActivateExtension(url, key, null)`（第 328 行）→ 把 `url.getParameter(key)` 用 `COMMA_SPLIT_PATTERN` 拆成 `String[] values` → 三参数版本（第 343 行）。`DefaultFilterChainBuilder` 传进来的 `key` 是 `SERVICE_FILTER_KEY` / `REFERENCE_FILTER_KEY`，`group` 是 `CommonConstants.PROVIDER` / `CONSUMER`，所以同一个 Filter 扩展在提供端和消费端是两次独立的 `getActivateExtension` 调用，各自得到一条链。另有 `getActivateExtensions()`（第 440 行）不过滤 URL 条件、直接返回全部 `@Activate` 扩展，`AbstractCluster.ClusterFilterInvoker` 取 `FilterChainBuilder` 列表用的就是它（`AbstractCluster.java:86`）——注意区分"取扩展点实现"和"取 Filter 实例"是两种不同调用。

排序由 `ActivateComparator.compare()`（`ActivateComparator.java:53`）决定：先处理 `before` / `after`（`a1.isLess(n2)` / `a1.isMore(n2)`，第 83-109 行），再 `return a1.order > a2.order ? 1 : -1`（第 111 行）；没有 `before`/`after` 时走第 114-123 行，order 相同用 `o1.getSimpleName().compareTo(o2.getSimpleName())` 兜底——注释说这是"为了避免两个 filter 在不同加载场景下顺序不一致"。所以不写 `order` 的两个 Filter，先后顺序由类名字母序决定，不要依赖 SPI 文件里的书写顺序。

把 `dubbo-rpc-api/.../filter/` 里的真实注解排一遍就很直观：`ContextFilter` 与 `ProfilerServerFilter` 是 `order = Integer.MIN_VALUE`（最外层，先建上下文/先开 profiler），`EchoFilter` -110000，`ClassLoaderFilter` -30000，`GenericFilter` -20000；一大批默认 order = 0 的（`@Activate(group = PROVIDER)` 的 `AccessLogFilter`、`ExceptionFilter`、`TimeoutFilter`，带 `value` 的 `TokenFilter`、`ExecuteLimitFilter`、`TpsLimitFilter`，以及 CONSUMER 侧的 `RpcExceptionFilter`、`ActiveLimitFilter`、`DeprecatedFilter`）；`GenericImplFilter` 是 `order = 20000`（消费端，靠后），`ClassLoaderCallbackFilter` 是 `order = Integer.MAX_VALUE`（最内层，最后收尾）。`TokenHeaderFilter` 干脆是空的 `@Activate`——`group`、`value` 都不写，两端都生效，纯靠 URL 条件之外无条件挂载。`AdaptiveLoadBalanceFilter` 用了多行 `@Activate`（`before`/`after` 形式）。这个布局的意图是：上下文和 ClassLoader 最先就位，业务语义的 Filter 居中，回调与 ClassLoader 还原放最后。跨模块的监控类不在这个目录：`MonitorFilter` 现在位于 `dubbo-metrics/dubbo-metrics-default`（`@Activate(group = {PROVIDER})`，`implements Filter, Filter.Listener`），另有 `MetricsFilter` / `MetricsClusterFilter`。

### 6.4 内置 Filter 分类，以及 onResponse 到底是给谁用的

按职责把内置 Filter 归一下类，注意每一类的 `group` 和触发条件：**超时**——`TimeoutFilter`（PROVIDER），Javadoc 明说 "Log any invocation timeout, but don't stop server from running"（第 36 行），`invoke` 里只 `return invoker.invoke(invocation)`，真正的判断放在 `onResponse`：从 `RpcContext.getServerAttachment().getObjectAttachment(TIME_COUNTDOWN_KEY)` 取 `TimeoutCountDown`，`countDown.isExpired()` 才打 `PROXY_TIMEOUT_REQUEST` warn（第 50-61 行）。它不掐断请求，掐断是消费端 timeout 与 `AsyncRpcResult` 的事，别混淆。**异常**——`ExceptionFilter`（PROVIDER，`implements Filter, Filter.Listener`，第 50 行）在 `onResponse` 里对 `appResponse.hasException()` 做一串"直接放行"判断（checked exception、方法签名声明的异常、同 jar、JDK 异常、`RpcException`），其余用 `appResponse.setException(new RuntimeException(StringUtils.toString(exception)))`（第 113 行）包一层，避免客户端反序列化失败；CONSUMER 侧对称的是 `RpcExceptionFilter`（`@Activate(group = CONSUMER)`，`implements Filter, Filter.Listener`，第 35-36 行），它的 `invoke` 只透传，全部逻辑在 `onResponse`：`appResponse.hasException()` 且 `exception instanceof RpcException` 时 `throw (RpcException) exception`（第 44-51 行）。注意这是在 `onResponse` **里抛异常**——结合 `CallbackRegistrationInvoker` 的 catch 块（第 226-244 行），这个 `RpcException` 会被记日志并替换掉结果，最终冒泡给 `AbstractClusterInvoker`。这正是"业务异常改写"和"框架异常上抛"两条路径在消费端合流的地方，也再次说明 `onResponse` 不等于"一定成功返回"。**Token**——`TokenFilter`（PROVIDER，value=TOKEN_KEY）比对 `inv.getObjectAttachmentWithoutConvert(TOKEN_KEY)`，不等就抛 `RpcException`（第 46-53 行），配 `TokenHeaderFilter`。**AccessLog**——`AccessLogFilter`（PROVIDER），`dubbo.accesslog` 开关，`ConcurrentMap<String, Queue<AccessLogData>>` 缓冲、`LOG_OUTPUT_INTERVAL = 5000` 定时刷（第 77-91 行）。**泛化**——`GenericFilter`（PROVIDER，order=-20000）把 `$invoke` 的参数反序列化成真实方法调用，`GenericImplFilter`（CONSUMER，order=20000）反向，两者都在 `onResponse` 里做结果序列化。**上下文**——`ContextFilter`（PROVIDER，`Integer.MIN_VALUE`）在 `invoke` 里把 invocation attachments 灌进 `RpcContext.getServerAttachment()`，并在 `onResponse` / `onError` 里 `removeServerAttachment()` / `removeClientAttachment()` / `removeServiceContext()` 等（第 225-229 行）。**监控**——3.3 挪到 `dubbo-metrics-default` 的 `MonitorFilter` / `MetricsFilter` / `MetricsClusterFilter`。**限流**——`ActiveLimitFilter`（CONSUMER，ACTIVES_KEY）用 `RpcStatus.beginCount` + `synchronized (rpcStatus)` 里 `rpcStatus.wait(remain)` 等待并发额度（第 56-80 行）；`ExecuteLimitFilter`（PROVIDER，EXECUTES_KEY）超了直接 `LIMIT_EXCEEDED_EXCEPTION`（第 50-55 行）；`TpsLimitFilter`（PROVIDER，TPS_LIMIT_RATE_KEY）管 TPS。剩下的几个补齐分类：`EchoFilter`（PROVIDER，`order = -110000`）拦截 `$echo` 回声测试；`DeprecatedFilter`（CONSUMER，value=DEPRECATED_KEY）对标记废弃的方法打日志；`ClassLoaderFilter`（PROVIDER，`order = -30000`）在调用前把线程上下文 ClassLoader 换成 `invoker.getUrl()` 对应的那个，`ClassLoaderCallbackFilter`（PROVIDER，`order = Integer.MAX_VALUE`）在最内层负责换回来——两个一前一后正好夹住整条链；`ProfilerServerFilter`（PROVIDER，`Integer.MIN_VALUE`）与 `ContextFilter` 并列最外层，负责调用链 profiler；`CompatibleFilter`、`TokenHeaderFilter`、`AdaptiveLoadBalanceFilter` 分别处理兼容、token 头透传与自适应负载均衡。数一下 `dubbo-rpc-api/.../filter/` 目录：`AccessLogFilter`、`ExecuteLimitFilter`、`ActiveLimitFilter`、`AdaptiveLoadBalanceFilter`、`ClassLoaderCallbackFilter`、`ClassLoaderFilter`、`GenericFilter`、`GenericImplFilter`、`ProfilerServerFilter`、`RpcExceptionFilter`、`TimeoutFilter`、`TokenFilter`、`TokenHeaderFilter`、`TpsLimitFilter`、`ContextFilter`、`DeprecatedFilter`、`EchoFilter`、`ExceptionFilter`、`CompatibleFilter` 共十九个，加上 `dubbo-metrics-default` 的 `MonitorFilter` / `MetricsFilter` 与 `dubbo-cluster/.../filter/support/` 的四个 `ClusterFilter`，才是 3.3 的完整内置链。

`ListenableFilter`（`ListenableFilter.java:29`）在 3.3 已经 `@Deprecated`。它做的是"一次 RPC 一个 Listener"：内部 `ConcurrentMap<Invocation, Listener> listeners`，暴露 `listener(invocation)` / `addListener(invocation, listener)` / `removeListener(invocation)`，Javadoc（第 23-27 行）明确说"It's recommended to implement Filter.Listener directly for callback registration, check the default implementation, see ExceptionFilter"，并解释只有"不想在 RPC 之间共享 Listener 实例"时才用它。`FilterChainBuilder` 里两种形态都兼容：`filter instanceof ListenableFilter` 走 `listenableFilter.listener(invocation)` 并在 `finally` 里 `removeListener`，`filter instanceof FILTER.Listener` 直接强转（第 360-373、204-225 行）。

`BaseFilter.Listener`（`BaseFilter.java:32-53`）的两个回调语义必须分清：`onResponse(Result appResponse, ...)` 的 Javadoc 说"This method will only be called on successful remote rpc execution"，且 `appResponse` "can represent both normal result and exceptional result"（第 38 行）——也就是说**业务异常走 `onResponse`，要用 `appResponse.hasException()` 自己判**（`ExceptionFilter` 第 60 行、`TimeoutFilter` 第 49 行都是这个模式）；`onError(Throwable t, ...)` 是"framework exceptions, for example, TimeoutException, NetworkException, Exception raised in Filters"（第 45-51 行）。`BaseFilter.java:29-30` 还有一句对迁移最关键的提示："try to move logics previously defined in the 'finally block' to both onResponse and onError"。这就解释了为什么 `ActiveLimitFilter` 的 `RpcStatus.endCount` 同时写在 `onResponse`（第 88-96 行）和 `onError`（第 98-112 行）、`ContextFilter` 的 `removeXxx` 两边都写、`ExecuteLimitFilter` 的 `endCount` 两边都写（第 70-84 行）——少写一个，成功/失败必有一路漏计数或漏清理。`CallbackRegistrationInvoker` 的 `whenCompleteWithContext` 里 `t == null` 走 `onResponse`、`t != null` 走 `onError`，且循环是 `for (i = filters.size()-1; i >= 0; i--)`（第 200 行），回调顺序与 `invoke` 的进入顺序相反，也是洋葱的"出栈"。

为什么 Filter 里不能吞异常？从源码能给出四层理由：其一，`CopyOfFilterChainNode.invoke` 的 `catch` 里先 `listener.onError(...)` 再 `throw e`（第 358-374 行），链的设计前提就是异常会一直冒泡到最外层；吞掉后 `whenCompleteWithContext` 的 `t == null` 分支成立，所有 `onError` 都不触发，`RpcStatus.endCount`、`RpcContext.removeXxx`、异常日志全部错位。其二，集群容错完全靠这个信号：`FailoverClusterInvoker.doInvoke` 只在 `catch (RpcException e)` 且 `e.isBiz()` 时直接抛，其余（超时、网络）都当作可重试失败（`FailoverClusterInvoker.java:103-109`）；Filter 返回一个"看起来正常"的 `AsyncRpcResult`，消费端 `AbstractClusterInvoker` 就看不到失败，Failover 不会重试、Failback 不会补发、Failsafe 那类策略也失去意义。其三，第 350-357 行的 `AsyncRpcResult` 检查：吞异常后若顺手 `return new AppResponse()`，节点直接抛 `RpcException`，反而变成更难定位的框架错。其四，合法做法只有三种——不 catch；`throw new RpcException(...)` 原样/包装抛出；或用 `AsyncRpcResult.newDefaultAsyncResult(exception, invocation)` 把异常放进 Result（此时走 `onResponse` 的 `hasException()`，仍是"显式表达失败"）。`ExceptionFilter` 的 `appResponse.setException(...)`（第 113 行）是"改写异常"而不是"吞异常"的范例。至于 `FailsafeClusterInvoker` 的 `return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation)`（第 61 行），那是**集群层**显式选择的容错策略，不是 Filter 该干的事——层次别搞反。

**容易误解的地方**：

1. **3.3 的 `ProtocolFilterWrapper` 里已经没有 `buildInvokerChain` 方法了**，而且类在 `dubbo-cluster/src/main/java/org/apache/dubbo/rpc/cluster/filter/`，不在 `dubbo-rpc-api`。2.x 那个"私有 `buildInvokerChain` + 匿名 `Invoker` 递归 + `invoker.invoke` 前后手写逻辑"是老知识；现在真正拼链的是 `DefaultFilterChainBuilder.buildInvokerChain`（第 44 行），节点是 `CopyOfFilterChainNode`，最外层还多了一个 `CallbackRegistrationInvoker`。
2. **`ListenableFilter` 已废弃，别再继承它写新 Filter**。3.3 推荐直接 `implements Filter, Filter.Listener`（`ExceptionFilter`、`TimeoutFilter`、`ActiveLimitFilter` 都是这么写的）。而且 `onResponse` 不是"业务成功回调"：它同样会收到带业务异常的 `appResponse`，框架异常才走 `onError`；只实现其中一个回调，计数和上下文清理必然漏一半。
3. **`order` 相同不代表顺序稳定，更不代表按 SPI 文件顺序**。`ActivateComparator` 在 order 相等时用 `getSimpleName().compareTo(...)` 兜底（`ActivateComparator.java:120`）；`@Activate` 的 `group` 只是筛选、`value` 是 URL 条件，两者都不写（如 `TokenHeaderFilter`）就默认两端都生效。
4. **Filter 和 ClusterFilter 是两条链**。`Filter.java:40-41` 写得很清楚：`Filter` 绑定单个 Provider 实例，`ClusterFilter` 在负载均衡选 Invoker 之前。把"每调用一次"的埋点写在 `Filter` 上，遇到 Failover 重试或多实例就会重复执行；反过来想按实例做统计，也不能挂到 `ClusterFilter`。

**记住这一句**：Filter 链的每一层只负责"进"和"出"，异常必须原样往外抛，回调统一由最外层的 CallbackRegistrationInvoker 收口。
