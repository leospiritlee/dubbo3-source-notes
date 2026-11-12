# 第1章 全局地图：一次调用经过哪些类

**一句话**：一次 RPC 是十几个接口的接力，每个接口只负责一件事。

**为什么需要它**：

先看四个真实的问题：

1. 你在 `@DubboReference(timeout = 300)` 上写了 300ms，压测还是卡 3 秒。
2. 你在 `Filter` 里调 `RpcContext.getServiceContext().getRemoteAddress()`，拿到 `null`。
3. 你写了个 `@Activate(group = CONSUMER)` 的 Filter，注册进 `META-INF/dubbo` 了，日志里一次都没打印。
4. 一个"创建订单"接口开了 `retries=2`，超时后重试，数据库里出现两条订单。

四个问题的共同点不是"某个类有 bug"，而是你不知道这个参数、这个对象、这个扩展点**在哪一层被读、被谁覆盖、在哪个时刻被构造**。Dubbo 有 3915 个 java 文件，但一次调用真正经过的类不超过二十个。这一章把这二十个挑出来按顺序摆好，标清每层的接口、典型实现，以及它唯一负责的那件事。

读源码最常见的浪费，是在 `dubbo-common` 里泡三天（那里有 URL、SPI、线程池、字节码），却没搞清 `dubbo-rpc` 里那几个接口的关系。`Invoker`、`Protocol`、`ProxyFactory`、`Filter`、`Exporter`、`Result` 加起来不到三百行代码，但它们定义了整条链的形状。先把骨架立住，再去填肉。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `ProxyFactory` | 双向翻译。`getProxy(Invoker)` 把 Invoker 变成业务接口代理（消费端），`getInvoker(proxy, type, url)` 把业务对象包成 Invoker（提供端）。`@SPI("javassist")`，生成失败自动回退 JDK 动态代理 |
| `Invoker<T>` | 框架里唯一的"可调用体"。只有 `getInterface()` 和 `invoke(Invocation)` 两个方法，`extends Node` 拿到 `getUrl()` / `isAvailable()` / `destroy()`。链上每一层都是 Invoker 包 Invoker |
| `Invocation` | 一次调用的全部输入：方法名、参数类型、实参、attachment、`InvokeMode`。唯一实现是 `RpcInvocation` |
| `Filter` | 实例级拦截，挂在**单个 provider invoker** 上，不在 Cluster 外层 |
| `ClusterFilter` | 3.0 新增，在选机器**之前**拦截，一个服务一条链 |
| `Cluster` / `ClusterInvoker` | 失败策略：failover / failfast / failsafe / failback / forking / broadcast / available。**不负责选机器** |
| `Directory` | provider 列表的来源。`list(invocation)` 时才跑路由，不是缓存快照 |
| `Router` / `RouterChain` | 按规则裁剪 provider 列表：tag、condition、script、mesh、state |
| `LoadBalance` | 从候选里选一个：random / roundrobin / leastactive / consistenthash / shortestresponse / adaptiveloadbalance |
| `Protocol` | `export` / `refer` 的边界，唯一知道"用什么协议通信"的层。`@SPI("dubbo")` |
| `Exporter` | export 的返回值，持有 Invoker，负责 `unexport` / `register` |
| `Result` | 调用结果。`AsyncRpcResult` 是 `CompletionStage`，`AppResponse` 是它的载荷 |
| `ExchangeClient` | 请求/响应语义：一个 `Request` 换一个 `CompletableFuture` |
| `Codec` | 字节 ↔ `Request` / `Response`。dubbo 协议头 16 字节，低 5 位存序列化 ID |
| `Transporter` | 真正的 socket。默认 netty4 |
| `MigrationInvoker` | 3.x 特有的一层，同时持有接口级和实例级两个 ClusterInvoker，按迁移规则切换 |

**关键代码**：

第一个片段是 Cluster 层的全部工作。它做四件事，但没有一件是"选机器"：

```java
// AbstractClusterInvoker.java:345
public Result invoke(final Invocation invocation) throws RpcException {
    checkWhetherDestroyed();

    InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Router route.");
    List<Invoker<T>> invokers = list(invocation);        // 355: 走 Directory → RouterChain，每次调用都算
    InvocationProfilerUtils.releaseDetailProfiler(invocation);

    checkInvokers(invokers, invocation);                 // 358: 空列表在这里就炸，轮不到 LoadBalance
    LoadBalance loadbalance = initLoadBalance(invokers, invocation);  // 360: 按第一个 invoker 的 url 取 LB
    RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);         // 361: 只有异步调用才塞 invocationId

    InvocationProfilerUtils.enterDetailProfiler(
            invocation, () -> "Cluster " + this.getClass().getName() + " invoke.");
    try {
        return doInvoke(invocation, invokers, loadbalance);  // 366: 子类决定重试几次、并行几次
    } finally {
        InvocationProfilerUtils.releaseDetailProfiler(invocation);
    }
}

// AbstractClusterInvoker.java:451
protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
    return getDirectory().list(invocation);              // 只有一行，背后是整条 RouterChain
}
```

第二个片段是链路的最末端。同步调用在这里已经变成异步了——`doInvoke` 返回 Future 而不是值：

```java
// DubboInvoker.java:121
Request request = new Request();
if (payload != null) {
    request.setPayload(payload);       // 122-124: 单请求载荷上限，超了在提供端拆包
}
request.setData(inv);                  // 125: 载荷就是 RpcInvocation 本身，不是"参数数组"
request.setVersion(Version.getProtocolVersion());

if (isOneway) {
    request.setTwoWay(false);
    currentClient.send(request, isSent);                       // 131: 单向调用，发完就走
    return AsyncRpcResult.newDefaultAsyncResult(invocation);
} else {
    request.setTwoWay(true);
    ExecutorService executor = getCallbackExecutor(getUrl(), inv);   // 135: 回调线程池，不是业务线程池
    CompletableFuture<AppResponse> appResponseFuture =
            currentClient.request(request, timeout, executor).thenApply(AppResponse.class::cast);  // 136-137
    AsyncRpcResult result = new AsyncRpcResult(appResponseFuture, inv);
    result.setExecutor(executor);
    return result;                                              // 144: 同步方法也返回 Future
}

// HeaderExchangeChannel.java:153 —— 请求真正发出去的地方
DefaultFuture future = DefaultFuture.newFuture(channel, req, timeout, executor);  // 153: 先登记 future
try {
    channel.send(req);                                                            // 155: 再发
} catch (RemotingException e) {
    future.cancel();
    throw e;
}
```

**先登记再发送**。反过来的话，响应可能在登记之前就回来了，`DefaultFuture` 按 request id 找不到对应的 future，这次调用只能等超时。这是所有 RPC 框架的固定写法，也是"偶发超时但服务端日志显示耗时 2ms"的经典成因之一。

**拆解**：

### 1.1 链是启动时搭好的，不是调用时拼的

消费端 `ReferenceConfig` 的初始化路径只有一条：

```
ReferenceConfig.java:522   proxyFactory.getProxy(invoker, ProtocolUtils.isGeneric(generic))
```

那个 `invoker` 从哪来？`protocolSPI.refer(type, url)`，此时 url 的协议是 `registry`：

```
Protocol$Adaptive → getExtension("registry")
  → InvokerCountWrapper → ProtocolSerializationWrapper → ProtocolFilterWrapper
  → ProtocolSecurityWrapper → ProtocolListenerWrapper → RegistryProtocol
    RegistryProtocol.java:577  doRefer(...)
    RegistryProtocol.java:592  getMigrationInvoker(...)   // ServiceDiscoveryMigrationInvoker
    RegistryProtocol.java:668  cluster.join(directory, true)
      AbstractCluster.java:58  join → buildClusterInterceptors(doJoin(directory))
      AbstractCluster.java:46  → InvocationInterceptorInvoker(ClusterFilterInvoker(FailoverClusterInvoker))
```

`MigrationInvoker` 这一层是 3.x 独有的，值得单独说一句。它内部有三个字段：`invoker`（接口级，`RegistryDirectory`）、`serviceDiscoveryInvoker`（应用级，`ServiceDiscoveryRegistryDirectory`）、`currentAvailableInvoker`（`MigrationInvoker.java:64-66`）。`invoke`（285）只做一件事——转发给 `currentAvailableInvoker`（296 / 311）。3.0 到 3.2 的注册模型迁移就是靠这一层平滑完成的：两套地址发现同时在跑，按比例切流量，切完把旧的那套 destroy 掉。你在排查"为什么同一个服务有两个 provider 列表"时，答案通常在这里。

所以启动结束时，代理对象背后已经是一棵固定的树：`InvocationInterceptorInvoker → ClusterFilter 链 → FailoverClusterInvoker → MigrationInvoker → Directory`。运行时只做"沿着树往下走"，不再有任何"查找扩展点"的动作。

记住这棵树的形状，最大的收益是**定位问题有了坐标系**。每个节点都实现 `toString`，而且都是"接口名 → url"的格式（`AbstractClusterInvoker.java:381-384`），所以在任意一层打断点、打印 `invoker`，你能直接看出当前在哪一层、url 是哪一份。排查"参数没生效"时，第一件事就是在这几层各打一次 url，看它是在哪一层被换掉的——这比在 3915 个文件里猜快得多。

还有一个容易被忽略的细节：`AbstractProxyFactory.getProxy` 会往代理的接口列表里额外塞两个接口——`EchoService` 和 `Destroyable`（`AbstractProxyFactory.java:45`、`99`）。所以你的业务接口代理天然支持 `$echo()` 和 `$destroy()`，这也是为什么 `InvokerInvocationHandler.invoke` 里要特判 `"$destroy"`（60）。

每个 provider 对应的 `DubboInvoker` 是另一条支线，在 `RegistryDirectory.toInvokers`（440）里由 `protocol.refer(serviceType, url)`（473）创建——**这时候** `ProtocolFilterWrapper.refer`（75）才把消费端 Filter 链套上去。也就是说，消费端 Filter 实例数 = provider 数 × 启用的 Filter 数，不是"一个服务一条"。500 个 provider、8 个 Filter，就是 4000 个 chain node 常驻堆内存。这也是为什么 Dubbo 的 Filter 文档反复强调"每加一个 Filter 都要考虑开销"。

### 1.2 消费端一次 invoke 的十二跳

按顺序，每一跳都标了类和行号，可以对着断点走：

1. **代理对象** → `InvokerInvocationHandler.invoke`（51）。先短路 `Object` 方法：`toString` / `hashCode` / `equals` / `$destroy`（52-68），否则构造 `RpcInvocation`（69）。构造参数是六元组：`serviceModel`、方法名、接口全名、`protocolServiceKey`、参数类型、实参。注意这里还往 attributes 里塞了 `CONSUMER_MODEL` 和 `METHOD_MODEL`（78-79），后面 Filter 和监控靠它拿元数据，不走反射。
2. `InvocationUtil.invoke`（39）。`RpcContext.storeServiceContext()` 保存现场，`setConsumerUrl`（48），最后 `invoker.invoke(rpcInvocation).recreate()`（102）。**同步阻塞就发生在这一行的 `.recreate()`**，不在 `DubboInvoker` 里。
3. `InvocationInterceptorInvoker` → ClusterFilter 链。节点类型是 `CopyOfClusterFilterChainNode`（`DefaultFilterChainBuilder:112`）。
4. `FailoverClusterInvoker.doInvoke`（59）。`calculateInvokeTimes`（130）= `retries + 1`；每次重试都 `list(invocation)` 重新取列表（73），因为地址可能刚变过。
5. `AbstractClusterInvoker.select`（155）→ `doSelect`（187）。粘滞（sticky）逻辑在这里，`stickyInvoker` 命中就跳过负载均衡（172）。
6. `AbstractDirectory.list`（201）→ `routerChain.getSingleChain`（223）→ `SingleRouterChain.route`。路由是**每次调用**都跑的。
7. `loadbalance.select(invokers, getUrl(), invocation)`（199）。选中的 invoker 如果不可用，会 `invalidateInvoker`（206）再 `reselect`（210）；`reselect` 最多挑 `reselectCount` 个候选，避免 provider 上千时全量遍历。
8. 选中 invoker 的消费端 Filter 链（`CopyOfFilterChainNode`）→ `DubboInvoker.doInvoke`（90）。
9. 多连接轮询：`exchangeClients.get(index.getAndIncrement() % size)`（101）。`connections=0` 时走共享连接（`getSharedClient:481`），否则每个服务独占。
10. `HeaderExchangeClient.request`（107）→ `HeaderExchangeChannel.request`（135）→ `DefaultFuture.newFuture`（153）。
11. `channel.send` → `NettyChannel` → `NettyCodecAdapter.InternalEncoder.encode`（66）→ `codec.encode`（86）→ `DubboCodec.encodeRequestData`（279）。序列化顺序是：协议版本、服务名、服务版本号、方法名、参数类型描述、逐个参数、attachment（283-300）。
12. 响应回来：`DubboCodec.decodeResponse` 完成 future → `AsyncRpcResult.getAppResponse()` 拿到 `AppResponse` → `recreate`（238）按 `InvokeMode` 分三种返回：FUTURE 返回 `RpcContext` 里的 Future，ASYNC 返回默认值（`createDefaultValue`），SYNC 阻塞取值。

这条链上有两处会"吞掉"你的参数，值得记住：

**attachment 的传递**。`RpcInvocation` 里的 attachment 不是自动跟着 `RpcContext` 走的。`RpcContext.getClientAttachment()` 里的东西要靠 `ConsumerContextFilter` 之类的 Filter 在调用前合并进 `invocation`，到了提供端再由 `ContextFilter` 还原到 `RpcContext.getServerAttachment()`。所以"我 set 了 attachment 但提供端读不到"通常不是序列化问题，是那一段 Filter 没启用或者被 `-context` 排除了。

**重试的边界**。`FailoverClusterInvoker` 捕获 `RpcException` 后先判断 `e.isBiz()`（104），业务异常直接抛出、不重试；只有框架异常（超时、网络、序列化）才进重试循环。所以第 4 个问题的正解不是关重试，而是让业务异常继承 `RpcException` 的 biz 语义，或者给写操作单独配 `cluster=failfast`。

**三种调用模式在这一层分叉**。`RpcUtils.isOneway`（105）为真时走 `send` 分支，返回一个已经完成的 `AsyncRpcResult`，永远拿不到异常——单向调用丢包不会报错。同步和异步走同一个 `request` 分支，区别只在最后 `recreate`（238）怎么解释这个 future：FUTURE 模式把 future 还给你，ASYNC 模式返回默认值，SYNC 模式阻塞。也就是说网络层根本不知道你是同步还是异步，这个区分是"消费端怎么读结果"，不是"怎么发请求"。

### 1.3 提供端：从字节到业务方法

Netty 的 pipeline 在 `NettyServer.initServerBootstrap`（185-188）里固定为四段：

```
negotiation(SslServerTlsHandler) → decoder → encoder → server-idle-handler → handler(NettyServerHandler)
```

1. `NettyCodecAdapter.InternalDecoder.decode`（94）循环调用 `codec.decode`（103）。返回 `NEED_MORE_INPUT` 时把 `readerIndex` 复位再 break（104-106）——半包就等下一批字节，不抛异常。
2. 解出来的是 `DecodeableRpcInvocation`，它同时实现 `Codec` 和 `Decodeable`（`DecodeableRpcInvocation.java:66`）。**解码时机是可控的**：默认在 IO 线程里就地 `inv.decode()`（`DubboCodec.java:200`）；只有在隔离线程池模式（`isDecodeDataInIoThread:231` 返回 false）下才不立即解码，把 `decode()` 推迟到业务线程，由 `DecodeHandler` 触发（`DecodeHandler.java:61`）。所以"参数反序列化占的是 IO 线程还是业务线程"取决于部署模式，不是固定的。
3. `NettyServerHandler.channelRead` → `handler.received`。这个 handler 是 `ChannelHandlers.wrap`（44-47）包出来的：`MultiMessageHandler → HeartbeatHandler → Dispatcher.dispatch(handler, url)`。默认 dispatcher 是 `all`，即"请求派发到业务线程池"；`Dispatcher` 本身也是 SPI，可以换成 `direct`（IO 线程直接跑）、`message`（按请求数派发）等。
4. `HeaderExchangeHandler.received`（196）→ `handleRequest`（88）→ `handler.reply(channel, msg)`（110）。这里把返回值写进 `Response`：成功 `setStatus(Response.OK)` + `setResult`，失败 `SERVICE_ERROR` + `setErrorMessage`（114-119），最后 `channel.send(res)`（120）。
5. `DubboProtocol.requestHandler.reply`（117）。`getInvoker`（131，实现 287）用 `path + version + group + port` 拼 `serviceKey`，去 `exporterMap` 查（310），查不到直接抛 `Not found exported service: ... may be version or group mismatch`，并把 `exporterMap.keySet()` 一起打出来。这个异常信息是全框架里最有用的几条之一。
6. `invoker.invoke(inv)`（167）→ 提供端 Filter 链（`ProtocolFilterWrapper.export:61` 建的，group=provider）→ `AbstractProxyInvoker.invoke`（87）→ `doInvoke`（100）→ `Wrapper.invokeMethod`（89）。最后一步是 javassist 生成的 `Wrapper` 直接调 `proxy.method(args)`，没有反射。
7. 返回值包成 `AppResponse`，`HeaderExchangeHandler` 在 future 完成时回写 `Response`，`DubboCodec.encodeResponseData` 编码。注意返回值的三种形态：`RESPONSE_NULL_VALUE`、`RESPONSE_VALUE`、`RESPONSE_WITH_EXCEPTION`，以及 3.x 加的带 attachment 的变体（`RESPONSE_NULL_VALUE_WITH_ATTACHMENTS` 等，`DubboCodec.java:313` 附近）。

提供端这条链上最反直觉的一点：**`invoker.invoke` 拿到的不是 `DubboInvoker` 也不是 `AbstractProxyInvoker`，而是 Filter 链的最外层**。所以在 `TimeoutFilter` 里 `invoker.invoke` 之后做的事，是在业务方法返回之后、序列化之前。想统计"业务耗时"要卡在这个位置，卡在 `reply` 外面会把序列化也算进去。

异常的回传也在这条链上被改写过。`ExceptionFilter`（`@Activate(group = PROVIDER)`）在 `onResponse` 里检查异常类型：checked 异常、方法签名里声明过的异常、JDK 异常、`RpcException` 都原样放过（65-110）；剩下的"未声明的运行时异常"，会被替换成 `new RuntimeException(StringUtils.toString(exception))`（113）——把原始堆栈压成字符串，这样消费端不需要有这个异常类也能拿到信息。所以"服务端抛的是 `MyBizException`，客户端收到的是 `RuntimeException`"不是 bug，是设计。

### 1.4 两条链路靠什么对齐

只有一个东西：`serviceKey`。

```java
// AbstractProtocol.java:84
protected static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
    return ProtocolUtils.serviceKey(port, serviceName, serviceVersion, serviceGroup);
}
```

消费端在 `DubboInvoker.doInvoke` 里把 `path`、`version` 写进 attachment（93-94），提供端在 `DubboProtocol.getInvoker`（308）用同样的四项拼 key。任何一项对不上，就是"提供端明明启动了，消费端说找不到服务"。

`timeout` 也是同理。`DubboInvoker.doInvoke` 里 `RpcUtils.calculateTimeout(getUrl(), invocation, methodName, DEFAULT_TIMEOUT)`（107），读的是 **invoker 的 url**，不是注解上的值。而 invoker 的 url 在 refer 时由 `RegistryDirectory.mergeUrl`（452）把 provider url 和 consumer 参数合并固化。参数优先级大致是：

```
方法级（methods.xxx.timeout） > 接口级（timeout） > 全局默认（DEFAULT_TIMEOUT = 1000）
```

所以"改注解不生效"的典型原因不是优先级算错，而是 `ReferenceConfig` 被缓存复用了——同一个 `interface + group + version` 只 init 一次，第二次改参数不会重新 refer，url 还是旧的。这类问题只能靠 `dump` 出来的 url 确认，看代码看不出来。

那份 url 是怎么拼出来的，`mergeUrl`（600）写得很清楚：先 `ClusterUtils.mergeUrl(providerUrl, queryMap)` 把消费端参数并进 provider url，再 `overrideWithConfigurator` 应用注册中心下发的 override 规则。所以最终那份 url 上，`timeout` 可能来自三个地方——消费端配置、provider 端配置、注册中心的动态覆盖。`getMethodParameter` 只是最后读它的人，不是决定它的人。

**容易误解的地方**：

1. **消费端 Filter 不在 Proxy 后面第一层。** `ProtocolFilterWrapper.refer`（71）第一句是 `if (UrlUtils.isRegistry(url)) return protocol.refer(type, url);`——registry 协议的 url 直接放行，不建链。真正的链在 `RegistryDirectory.toInvokers`（473）里，套在每个 `DubboInvoker` 上。所以 Filter 的位置是 **LoadBalance 之后、网络之前**。想在选机器之前拦截，只能用 `ClusterFilter`（`AbstractCluster:92` 的 `buildClusterInvokerChain`）。这也是为什么"灰度路由"类需求必须用 Router 或 ClusterFilter，普通 Filter 拿不到"候选列表"。顺带一提：同一个 Filter 类如果 `@Activate(group = {CONSUMER, PROVIDER})`，会在两端各执行一次，中间隔着序列化和网络往返，两边的 `Invocation` 也不是同一个对象。

2. **Cluster 不管选机器。** `FailoverClusterInvoker.doInvoke` 全程只做三件事：算次数、调 `select`、捕异常。选谁在 `LoadBalance`（`AbstractClusterInvoker:199`）。"failover 会换机器重试"的准确表述是：failover 每次循环都重新 `select` 并传入 `invoked` 列表（77-78），由 `doSelect` 的 `isSelected` 判断（202）避开刚失败的那台；如果候选只有一台，它还是会选同一台。

3. **Directory 不是本地缓存。** `AbstractDirectory.list`（201）每次都拿 `validInvokers.clone()`（217）再过 RouterChain，还额外加了读锁（213、224）配合 RouterChain 的双缓冲切换（`RouterChain.getSingleChain:100`）。名字叫 Directory，行为是"查询"。在 Router 里做 IO，会直接打到每一次调用的延迟上。

4. **同步调用内部也是异步的。** `DubboInvoker.doInvoke` 无条件返回 `AsyncRpcResult`（142），阻塞点在 `InvocationUtil` 的 `.recreate()`（102）→ `AsyncRpcResult.getAppResponse()`。这就是为什么 `RpcContext.getClientAttachment().getFuture()` 在同步调用里也能拿到东西——future 一直都在，只是被 `recreate` 消费掉了。

**记住这一句**：Dubbo 的一次调用，是启动时用 URL 拼好的一棵 Invoker 树，在运行时被从上到下走一遍。

---
