## 序：为什么读 Dubbo 的源码

会用 Dubbo 和懂 Dubbo 之间，隔着一整层接口。

加个 `@DubboService` 就能发布服务，加个 `@DubboReference` 就能调远程方法，`retries`、`timeout`、`loadbalance` 几个参数配一配就能上线。可一旦出问题——"服务启动了但消费端报 No provider available"、"重试之后订单被创建了两次"、"灰度发布请求还是打到了老版本"、"压测时 `selector.select()` 一直空转"——配置项就不管用了，你得知道这些参数最终落到了哪个类的哪一行。

这本书基于 **Apache Dubbo 3.3 源码**（3900+ 个 Java 文件）写成，不是二手文章的汇编。全书 11 章，按"一条调用链从上往下"的顺序组织：

| 部分 | 章节 | 回答的问题 |
|---|---|---|
| 骨架 | 1–2 | 一次调用经过哪些接口？为什么每个可变行为都是 SPI？ |
| 配置层 | 3–5 | 一个注解怎么变成监听端口和代理对象？Spring 在哪一步接进来？ |
| 集群层 | 6–9 | Filter、容错、负载均衡、路由分别在链路的哪一段生效？ |
| 通信层 | 10–11 | Netty 之上还包了几层？16 字节头里装了什么？ |

> 读法建议：第 1 章是地图，务必先读；第 2 章 SPI 是所有后续章节的前置；其余九章可以按需跳读，每章自包含，都有结构表、真实源码片段和"容易误解的地方"。

## 一、两条链路速查

一次同步调用，消费端把方法调用逐层往下翻译成字节，提供端再逐层往上翻译回来。中间那一层网络是唯一跨越进程的地方。

| 层 | 消费端 | 提供端 |
|---|---|---|
| 业务代码 | 调用接口方法 | 执行实现类方法 |
| 代理 | `ProxyFactory.getProxy` 生成的代理对象 | `ProxyFactory.getInvoker` 包出的 `AbstractProxyInvoker` |
| 调用入口 | `InvokerInvocationHandler.invoke` | `DubboProtocol` 的 `requestHandler` |
| Filter | `ClusterFilter` 链 → 实例级 `Filter` 链 | `Filter` 链（`ProtocolFilterWrapper` 拼装） |
| 集群 | `ClusterInvoker`（Failover 等） | — |
| 目录 | `RegistryDirectory` / `RouterChain` | — |
| 选路 | `LoadBalance.select` | — |
| 协议 | `DubboInvoker` → `ExchangeClient` | `DubboExporter` → `ExchangeServer` |
| 交换层 | `HeaderExchangeChannel.request` → `DefaultFuture` | `HeaderExchangeHandler.received` |
| 传输层 | `NettyClient` / `NettyChannel` | `NettyServer` / `NettyChannel` |
| 编解码 | `DubboCountCodec` → `ExchangeCodec` → `DubboCodec` | 同一套，方向相反 |
| 序列化 | `Hessian2Serialization` 等（URL 参数选定） | 同一套 |

关键分界：**ClusterInvoker 之前是"选谁"，之后是"怎么发"**；`Exchange` 层之上是"一次请求-响应"的语义，之下是"一条字节流"。很多配置不生效，第一件事就是判断它属于哪一侧。

## 二、11 章一句话速查

<div class="refsec" markdown="1">

| 章 | 一句话 | 什么时候翻它 |
|---|---|---|
| 1 全局地图 | 一次 RPC 是十几个接口的接力，每个接口只负责一件事 | 刚接手 Dubbo，想建立整体印象 |
| 2 SPI | 每个可变行为都是一个 `@SPI` 接口，配置只决定取哪个名字 | 想加自定义实现、或奇怪的配置没生效 |
| 3 服务导出 | `ServiceConfig` 把注解和配置揉成一个 URL，再让 `Protocol` 把它变成监听端口 | 服务没注册上、端口没开、导出时机不对 |
| 4 服务引用 | `ReferenceConfig` 攒出一串 Invoker，套上 Cluster 和代理，交给你一个假对象 | `No provider available`、`check`、直连 vs 注册中心 |
| 5 Spring 接缝 | 注解扫成 `ServiceBean`，`Deployer` 统一导出 | 自动配置没扫到、`@DubboReference` 字段为 null |
| 6 Filter 链 | Filter 逆序包成 Invoker 链，逐层递归 | 自定义 Filter 不生效、想做全链路透传 |
| 7 集群容错 | `invoke` 定四步模板，`doInvoke` 决定失败后做什么 | 重试导致重复提交、想换容错策略 |
| 8 负载均衡 | 选一台机器不难，难的是为"选错"付出多少运行时成本 | 流量不均、新机器预热被打满 |
| 9 目录与路由 | `Directory` 回答"有哪些地址"，`RouterChain` 回答"这次能用哪些" | 灰度/标签路由不生效、地址列表不更新 |
| 10 网络层 | transport 决定"字节怎么进出"，exchange 决定"回来时还给谁" | 超时、请求响应错配、线程模型选择 |
| 11 协议 | 16 字节头只回答四个问题，剩下全交给序列化层 | 半包解码异常、序列化兼容、要不要换 Triple |

</div>

## 三、症状 → 章节

| 现象 | 先看 |
|---|---|
| `No provider available` 启动就失败 | 第4、9章 |
| 服务启动了，注册中心里看不到 | 第3、5章 |
| `@DubboReference` 字段是 null | 第5章 |
| 自定义 Filter / LoadBalance 没被调用 | 第2、6章 |
| 重试后数据重复 | 第7章 |
| 流量总打到同一台 / 新机器被打满 | 第8章 |
| 灰度、标签路由不生效 | 第9章 |
| 调用超时但服务端其实很快 | 第10章 |
| 半包 / 解码异常 / 序列化不兼容 | 第11章 |
| 想加一种序列化或注册中心 | 第2章 |

## 四、18 个自测问题

**骨架**

- **Q1**　`Invoker`、`Exporter`、`Protocol`、`ProxyFactory` 各自只负责哪一件事？
- **Q2**　和 JDK SPI 比，Dubbo SPI 多做了哪三件事？
- **Q3**　`@Adaptive` 为什么要运行时生成字节码，而不是反射调用？
- **Q4**　扩展点加载的三级缓存分别缓存什么？

**配置层**

- **Q5**　一个 `@DubboService` 最终拼出的 URL 里，参数有哪几个来源？
- **Q6**　本地导出（injvm）和远程导出走的是同一条 `Protocol.export` 吗？
- **Q7**　`ReferenceConfig.get()` 返回的对象里持有的是远程连接，还是 Invoker？
- **Q8**　`check=false` 把失败从什么时候推迟到什么时候？
- **Q9**　`ServiceAnnotationPostProcessor` 和 `ReferenceAnnotationBeanPostProcessor` 分别在 Spring 的哪个扩展点工作？

**集群层**

- **Q10**　3.x 消费端为什么是两条 Filter 链？分界线在哪？
- **Q11**　Filter 链为什么必须逆序包装？
- **Q12**　`retries=2` 到底是几次调用？为什么 Failover 只对幂等操作安全？
- **Q13**　`AbstractClusterInvoker.invoke` 的四步模板是什么？
- **Q14**　随机负载均衡为什么用 `ThreadLocalRandom`？预热降权在哪里实现？
- **Q15**　`RouterChain` 的"至少保留一个"保护逻辑解决什么问题？

**通信层**

- **Q16**　`DefaultFuture` 靠什么把响应还给当初那个线程？
- **Q17**　`Dispatcher` 的五种线程模型分别在什么场景用，默认为什么是 all？
- **Q18**　Dubbo 协议头里的 flag 字节编码了哪几件事？

## 五、一个提醒

这本书讲的是 **3.3**，不是 2.7。两者差别不小，网上大部分源码分析文章写的是 2.x，直接套会错：

- `ProtocolFilterWrapper` 已不在拼装 Filter 链的那个方法里，节点变成 `CopyOfFilterChainNode` + `CallbackRegistrationInvoker`
- `ListenableFilter` 已废弃，新写法是 `implements Filter, Filter.Listener`
- 消费端拆成 `ClusterFilter`（负载均衡之前）和实例级 `Filter`（之后）两条链
- 多了 `ApplicationModel` / `ModuleModel` 这套分层，`ServiceConfig` 不再自己决定导出时机，交给 `DefaultModuleDeployer`
- 服务发现从接口级转向应用级，Triple 协议成为主推方向

每章的"**容易误解的地方**"比正文更值得反复看——那里写的都是"以为懂了、其实理解偏了"的位置。

> 判断有没有读懂一个机制，标准只有一个：**能不能说出"如果没有这一层，会出什么具体的错"**。说不出，就还是只记住了流程。