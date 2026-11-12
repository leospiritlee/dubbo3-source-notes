# 第4章 服务引用：一个 @DubboReference 是怎么变成代理的

**一句话**：ReferenceConfig 攒出一串 Invoker，套上 Cluster 和代理，交给你一个假对象。

**为什么需要它**：`@DubboReference` 标注的字段在 Spring 容器里找不到任何实现类，却能直接调方法；`check = true` 时服务起不来，改成 `check = false` 就能起来，但第一次调用又炸；`lazy = true` 到底省掉了什么；两个配置类引用同一个接口，只改了 `timeout`，为什么超时时间没变。这些问题都指向同一件事：你手里那个"接口实例"到底是什么。这一章拆开 `ReferenceConfig.get()` 到 `ProxyFactory.getProxy()` 的全过程，说明代理里握着的不是连接而是 Invoker、`check` 检查的是哪一刻的什么状态、直连和注册中心在哪一行分叉、`Directory`/`Cluster`/`Invoker` 三层各自负责什么，以及为什么同 `group/interface/version` 的两个引用会共用同一个代理。

理解这条链的收益很直接：调不通时能判断是"没有地址"（Directory 空）、"地址被路由筛掉"（Router）、"选中了但连不上"（Invoker），而不是笼统地说"注册中心有问题"。这三类故障在日志、异常类型、错误码上都不一样。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `ReferenceConfig<T>` | 引用入口。持有 `ref`（代理）、`invoker`（ClusterInvoker）、`initialized`、`destroyed` 四个 volatile 标志和一个 `ReentrantLock` |
| `ReferenceConfigBase<T>` | `shouldCheck()` / `shouldInit()` 的默认值推导；`preProcessRefresh()` 补默认 Consumer |
| `ConsumerModel` | 消费端运行时模型，`consumerModel.setProxyObject(ref)` 后可通过它反查代理 |
| `RegistryProtocol` | `refer()` 返回 `ClusterInvoker`，内部完成注册 consumer、订阅 provider、构建路由链 |
| `ServiceDiscoveryMigrationInvoker` | 3.3 默认返回的迁移型 Invoker，按 `MigrationStep` 决定用应用级还是接口级 Directory |
| `RegistryDirectory` / `ServiceDiscoveryRegistryDirectory` | 地址簿。`notify()` 收到推送后把 provider URL 逐个 `refer` 成 Invoker |
| `Cluster` / `ClusterInvoker` | 把 Directory 包成带容错、负载均衡、路由的 Invoker，`failover` 是默认值 |
| `ProxyFactory` | `@SPI("javassist")`，默认 Javassist 字节码代理，`jdk` 是备选 |
| `InvokerInvocationHandler` | 代理的 `InvocationHandler`，持有 `Invoker`，把方法调用翻译成 `RpcInvocation` |
| `ReferenceCache` / `SimpleReferenceCache` | 按 `group/interface:version` 缓存代理（老版本叫 `ReferenceConfigCache`） |
| `DubboInvoker` | 真正发包的 Invoker，持有 `ClientsProvider`（内部是 `ExchangeClient` 列表） |

**关键代码**：

```java
// ReferenceConfig.java:230
public T get(boolean check) {
    if (destroyed) {                       // 销毁后不可复活
        throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
    }
    if (ref == null) {                     // 双检第一层：ref 是 volatile
        if (getScopeModel().isLifeCycleManagedExternally()) {
            getScopeModel().getDeployer().prepare();
        } else {
            getScopeModel().getDeployer().start();   // 兼容旧 API 直接 new ReferenceConfig
        }
        init(check);                       // 真正的 refer 在这里
    }
    return ref;
}

// ReferenceConfig.java:332
protected void init(boolean check) {
    lock.lock();
    try {
        if (initialized && ref != null) {  // 锁内二次检查
            return;
        }
        try {
            if (!this.isRefreshed()) {
                this.refresh();
            }
            Map<String, String> referenceParameters = appendConfig();   // side=consumer 等消费端参数
            serviceDescriptor = repository.registerService(interfaceClass);   // nativestub 分支略
            consumerModel = new ConsumerModel(serviceMetadata.getServiceKey(), proxy,
                    serviceDescriptor, getScopeModel(), serviceMetadata, createAsyncMethodInfo(), interfaceClassLoader);
            repository.registerConsumer(consumerModel);                 // 先注册模型，再建 Invoker
            ref = createProxy(referenceParameters);                     // Invoker 链 + 代理都在这里面
            consumerModel.setProxyObject(ref);
            consumerModel.initMethodModels();
            if (check) {
                checkInvokerAvailable(0);      // 只有 check=true 才在 init 末尾校验
            }
        } catch (Throwable t) {
            logAndCleanup(t);                  // 失败要把 invoker/consumerModel/ref 全部清干净
            throw t;
        }
        initialized = true;
    } finally {
        lock.unlock();
    }
}
```

```java
// ReferenceConfig.java:490
private T createProxy(Map<String, String> referenceParameters) {
    urls.clear();
    if (StringUtils.isNotEmpty(url)) {
        parseUrl(referenceParameters);                  // 配了 url：直连或多注册中心
    } else {
        aggregateUrlFromRegistry(referenceParameters);  // 没配 url：从 registry 配置生成 registry://
    }
    createInvoker();                                    // 所有 Invoker 的组装
    return (T) proxyFactory.getProxy(invoker, ProtocolUtils.isGeneric(generic));   // 最后一步才是代理
}

// ReferenceConfig.java:669
private void createInvoker() {
    if (urls.size() == 1) {
        URL curUrl = urls.get(0);
        invoker = protocolSPI.refer(interfaceClass, curUrl);      // registry:// → RegistryProtocol.refer
        if (!UrlUtils.isRegistry(curUrl) && !curUrl.getParameter(UNLOAD_CLUSTER_RELATED, false)) {
            invoker = Cluster.getCluster(getScopeModel(), Cluster.DEFAULT)   // 直连要自己补 Cluster
                    .join(new StaticDirectory(curUrl, invokers), true);
        }
    } else { /* 多注册中心：逐个 refer，有 registry 则 ZoneAwareCluster 包一层 */ }
}

// JdkProxyFactory.java:35
public <T> T getProxy(Invoker<T> invoker, Class<?>[] interfaces) {
    return (T) Proxy.newProxyInstance(
            invoker.getInterface().getClassLoader(), interfaces, new InvokerInvocationHandler(invoker));
}

// InvokerInvocationHandler.java:51
public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getDeclaringClass() == Object.class) { return method.invoke(invoker, args); }
    ...
    RpcInvocation rpcInvocation = new RpcInvocation(serviceModel, method.getName(),
            invoker.getInterface().getName(), protocolServiceKey, method.getParameterTypes(), args);
    return InvocationUtil.invoke(invoker, rpcInvocation);   // 代理的活到此结束，剩下全是 Invoker 链
}
```

**拆解**：

### 4.1 一次 refer 的时序：模型先建，代理后建

Spring 场景下还有一层包装。`ReferenceBean` 是 `FactoryBean`，`getObject()` 返回的不是 Dubbo 代理，而是一个"懒代理"：`createLazyProxy()` 用 `Proxy.getProxy(interfaces).newInstance(new LazyTargetInvocationHandler(new DubboReferenceLazyInitTargetSource()))` 生成（Javassist 失败则回退 JDK 代理），handler 里 `target` 为 null，第一次真正调方法时才 `target = lazyTargetSource.getTarget()` → `getCallProxy()` → `referenceConfig.get()`。源码注释把动机写得很清楚：Spring 按类型查找 Bean 时可能提前初始化 FactoryBean，而那时 Dubbo 配置 Bean 还没就绪。所以"字段注入完成"不等于"refer 完成"，被注入的那个对象在被调用之前是个空壳。

`init()` 的顺序不能乱：先 `refresh()` 补默认值，再 `appendConfig()` 生成消费端参数 Map（`side=consumer`、`interface`、`methods`、`revision`、`register.ip` 都在这里，注意 `register.ip` 是消费端自己的地址，用来在注册中心登记 consumer），再 `registerService(interfaceClass)` 拿到 `ServiceDescriptor`，再 `new ConsumerModel(...)` 并 `registerConsumer()`，然后才 `createProxy()`。

先建模型的好处是：`createProxy()` 内部构造 URL 时可以直接 `url.setServiceModel(consumerModel)`，`InvokerInvocationHandler` 的构造器又能从 `invoker.getUrl().getServiceModel()` 里拿到 `ConsumerModel` 和 `protocolServiceKey`。模型是 URL 上的对象属性，URL 是字符串拼的，这条路是对象在链路上传递的唯一通道。

`ConsumerModel` 里还装了两样东西：`createAsyncMethodInfo()` 从每个 `MethodConfig.convertMethodConfig2AsyncInfo()` 得到的 `Map<String, AsyncMethodInfo>`，以及 `initMethodModels()` 建出来的方法级模型。它们不只是统计用的——`InvokerInvocationHandler` 在 `serviceModel instanceof ConsumerModel` 时会把 `CONSUMER_MODEL` 和 `METHOD_MODEL` 塞进 `RpcInvocation` 的 attachment，下游的 metrics、异步回调、方法级超时都靠这两个 key 找到对应模型。

异常路径同样重要：任何一步抛出，都走 `logAndCleanup(t)`——`invoker.destroy()`、`repository.unregisterConsumer(consumerModel)`、把 `initialized`/`invoker`/`ref`/`consumerModel` 全部置空。这就是为什么 `check=true` 启动失败之后，同一个 `ReferenceConfig` 对象在依赖重新可用时还能再 `get()` 一次，而不是永久卡在坏状态。

`createProxy()` 里还有一个 `meshModeHandleUrl()`，在 Service Mesh 模式下会把 `url` 改写成 `triple://{providedBy}.{namespace}.svc.cluster.local:80`，并强制 `unloadClusterRelated=false`。这是 3.3 里 Proxyless 场景的入口，普通项目不会走到。

### 4.2 check 检查的是"现在有没有可用 Invoker"，不是"能不能连上"

`shouldCheck()` 的推导链是：`isCheck()` → `consumer.isCheck()` → 默认 `true`。`checkInvokerAvailable(timeout)` 的逻辑很朴素：

```java
if (!shouldCheck()) { return; }
boolean available = invoker.isAvailable();
if (available) { return; }
long checkDeadline = System.currentTimeMillis() + timeout;
do {
    Thread.sleep(100);
    available = invoker.isAvailable();
} while (!available && checkDeadline > System.currentTimeMillis());
if (!available) { throw new IllegalStateException("Failed to check the status of the service ..."); }
```

注意 `init()` 里传的是 `0`，所以 `checkDeadline == startTime`，循环体仍然会执行一次 `sleep(100)`——`check=true` 至少容忍 100ms 的地址推送延迟。

抛出的异常类型是 `IllegalStateException`，不是 `RpcException`，消息里固定包含 `No provider available for the service`。`logAndCleanup()` 里专门用 `t.getMessage().contains("No provider available for the service")` 匹配它，打 `CLUSTER_NO_VALID_PROVIDER` 错误码。所以启动日志里那条 `No provider available.` 的 ERROR，是 `ReferenceConfig` 打的，不是 Cluster 打的。

这条异常消息本身值得完整读一遍，它把排查要用的信息都带上了：`interfaceName`、`group/version`（拼成 `group/interface:version`）、`invoker.getUrl()`（注册中心地址）、消费端本机 IP、Dubbo 版本。缺的只有"注册中心里到底有没有地址"，那要看 Directory 的订阅日志。所以看到它的第一反应不该是重启，而是拿 `invoker.getUrl()` 去查这个 service key 的 provider 列表——列表为空是 provider 没注册，列表非空就是本地订阅/路由出了问题。

Spring 场景下启动期的 check 其实不在 `get()` 里。`DefaultModuleDeployer` 的顺序是：`referServices()` 用 `referenceCache.get(rc, false)`（不 check，先把所有引用建起来），全部建完再 `checkReferences()` 用 `referenceCache.check(rc, timeout)` 统一等，`timeout` 取 `ModuleConfig.getCheckReferenceTimeout()`，缺省 30000ms。这个设计是为了让"所有引用并行建立、统一等待"，而不是每个引用各自阻塞。它还有一个副作用：`check=true` 的失败发生在 `checkReferences()` 阶段，此时所有 `ReferenceConfig` 的 `ref` 都已经建好，失败的那个被 `checkOrDestroy(timeout)` → `logAndCleanup()` 清掉，其它引用不受影响。这个 timeout 是"等地址推送"的总预算，不是单次连接超时。

`check=false` 时启动不报，但调用期照报：`AbstractClusterInvoker.checkInvokers()` 在 `invokers` 为空时抛 `RpcException(NO_INVOKER_AVAILABLE_AFTER_FILTER, "Failed to invoke the method ... No provider available for the service ... Please check if the providers have been started and registered.")`。所以 `check` 只决定"启动期是否失败"，不决定"调用期是否失败"。

实践上 `check` 是个部署期开关：provider 和 consumer 一起发布时通常设 `check=false`，避免消费端比提供端先起来导致整个应用起不来；代价是失败被推迟到第一次调用，异常类型从 `IllegalStateException` 变成 `RpcException`，日志位置也从启动阶段挪到了业务线程里。

`lazy` 是另一个维度。它不进 `ReferenceConfig` 的任何分支，只作为 URL 参数 `lazy=true` 传到 `DubboProtocol.initClient()`：

```java
return url.getParameter(LAZY_CONNECT_KEY, false)
        ? new LazyConnectExchangeClient(url, requestHandler)
        : Exchangers.connect(url, requestHandler);
```

`LAZY_CONNECT_KEY` 的值就是 `"lazy"`。所以 `lazy=true` 的含义是"refer 照做，但先不建 TCP 连接，首次 invoke 再连"。更坑的是 `LazyConnectExchangeClient.isConnected()` 在 `client == null` 时直接 `return true`（注释说这是为了防止 `ReconnectTask` 误重连），于是 `DubboInvoker.isAvailable()` 会返回 true——`check=true` + `lazy=true` 组合下，provider 明明没起来，启动检查也能通过。所以它只适合能容忍首调延迟的场景，不能拿来规避启动检查。

真正"启动时不 refer"的是 `init`：`shouldInit()` 为 false 时，`referServices()` 里的 `if (rc.shouldInit())` 直接跳过，代理要等到第一次被注入/调用时才 `get()`。

### 4.3 直连和注册中心在第 495 行分叉，`refer` 在末端收口

`createProxy()` 的第一个判断是 `StringUtils.isNotEmpty(url)`，这里的 `url` 是 `ReferenceConfigBase.url` 字段，也就是 `@DubboReference(url = "...")`。

走直连时 `parseUrl()` 用分号切分多个地址，每个地址 `URL.valueOf(u)`，path 为空则 `setPath(interfaceName)`，然后看 `UrlUtils.isRegistry(url)`：是注册中心地址就 `putAttribute(REFER_KEY, referenceParameters)`；不是就 `mergeUrl(url, referenceParameters)` 并 `putAttribute(PEER_KEY, true)`。`mergeUrl` 把消费端参数合并进 provider URL——这就是直连时 `timeout`、`retries` 仍然生效的原因。`url` 里可以混写，比如 `dubbo://a:20880;zookeeper://b:2181`，分号分隔的每一项各自判断是不是注册中心，所以"一半直连一半注册"在语法上是合法的。直连省掉注册中心一跳，适合灰度和跨环境调试；代价是没有地址列表，provider 挂了只能靠 `failover` 对同一台重试。

走注册中心时 `aggregateUrlFromRegistry()`：`checkRegistry()` 校验注册中心配置有效，`ConfigValidationUtils.loadRegistries(this, false)` 生成 `registry://` URL（`provider=false` 时只取 `subscribe=true` 的），挂上 monitor URL，`putAttribute(REFER_KEY, referenceParameters)`。如果 `urls` 还是空且 `shouldJvmRefer()` 为 true，补一个 `injvm://127.0.0.1:0/{interface}`；仍然为空就抛 `No such any registry to reference ... please config <dubbo:registry address="..."/>`。

这里有个设计要点：消费端参数不是拼进 URL 字符串的，而是作为 `REFER_KEY` 属性的一个 `Map` 挂在 registry URL 上。原因是这些参数是"我要怎么调"，不是"provider 是什么"，混进 URL 字符串会被注册中心当成 provider 元数据。`RegistryProtocol.refer()` 里那句 `Map<String, String> qs = (Map<String, String>) url.getAttribute(REFER_KEY)` 就是把它取回来。

`shouldJvmRefer()` 的四步判断写在方法注释里：`injvm` 显式配置优先；配了 `url` 就认为是远程；否则看 `scope`；都没配时用 `InjvmProtocol.getInjvmProtocol(scopeModel).isInjvmRefer(tmpUrl)` 探测本机是否真的导出过这个服务。`isInjvmRefer()` 内部先看 `scope=local` / `injvm=true`，再看 `scope=remote`，再看 `generic`（泛化调用不算本地），最后 `getExporter(exporterMap, url) != null` 才返回 true；`cluster=broadcast` 时强制返回 false——广播调用要打到所有机器，不能被本地短路。

`createInvoker()` 的分支值得逐条读：

- `urls.size() == 1` 且是 registry：`protocolSPI.refer()` 直接返回 `ClusterInvoker`，因为 `RegistryProtocol` 内部已经 `cluster.join(directory)` 过了，外面不能再包一层。
- `urls.size() == 1` 且不是 registry（直连）：`protocolSPI.refer()` 返回的是裸的 `DubboInvoker`，必须自己 `Cluster.getCluster(..., Cluster.DEFAULT).join(new StaticDirectory(curUrl, invokers), true)` 包一层，否则没有容错和负载均衡。
- 多个 URL：每个都 refer，如果有 registry URL 就用 `ZoneAwareCluster`（多注册中心场景默认按 zone 优先），否则用默认 `failover`。

`Cluster.DEFAULT` 就是 `"failover"`，`Cluster.getCluster()` 内部走 `ExtensionLoader.getExtension(name, wrap)`，`wrap=true` 时会把 `AbstractCluster` 那层 wrapper 也套上。

多注册中心这条分支还有两个细节。`Cluster.getCluster(..., cluster, false)` 传了 `wrap=false`——因为外层 `StaticDirectory` 里装的是"每个注册中心一个 ClusterInvoker"，再套通用 wrapper 没有意义。`ZoneAwareClusterInvoker.doInvoke()` 的优先级是：`preferred=true` 的 registry → 请求 attachment 里的 `registry.zone`（或 `ZoneDetector` 探测出的 zone）匹配的 registry → 按 registry 权重做一次负载均衡 → 第一个可用的 → 都没有则抛 `No provider available in ...`。

无论走哪条路，最后都落到 `protocolSPI.refer()`，也就是 `RegistryProtocol.refer()`（或它的接口级兼容子类）。

`refer()` 先把 registry URL 规范化（`getRegistryUrl(url)` 会把 `registry=` 参数记下、协议改成 `service-discovery-registry`），`getRegistry()` 拿到 `Registry` 实例。`RegistryService.class.equals(type)` 是特判：引用注册中心本身时，直接把 `Registry` 对象包成 Invoker 返回，不走 Directory。

group 多值或 `*` 时用 `MergeableCluster`，否则 `Cluster.getCluster(url.getScopeModel(), qs.get(CLUSTER_KEY))`。然后 `doRefer()`：用 `REFER_KEY` 里的参数构造 `consumerUrl`（protocol 取 `consumer`、host 取 `register.ip`、port 固定 0），`putAttribute(CONSUMER_URL_KEY, consumerUrl)`，再 `new ServiceDiscoveryMigrationInvoker<>(...)`，最后 `interceptInvoker()` 交给 `RegistryProtocolListener` 扩展点。

关键点在 3.3：`refer()` 返回的不是 `RegistryDirectory`，而是一个还没建 Directory 的 `MigrationInvoker`。真正的 Directory 由 `MigrationRuleListener.onRefer()` → `MigrationRuleHandler.doMigrate()` 触发；因为返回的是 `ServiceDiscoveryMigrationInvoker`，`doMigrate` 一开始就 `refreshInvoker(MigrationStep.FORCE_APPLICATION, 1.0f, rule)` 并 return，于是走 `migrateToForceApplicationInvoker` → `refreshServiceDiscoveryInvoker` → `registryProtocol.getServiceDiscoveryInvoker()` → `new ServiceDiscoveryRegistryDirectory<>(type, url)`。

`RegistryDirectory`（接口级）只由 `RegistryProtocol.getInvoker(cluster, registry, type, url)` 创建，而这个方法上面挂着源码注释 `// FIXME, this method is currently not used, create the right registry before enable.`。也就是说 3.3 默认链路里跑的是应用级 Directory，`RegistryDirectory` 是接口级兼容和迁移路径的产物。读源码时不要把两者混为一谈。

两个 Directory 共用 `doCreateInvoker()`：

```java
directory.setRegistry(registry);
directory.setProtocol(protocol);
Map<String, String> parameters = new HashMap<>(directory.getConsumerUrl().getParameters());
URL urlToRegistry = new ServiceConfigURL(parameters.get(PROTOCOL_KEY) == null ? CONSUMER : parameters.get(PROTOCOL_KEY),
        parameters.remove(REGISTER_IP_KEY), 0, getPath(parameters, type), parameters);
if (directory.isShouldRegister()) {
    directory.setRegisteredConsumerUrl(urlToRegistry);
    registry.register(directory.getRegisteredConsumerUrl());   // 消费端也往注册中心写一条 consumer://
}
directory.buildRouterChain(urlToRegistry);                     // 构建路由链（标签路由、条件路由）
directory.subscribe(toSubscribeUrl(urlToRegistry));            // category=providers,configurators,routers
return (ClusterInvoker<T>) cluster.join(directory, true);
```

`RegistryDirectory.notify()` 收到推送后按 category 分组，`providers` 走 `refreshInvoker()` → `toInvokers()`，逐个 `protocol.refer()` 生成 `DubboInvoker`（带 `Map<URL, Invoker>` 缓存，URL 没变就复用），最后 `setInvokers()`。地址变化靠注册中心推送，不靠轮询。

`toSubscribeUrl()` 只是加一个 `category=ALL_CATEGORIES`，一次订阅同时拿到 providers、configurators、routers 三类数据。`directory.buildRouterChain(urlToRegistry)` 建的是路由链（标签路由、条件路由、脚本路由），它在 `list()` 时对地址做过滤，所以 `No provider available after route` 和 `No provider available after connectivity filter` 是两个不同的失败点：前者是路由把地址筛没了，后者是筛完发现连不上。`registry.register(consumerUrl)` 这一步也容易被忽略——消费端会往注册中心写一条 `consumer://` 记录，治理台靠它显示"谁在调我"，配置覆盖也靠它匹配。

应用级订阅比接口级多一步"接口到应用名"的映射，这是 3.3 里 `No provider available` 的另一个高发原因。`ServiceDiscoveryRegistry.doSubscribe()` 先 `serviceDiscovery.subscribe(url, listener)`，再 `ServiceNameMapping.getMappingByUrl(url)` 拿这个接口属于哪些应用；拿不到就 `serviceNameMapping.getAndListen(...)` 注册一个 `DefaultMappingListener` 等推送，并直接 `return`，日志是 `No interface-apps mapping found in local cache, stop subscribing, will automatically wait for mapping listener callback`。provider 侧如果元数据中心不可用、`ServiceNameMapping.map()` 失败，消费端就会一直等这个回调——端口通、provider 也活着，但订阅列表始终是空的。排查这类问题时，要先确认元数据中心的接口-应用映射有没有写成功，而不是先怀疑网络。

### 4.4 代理里没有连接，只有一条 Invoker 链

`JdkProxyFactory.getProxy()` 就是 `Proxy.newProxyInstance(classLoader, interfaces, new InvokerInvocationHandler(invoker))`。默认实现是 `javassist`（`ProxyFactory` 上 `@SPI(value = "javassist")`），生成字节码而不是反射调用，但结构完全一样：代理 → `InvocationHandler` → `Invoker`。

`InvokerInvocationHandler` 只持有三样东西：`Invoker<?> invoker`、`ServiceModel serviceModel`、`String protocolServiceKey`。`invoke()` 里先拦 `Object` 的方法（`toString`/`hashCode`/`equals` 转发给 invoker，`$destroy` 调 `invoker.destroy()`），其余全部打包成 `RpcInvocation` 后 `InvocationUtil.invoke(invoker, rpcInvocation)`。

`InvocationUtil` 这一层做三件不显眼但重要的事：`rpcInvocation.setTargetServiceUniqueName(url.getServiceKey())` 把目标服务固定下来（后面重试、路由都读它）；`RpcServiceContext.getServiceContext().setConsumerUrl(url)` 把消费端 URL 放进上下文（Filter 里 `RpcContext.getServiceContext().getConsumerUrl()` 就来自这里）；最后 `invoker.invoke(rpcInvocation).recreate()`——`recreate()` 把 `AppResponse` 里的 `CompletableFuture` 展开成真正的返回值，异步转同步的最后一环在这里。

所以调用栈是：代理 → `InvokerInvocationHandler` → `ClusterInvoker`（`MigrationInvoker`/`FailoverClusterInvoker`）→ `Directory`（地址列表 + 路由）→ `DubboInvoker`（`ClientsProvider` → `ExchangeClient`）→ TCP。代理里没有任何连接、地址、序列化器，这些都在链的末端。这也解释了为什么改 `timeout` 要重建代理：`timeout` 在 URL 里，URL 在 `DubboInvoker` 里，而代理是链的入口，链是按 URL 建的。

消费端的过滤器分两层织入。`ClusterFilter` 在 Cluster 层生效，例如 `FutureFilter`（`@Activate(group = CONSUMER)`）包在 `ClusterInvoker` 的调用链上；普通 `Filter` 在协议层生效，`ProtocolFilterWrapper.refer()` 对非 registry URL 执行 `builder.buildInvokerChain(protocol.refer(type, url), REFERENCE_FILTER_KEY, CONSUMER)`，包在 `DubboInvoker` 外面。所以 `InvokerInvocationHandler` 一次调用要依次穿过 ClusterFilter、ClusterInvoker、Directory、协议层 Filter，最后才到 `DubboInvoker` 发包。

`AbstractProxyFactory.getProxy(invoker, generic)` 还会往 interfaces 里加两个内部接口：`EchoService` 和 `Destroyable`，所以任何 Dubbo 代理都能 `$echo()` 和 `$destroy()`。

泛化调用走的是同一个 handler。`generic=true` 时 `AbstractProxyFactory` 会把真实接口类和 `GenericService` 一起加进 interfaces，代理上于是既有业务方法也有 `$invoke(String, String[], Object[])`。`$invoke` 被 `InvokerInvocationHandler` 打包成 method 名为 `$invoke` 的 `RpcInvocation`（三个参数分别是方法名、参数类型数组、参数值数组），到 provider 侧由 `GenericFilter` 拦下来：`inv.getMethodName().equals($INVOKE)` 成立时，用 `findMethodByMethodSignature(interface, name, types, serviceModel)` 反查真实 `Method`，再 `PojoUtils.realize(args, params, genericParameterTypes)` 把 Map 还原成 POJO。所以泛化调用不是另一条链路，只是同一条链路上多了两次"方法名到 Method"的解析。

代理本身也不便宜，所以 Dubbo 在它外面又加了一层缓存。3.3 里叫 `ReferenceCache` / `SimpleReferenceCache`，2.x 叫 `ReferenceConfigCache`。`DEFAULT_KEY_GENERATOR` 是：

```java
return BaseServiceMetadata.buildServiceKey(iName, referenceConfig.getGroup(), referenceConfig.getVersion());
```

也就是 `group/interface:version`。`get(rc, check)` 在 `singleton` 为 true 时先 `get(key, type)`，命中就直接返回已有代理——**新 `ReferenceConfig` 上的 `timeout`、`retries`、`check` 全部不生效**，因为它们属于那个被丢弃的配置对象。没命中才 `rc.get(check)`，并把 `rc` 记进 `referenceKeyMap` 和 `referenceTypeMap`（一个 key 可以对应多个 `ReferenceConfig`，`destroy(key, type)` 会把它们全销毁）。注意 key 里没有 `timeout`、没有 `cluster`、没有 `loadbalance`：同一个接口的两个引用只要 `group` 和 `version` 相同，就一定是同一个代理，后者的调优参数会被静默忽略。要真正区分，只能改 `group`/`version`，或者不用缓存、直接调 `ReferenceConfig.get()`。

`SimpleReferenceCache` 的 `referenceTypeMap` 还支撑 `getAll(Class)` 和 `destroy(Class)`，Spring 的 `@DubboReference` 注入、`DubboBootstrap` 的引用检查都走这个入口。Spring 侧还有一层 `ReferenceBeanManager.referenceConfigMap`，按属性生成的 `referenceKey` 复用 `ReferenceConfig`，避免同参数重复建配置对象。

缓存也是内存泄漏的高发点。`SimpleReferenceCache` 是静态的 `CACHE_HOLDER`，按 name 存，默认 `_DEFAULT_`；`referenceKeyMap` 里存的是 `ReferenceConfigBase` 强引用，而每个 `ReferenceConfig` 又持有 `invoker`、`consumerModel`、代理。所以动态创建引用（比如按租户建 `ReferenceConfig`）而不调 `destroy(key, type)`，`invoker` 永远不会释放，注册中心的订阅也不会退订。`get(rc, check)` 里那条 `CONFIG_API_WRONG_USE` 警告就是为 `singleton=false` + 缓存同时使用准备的。

**容易误解的地方**：

1. **"代理里握着一条长连接"**。代理只握 `Invoker`。连接在 `DubboInvoker` 的 `ClientsProvider` 里（内部是 `ExchangeClient` 列表），而且默认是共享的（`connections=0` 时走 `SharedClientsProvider`，按 `host:port` 复用）。
2. **"`check=false` 就不会报 No provider available"**。它只关掉启动期检查。调用期 `AbstractClusterInvoker.checkInvokers()` 照样抛 `RpcException`，只是错误码从 `CLUSTER_NO_VALID_PROVIDER` 变成 `NO_INVOKER_AVAILABLE_AFTER_FILTER`。
3. **"`lazy=true` 表示启动不 refer"**。它只是让 `DubboProtocol` 用 `LazyConnectExchangeClient` 延迟建连，而且这个 client 未连接时 `isConnected()` 返回 true，会让 `check` 失效。启动不 refer 是 `init=false`。
4. **"refer 之后就是 `RegistryDirectory` 在工作"**。3.3 默认返回 `ServiceDiscoveryMigrationInvoker`，`doMigrate` 直接 `FORCE_APPLICATION`，跑的是 `ServiceDiscoveryRegistryDirectory`；`RegistryDirectory` 只在接口级/迁移路径里由 `RegistryProtocol.getInvoker()` 创建，源码自己标了 FIXME。

**记住这一句**：引用 = 一堆 provider Invoker 被 Directory 收拢、被 Cluster 包住、最后被 ProxyFactory 糊成一个你以为是本地对象的代理。
