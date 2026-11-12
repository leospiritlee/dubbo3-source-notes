# 第3章 服务导出：一个 @DubboService 是怎么变成端口的

**一句话**：ServiceConfig 把注解和配置揉成一个 URL，再让 Protocol 把它变成监听端口。

**为什么需要它**：你在类上打了 `@DubboService`，Spring 里它只是一个普通 Bean，没有任何网络能力。可服务起来之后，20880 端口上确实有人在 listen，注册中心里也确实多了一条 `dubbo://10.0.0.7:20880/com.foo.DemoService?...`。这中间的转换过程不透明，于是故障也就无从下手：日志明明打印了 `Export dubbo service ... to url`，消费端还是 `No provider available`；端口被别的进程占了，异常栈却指向一个跟端口无关的配置类；同一个 JVM 内自测通过，跨进程调用就超时——那说明只走了 injvm，远程根本没导出。这一章要回答的就是：配置对象在什么时候被补齐、URL 里那几十个参数各自从哪来、本地导出和远程导出差在哪、端口究竟由谁打开、以及为什么重复 export 不会把端口开两次。

排查思路上，可以先把"导出"拆成三个可独立验证的事实：`exported` 是否为 true（配置对象层面）、`Exporter` 是否存在于 `exporters`（协议层面）、`host:port` 是否在 listen（网络层面）。三者依次成立才算真正导出成功，而日志里那行 `Export dubbo service` 只证明走到了 `exportRemote()`，不代表后面两步成功。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `ServiceConfig<T>` | 导出入口。持有 `exported`/`unexported` 两个 volatile 标志、`exporters` 按 `RegisterTypeEnum` 分组的 Exporter 列表 |
| `ServiceConfigBase<T>` | 持有 `ref`（实现类实例）、`interfaceClass`、`path`、`provider`；`preProcessRefresh()` 在这里补默认 Provider |
| `AbstractServiceConfig` | 持有 `export`、`delay`、`protocols`、`protocolIds`、`version`、`group`、`token`、`weight` |
| `AbstractInterfaceConfig` | 持有 `registries`、`application`、`module`、`monitor`、`local`/`stub`；`completeCompoundConfigs()` 做配置继承 |
| `AbstractConfig` | `refresh()` 模板方法、`appendParameters()` 反射把 getter 写进 Map |
| `ProviderModel` | 一个已导出服务的运行时模型，被挂到 URL 的 `serviceModel` 上 |
| `ProxyFactory` | `getInvoker(ref, interfaceClass, url)` 把实现类包成 provider 侧 Invoker |
| `Protocol`（adaptive） | `export(Invoker)` 按 URL 协议分派到 `RegistryProtocol` / `DubboProtocol` / `InjvmProtocol` |
| `Exporter` | 导出的句柄，`unexport()` 时用来回收端口与注册信息 |
| `ServiceConfigURL` | 真正被拼出来的 URL，带 `scopeModel`/`serviceModel` 两个对象属性 |

**关键代码**：

```java
// ServiceConfig.java:310
public void export(RegisterTypeEnum registerType) {
    if (this.exported) {                 // 快路径：绝大多数重复调用在这一行就返回了，无锁
        return;
    }
    if (getScopeModel().isLifeCycleManagedExternally()) {
        getScopeModel().getDeployer().prepare();   // 生命周期由外部（Spring/Bootstrap）管
    } else {
        getScopeModel().getDeployer().start();     // 兼容旧 API：直接 new ServiceConfig 也能用
    }
    synchronized (this) {
        if (this.exported) {             // 双检：并发 export 只放一个线程进去
            return;
        }
        if (!this.isRefreshed()) {
            this.refresh();              // 补默认值 + 校验，不是"读注解"（注解在构造器里已处理）
        }
        if (this.shouldExport()) {
            this.init();                 // 建 ServiceMetadata、加载 ServiceListener
            if (shouldDelay()) {
                doDelayExport();         // delay>0：丢给调度线程，export() 立刻返回
            } else if (Integer.valueOf(-1).equals(getDelay()) && Boolean.parseBoolean(
                    ConfigurationUtils.getProperty(getScopeModel(),
                            CommonConstants.DubboProperty.DUBBO_MANUAL_REGISTER_KEY, "false"))) {
                doExport(RegisterTypeEnum.MANUAL_REGISTER);   // delay=-1 且开了手动注册开关
            } else {
                doExport(registerType);
            }
        }
    }
}

// ServiceConfig.java:544
protected synchronized void doExport(RegisterTypeEnum registerType) {
    if (unexported) {                    // 单向门：unexport 过的配置不能再导出
        throw new IllegalStateException("The service " + interfaceClass.getName() + " has already unexported!");
    }
    if (exported) {
        return;                          // 第三道幂等检查
    }
    if (StringUtils.isEmpty(path)) {
        path = interfaceName;            // URL 的 path 默认就是接口全名
    }
    doExportUrls(registerType);
    exported();                          // 全部 URL 都导出成功之后才置位，失败可重试
}
```

```java
// ServiceConfig.java:955
private void doExportUrl(URL url, boolean withMetaData, RegisterTypeEnum registerType) {
    if (!url.getParameter(REGISTER_KEY, true)) {
        registerType = RegisterTypeEnum.MANUAL_REGISTER;   // register="false" 的地址不自动注册
    }
    if (registerType == RegisterTypeEnum.NEVER_REGISTER
            || registerType == RegisterTypeEnum.MANUAL_REGISTER
            || registerType == RegisterTypeEnum.AUTO_REGISTER_BY_DEPLOYER) {
        url = url.addParameter(REGISTER_KEY, false);       // 把"注册意图"写进 URL，交给 RegistryProtocol 判断
    }
    Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);  // 实现类 → Invoker
    if (withMetaData) {
        invoker = new DelegateProviderMetaDataInvoker(invoker, this);   // 把 ServiceConfig 挂到 Invoker 上
    }
    Exporter<?> exporter = protocolSPI.export(invoker);    // adaptive：按 url.getProtocol() 选实现
    exporters.computeIfAbsent(registerType, k -> new CopyOnWriteArrayList<>()).add(exporter);
}

// ServiceConfig.java:978
private void exportLocal(URL url) {
    URL local = URLBuilder.from(url)
            .setProtocol(LOCAL_PROTOCOL)      // injvm
            .setHost(LOCALHOST_VALUE)         // 127.0.0.1
            .setPort(0)                       // 端口 0：本地导出根本没有端口
            .build();
    local = local.addParameter(EXPORTER_LISTENER_KEY, LOCAL_PROTOCOL);
    doExportUrl(local, false, RegisterTypeEnum.AUTO_REGISTER);   // withMetaData=false：本地不需要元数据
}
```

**拆解**：

### 3.1 配置对象是四层继承 + 一个 refresh 模板方法

继承链是 `ServiceConfig` → `ServiceConfigBase` → `AbstractServiceConfig` → `AbstractInterfaceConfig` → `AbstractConfig`。字段分散在这几层，所以"这个属性归谁管"不能靠猜：`export`/`delay`/`protocols` 在 `AbstractServiceConfig`，`ref`/`path`/`provider` 在 `ServiceConfigBase`，`registries`/`application` 在 `AbstractInterfaceConfig`，`parameters`/`id`/`name` 在 `AbstractConfig`。

`AbstractConfig.refresh()` 是模板方法，四个钩子按顺序跑：

```java
public void refresh() {
    if (needRefresh) {
        preProcessRefresh();                              // 1. 子类补前置依赖
        refreshWithPrefixes(getPrefixes(), getConfigMode());  // 2. 从 Environment 拉配置覆盖字段
        postProcessRefresh();                             // 3. 子类做校验与派生
    }
    refreshed.set(true);                                  // 4. 无论 needRefresh 与否都置位
}
```

对 `ServiceConfig` 来说，`preProcessRefresh()` 里做三件事：`convertProviderIdToProvider()`（把 `providerIds` 解析成 `ProviderConfig`）、若 `provider == null` 就从 `getModuleConfigManager().getDefaultProvider()` 取，取不到直接抛 `Default provider is not initialized`、最后用 `dubbo.services.{id}` / `dubbo.services.{name}` / `dubbo.service` 三个前缀做一次 `OVERRIDE_IF_ABSENT` 的刷新。第三步用 `ConfigMode.OVERRIDE_IF_ABSENT`，意思是"字段已有值就不覆盖"——这就是配置优先级里 XML/注解 > 配置文件的实现位置。

`checkProtocol()` 也值得看一眼：如果 service 自己既没配 `protocols` 也没配 `protocolIds`（`notHasSelfProtocolProperty()`），就先继承 `provider` 的协议；然后 `convertProtocolIdsToProtocols()` 在 `protocolIds` 为空且 `protocols` 仍为空时，从 `getConfigManager().getDefaultProtocols()` 取全局默认协议，取不到抛 `The default protocol has not been initialized`。所以"一个服务导出到哪些协议"最终由三层决定：service 自己 → provider → 全局 `dubbo.protocol`。`protocols` 是 `List<ProtocolConfig>`，一个服务可以同时导出到 dubbo 和 triple，这就是 `doExportUrls()` 里那个 `for (ProtocolConfig protocolConfig : protocols)` 循环的由来。

`postProcessRefresh()` 被 `ServiceConfig` 覆写成 `checkAndUpdateSubConfigs()`，这才是重活：`completeCompoundConfigs()` 把 `provider`/`module`/`application` 上的 `registries`、`monitor`、`configCenter` 逐级下灌；`ConfigInitializer` 扩展点补空值；非纯 injvm 时 `checkRegistry()`；`Class.forName(interfaceName)`；`checkRef()` 校验 `ref` 非空且确实实现了接口；`ConfigValidationUtils.validateServiceConfig(this)`；最后 `postProcessConfig()` 走 `ConfigPostProcessor` 扩展点。

`refresh()` 的幂等靠 `needRefresh` 和 `refreshed` 两个标志：已经 refresh 过的对象再次进入不会重复反射赋值，但 `refreshed.set(true)` 在 `if` 外面，所以"没做事"也会被标记成"已刷新"。这就是 `ServiceConfig.export()` 里 `if (!this.isRefreshed()) { this.refresh(); }` 能安全重复执行的原因。

`refreshWithPrefixes()` 本身只做一件事：在 `Environment.getConfigurationMaps()` 里按前缀顺序找第一个"有子属性"的前缀，把它下面的所有 key 抽成 `InmemoryConfiguration`，再 `assignProperties()` 反射写回字段。`getPrefixes()` 的顺序就是优先级：`ServiceConfigBase` 把它覆写成只有 `dubbo.service.{interfaceName}`，`AbstractConfig` 的版本是 `dubbo.services.{id}` → `dubbo.services.{name}` → `dubbo.service`。前者用于 `preProcessRefresh()` 里的兜底刷新，后者用于正常刷新——所以 `dubbo.service.com.foo.DemoService.timeout` 会盖掉 `dubbo.service.timeout`。

注解这条路值得单独说清楚。`@DubboService` 并不会直接 new 一个 `ServiceConfig`，Spring 侧生成的是 `ServiceBean<T> extends ServiceConfig<T>`（ServiceBean.java:42）。构造器 `ServiceBean(ApplicationContext, ModuleModel, Service service)` 调 `super(moduleModel, service)`，而 `ServiceConfigBase(ModuleModel, Service)` 的第一件事就是 `appendAnnotation(Service.class, service)`——注解上的十几个属性在这一刻被反射写进字段，远早于 `refresh()`。`ServiceBean.afterPropertiesSet()` 也只做两件事：`setPath(getInterface())` 兜底，以及 `moduleModel.getConfigManager().addService(this)` + `getDeployer().setPending()`。**它不导出**。真正的 `export()` 由 `DefaultModuleDeployer` 启动时的 `exportServices()` 统一触发，`exportServiceInternal()` 里先 `refresh()` 再 `sc.export(RegisterTypeEnum.AUTO_REGISTER_BY_DEPLOYER)`。所以"Bean 初始化完成"和"端口已打开"是两个时刻，中间隔着 Deployer 的一次状态流转。

有一个容易忽略的细节：`ServiceConfig.getDelay()`、`getExport()`、`getGroup()`、`getVersion()` 都做了 provider 兜底，例如 `getDelay()` 是 `(delay == null && provider != null) ? provider.getDelay() : delay`。所以你在 `ServiceConfig` 上读到的值，未必是它自己字段的值。

### 3.2 URL 是六层 Map 叠加出来的，不是"用户配的"

`doExportUrlsFor1Protocol()` 第一行 `Map<String, String> map = buildAttributes(protocolConfig)`，这个 Map 的写入顺序就是优先级顺序，后写的覆盖先写的：

```java
map.put(SIDE_KEY, PROVIDER_SIDE);                  // side=provider
ServiceConfig.appendRuntimeParameters(map);        // dubbo=3.3.x, release=..., timestamp=..., pid=...
AbstractConfig.appendParameters(map, getApplication());   // application=..., qos..., 序列化偏好...
AbstractConfig.appendParameters(map, getModule());
AbstractConfig.appendParameters(map, provider);    // 注意：没有 "default." 前缀，2.x 的 default.xxx 已被去掉
AbstractConfig.appendParameters(map, protocolConfig);
AbstractConfig.appendParameters(map, this);        // service 级，优先级最高
```

之后才追加方法级参数：`appendParametersWithMethod()` 用方法名做前缀写 `sayHello.timeout` 这类 key，`arguments` 再按 `方法名.索引` 写。最后是 `generic`/`revision`/`methods`/`token`：`revision` 由 `Version.getVersion(interfaceClass, version)` 算接口的版本指纹；`methods` 用 `StringUtils.join(new TreeSet<>(Arrays.asList(methods)), ",")` 排序后拼接——排序不是洁癖，URL 的 `equals` 决定注册中心 diff 和 override 比较，方法顺序不稳定会导致"配置没变却一直重新导出"。

`AbstractConfig.appendParameters0()` 的规则值得记住：只看 getter，`@Parameter(excluded = true)` 跳过，值 `null` 或空串跳过，`@Parameter(append = true)` 时与已有值逗号合并并去重。这就是为什么 `protocols` 这种集合字段不会出现在 URL 里，而 `methods` 这种字符串会出现。

host 和 port 的解析在 `findConfiguredHosts()` / `findConfiguredPort()`，优先级是"协议前缀的系统属性 → 协议配置 → provider 配置 → 默认值"。`DUBBO_IP_TO_BIND` 和 `DUBBO_IP_TO_REGISTRY` 是两个独立概念：前者是网卡上 bind 的地址（可能是 `0.0.0.0`），后者是写进注册中心的地址，默认等于前者。端口同理，`findConfiguredPort` 会调 `extensionLoader.getExtension(name).getDefaultPort()` 拿协议默认端口（dubbo 是 20880），小于等于 0 时先查静态 `RANDOM_PORT_MAP`，再 `getAvailablePort(defaultPort)` 并缓存——所以同一进程内多个没配端口的 dubbo 服务会共用同一个随机端口。

`buildAttributes()` 末尾还有一段值得注意：`if (ConfigUtils.isEmpty(token) && provider != null) { token = provider.getToken(); }`，非空时 `ConfigUtils.isDefault(token)` 为 true 就 `map.put(TOKEN_KEY, UUID.randomUUID().toString())`。这是服务端鉴权：provider URL 带 `token=xxx`，注册中心把它暴露给消费端，消费端调用时带上同一个值，`TokenFilter` 比对不一致就拒绝。所以 `token="true"` 的语义不是"开启鉴权"，而是"生成一个随机 token"。

`buildUrl()` 里还有三段容易被跳过的逻辑。第一段是 IPv6：`findConfiguredHosts()` 返回的 host 如果是标准 IPv6 字面量，会被包成 `[::1]` 形式；否则若本机存在 v6 地址，会额外往参数里塞 `IPV6_KEY`，供注册中心侧做双栈展示。第二段是 contextPath：`getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path)`，所以 URL 的 path 可能是 `api/com.foo.DemoService` 而不是纯接口名，`ServiceConfigBase.getContextPath()` 先看协议配置、再看 provider 配置。第三段是 `ConfiguratorFactory`：如果当前协议有对应的工厂扩展，`url` 会被 `getConfigurator(url).configure(url)` 再加工一次，这是给 rest 之类需要改写 URL 的协议留的口子。加工完之后才 `setScopeModel()` / `setServiceModel()`，顺序不能反——`Configurator` 拿到的 URL 还没有模型。

最后一步 `new ServiceConfigURL(name, null, null, host, port, getContextPath(...).map(p -> p + "/" + path).orElse(path), params)`，`Configurator` 改完之后再 `setScopeModel()` / `setServiceModel(providerModel)`。这两个 setter 不是装饰：URL 是字符串拼出来的，`ServiceModel` 这种对象挂不进去，只能作为 URL 的属性带着走，后面 `DubboProtocol`、监控、异常上报全靠 `url.getServiceModel()` 反查 `ProviderModel`。

### 3.3 本地导出和远程导出是两条独立的 if，不是二选一

`exportUrl()` 用 `scope` 参数控制，默认 `scope == null`，两个分支都会进：

```java
String scope = url.getParameter(SCOPE_KEY);
if (!SCOPE_NONE.equalsIgnoreCase(scope)) {
    if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {
        exportLocal(url);                 // 默认走这里
    }
    if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {
        url = exportRemote(url, registryURLs, registerType);   // 默认也走这里
    }
}
this.urls.add(url);
```

`exportLocal()` 把协议换成 `injvm`、host 换成 `127.0.0.1`、port 换成 0，再调一次 `doExportUrl(local, false, AUTO_REGISTER)`。它最终落到 `InjvmProtocol.export()`，整个方法体只有一行：`new InjvmExporter<>(invoker, invoker.getUrl().getServiceKey(), exporterMap)`。也就是说 injvm 导出不开端口、不建连接，只是把 Invoker 塞进一个 `Map<String, Exporter<?>>`，key 是 `group/interface:version`。`InjvmProtocol.isInjvmRefer()` 反过来查这个 Map 判断"本机有没有这个服务"——这就是消费端默认优先走本地调用的依据。

`exportLocal()` 是无条件调用的（除非 `scope=remote`），哪怕你只配了 `injvm` 一个协议。`isOnlyInJvm()` 只影响一件事：`checkAndUpdateSubConfigs()` 里 `if (!isOnlyInJvm()) { checkRegistry(); }`——纯本地导出不校验注册中心。这也解释了一个经典现象：单测里 `@DubboReference` 调得通，打包部署后超时。单测时 provider 和 consumer 在同一个 JVM，`shouldJvmRefer()` 查到了 `exporterMap` 里的 exporter，走了 injvm；部署后不在同一进程，才真正走网络，序列化、超时、防火墙的问题一次性暴露。

`exportRemote()` 分两种情况。有注册中心时，对每个 registry URL 调 `doExportUrl(registryURL.putAttribute(EXPORT_KEY, url), true, registerType)`：注意传进去的是 `registry://...` 而不是 `dubbo://...`，真正的 provider URL 被塞进 `EXPORT_KEY` 属性里。这就是 `RegistryProtocol.export()` 里 `getProviderUrl()` 那行 `originInvoker.getUrl().getAttribute(EXPORT_KEY)` 的来历。没有注册中心时直接 `doExportUrl(url, true, registerType)`，adaptive 按 `url.getProtocol()` 选中 `DubboProtocol`。

`proxyFactory.getInvoker(ref, interfaceClass, url)` 返回的是 `AbstractProxyInvoker` 的匿名子类，`doInvoke` 只有一行：默认的 `JavassistProxyFactory` 是 `wrapper.invokeMethod(proxy, methodName, parameterTypes, arguments)`（`Wrapper.getWrapper()` 为每个实现类生成一个方法分发表），`JdkProxyFactory` 则是 `proxy.getClass().getMethod(methodName, parameterTypes).invoke(proxy, arguments)`。所以 provider 侧的 Invoker 非常薄：不做序列化、不做线程池、不做过滤器，只负责"按方法名和参数类型调到实现类"。`invoke()` 外层把返回值 `wrapWithFuture()` 包成 `CompletableFuture`，再用 `AppResponse` 承载结果或异常——异步方法和同步方法在这层被统一成 `CompletableFuture`，这也是 provider 实现类可以返回 `CompletableFuture` 而调用方无感的原因。

端口真正打开的位置在 `DubboProtocol.export()` → `openServer(url)`：

```java
String key = url.getAddress();               // host:port 粒度，不是服务粒度
ProtocolServer server = serverMap.get(key);
if (server == null) {
    synchronized (this) {
        server = serverMap.get(key);
        if (server == null) {
            serverMap.put(key, createServer(url));   // Exchangers.bind() 在这里发生
            return;
        }
    }
}
server.reset(url);                           // 已存在则只更新参数（序列化、线程池等）
```

所以"一个服务一个端口"是错的：同一 `host:port` 上的所有服务共享一个 `ProtocolServer`，`DubboExporter` 按 `serviceKey` 进 `exporterMap`，请求进来后按同一个 key 反查 exporter。

这个反查是理解"服务明明导出了却找不到"的关键。`DubboProtocol.export()` 里 `String key = serviceKey(url)`，`serviceKey(URL)` 取 `BIND_PORT_KEY` 而不是 `url.getPort()`，再和 `path`/`version`/`group` 拼成 key，`DubboExporter` 带着这个 key 进 `exporterMap`。请求到达时 `getInvoker(channel, invocation)` 用 `channel.getLocalAddress().getPort()` 加上请求 attachment 里的 `path`、`version`、`group` 拼出同一个 key 去查。三者任一不一致，就会看到 `Not found exported service`——端口是通的、服务也确实导出了，只是 key 对不上。改 `path` 或 `group` 之后消费端没同步，就是这类故障。

还有一层：过滤器链不是 `ServiceConfig` 加的。`ProtocolFilterWrapper.export()` 开头就有 `if (UrlUtils.isRegistry(invoker.getUrl())) { return protocol.export(invoker); }`——`registry://` 这一层直接透传。真正的 `buildInvokerChain(invoker, SERVICE_FILTER_KEY, PROVIDER)` 发生在 `RegistryProtocol.doLocalExport()` 里再以 provider URL 调 `protocol.export(invokerDelegate)` 的时候。换句话说，Filter 链是"本地导出"这一步织进去的，注册中心那一层不参与。

### 3.4 RegistryProtocol 先导出再注册，延迟与幂等各有各的实现

`RegistryProtocol.export()` 的执行顺序是有讲究的：

```java
URL providerUrl = getProviderUrl(originInvoker);            // 从 EXPORT_KEY 取出真正的 provider url
final URL overrideSubscribeUrl = getSubscribedOverrideUrl(providerUrl);
overrideListeners.computeIfAbsent(...).add(overrideSubscribeListener);   // 先登记 override 监听
providerUrl = overrideUrlWithConfig(providerUrl, overrideSubscribeListener);  // 应用已有的配置覆盖
final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker, providerUrl);  // ① 开端口
final Registry registry = getRegistry(registryUrl);
final URL registeredProviderUrl = customizeURL(providerUrl, registryUrl);
boolean register = providerUrl.getParameter(REGISTER_KEY, true) && registryUrl.getParameter(REGISTER_KEY, true);
if (register) {
    register(registry, registeredProviderUrl);              // ② 再写注册中心
}
```

①在②之前，保证"注册中心里出现的地址，端口一定已经在监听"。反过来不成立：`register()` 抛异常时，本地端口已经开了，这也是"端口被占用但异常看起来像注册失败"的来源之一。

`protocolSPI.export()` 选中的实现不一定是 `RegistryProtocol` 本身。`dubbo-registry-api` 的 SPI 文件里 `registry` 对应 `InterfaceCompatibleRegistryProtocol`，`service-discovery-registry` 对应 `RegistryProtocol`，前者是接口级兼容实现，后者是应用级实现。`ConfigValidationUtils.genCompatibleRegistries()` 在 provider 侧按 `register-mode` 决定生成哪些 registry URL：接口注册中心默认 `DEFAULT_REGISTER_MODE_ALL`，会额外补一个 `service-discovery-registry://`；服务发现注册中心默认 `DEFAULT_REGISTER_MODE_INSTANCE`，只有显式配 `all` 才补接口级地址。两个 URL 走两个不同的 RegistryProtocol 实现，各自导出一份、注册一份。

`doLocalExport()` 里有两层缓存：`bounds` 是 `Map<providerUrlKey, Map<registryUrlKey, ExporterChangeableWrapper<?>>>`，注释写得很直白——"To solve the problem of RMI repeated exposure port conflicts, the services that have been exposed are no longer exposed"。`computeIfAbsent` 保证同一 provider URL + 同一 registry URL 只导出一次，返回的 `ExporterChangeableWrapper` 支持在配置变更时 `setExporter(protocol.export(...))` 换掉内部 exporter 而不改外部引用。计数由 `ReferenceCountExporter` 维护。

`ExporterChangeableWrapper` 的存在是为了应对运行期配置变更。注册中心的 configurator 推送过来后，`OverrideListener.notify()` 会重算 URL，若 `!newUrl.equals(currentUrl)` 且 `newUrl.getParameter(Constants.NEED_REEXPORT, true)`，就调 `RegistryProtocol.reExport(originInvoker, newUrl)`：`exporter.setExporter(protocol.export(new InvokerDelegate<>(originInvoker, newUrl)))` 换掉内部 exporter，再 `reExportUnregister` 旧地址、`reExportRegister` 新地址。端口没动，`bounds` 里的映射也没动，变的只是 URL 和注册内容——这就是"改个 timeout 不用重启"的底层机制。返回给 `ServiceConfig` 的是 `DestroyableExporter`，一个薄壳，只转发 `getInvoker`/`unexport`/`register`/`unregister`，注释写明"Ensure that a new exporter instance is returned every time export"，防止调用方把 `ExporterChangeableWrapper` 当成可长期持有的对象。

`delay` 的实现很短：`shouldDelay()` 就是 `getDelay() != null && getDelay() > 0`，`doDelayExport()` 把 `doExport(AUTO_REGISTER)` 丢给 `ExecutorRepository.getServiceExportExecutor().schedule(..., getDelay(), MILLISECONDS)`。此时 `export()` 已经返回，`isExported()` 仍是 `false`，`exporters` 还是空的。配套的一行在 `DefaultModuleDeployer.registerServiceInternal()`：`if (sc.shouldDelay()) { return; }`——延迟导出的服务，deployer 不会替它补注册，注册动作完全由延迟任务内部的 `doExport` 完成。

`delay` 的典型用途不是"延迟启动"，而是"等依赖就绪"。比如服务启动时要连数据库预热本地缓存，如果立刻注册，流量会在预热完成前打进来。把 `delay` 设成略大于预热时间，端口和注册都推迟，消费端在这段时间里看到的是"没有这个 provider"，请求会打到别的实例上。代价是这段时间本机的 `@DubboReference` 也引用不到自己——除非走 injvm，因为 `exportLocal()` 同样在 `doExport` 里，一起被推迟了。

幂等分三层：`exported` 的 volatile 快检、`synchronized` 内的二次检查、`doExport()` 里的第三次检查，加上 `unexported` 的单向门。`exported` 只在 `doExportUrls()` 全部跑完之后由 `exported()` 方法置位，所以中途失败不会留下"半导出但标记已完成"的状态，可以重试。`unexport()` 的顺序也值得看：先对所有 exporter 调 `unregister()`，再 `waitForIdle()` 等最后一次调用结束（默认最多等 `ConfigurationUtils.getServerShutdownTimeout()`），最后才 `unexport()` 真正关端口——先摘流量再关服务。`waitForIdle()` 的算法很细：`idleTime > timeout` 直接返回；`timeout - idleTime < timeout/3` 说明刚被调用过，睡到超时；否则以 `timeout/3` 为粒度轮询 `providerModel.getLastInvokeTime()`，最长不超过 `timeout`。这段代码解决的是"注册中心已经把我摘了，但还有请求在路上"的窗口期问题。

**容易误解的地方**：

1. **"导出就是打开端口"**。`injvm` 导出完全不碰网络，只是往 Map 里放一个 `InjvmExporter`；`dubbo` 导出的端口是 `ProtocolServer` 级别共享的，一个 `host:port` 上可以挂几十个服务。判断"到底开没开端口"要看 `exporter.getInvoker().getUrl().getProtocol()`，而不是看日志里有没有 `Export dubbo service`。
2. **"URL 是我配出来的"**。用户能控制的只有少数几个 key，`side`、`dubbo`、`release`、`timestamp`、`pid`、`methods`、`revision`、`bind.ip`、`bind.port`、`anyhost` 全是运行时算出来的。排查 URL 问题时不要在配置文件里找 `bind.port`，它来自 `findConfiguredPort()`。
3. **"先注册再导出"**。源码顺序是先 `doLocalExport` 再 `register`。所以注册中心有地址却调不通，方向应该往"端口在但业务逻辑/序列化/线程池有问题"查，而不是怀疑注册。
4. **"3.3 一个服务只注册一次"**。`ConfigValidationUtils.loadRegistries(..., true)` 之后的 `genCompatibleRegistries()` 会按 `register-mode`（接口注册中心默认 `all`）同时产出 `service-discovery-registry://` 和 `registry://` 两个 URL，于是同一个服务会导出两次、注册两次：一次应用级实例，一次接口级地址。

**记住这一句**：导出 = 把实现类包成 Invoker，把 Invoker 交给 Protocol 换成 Exporter；URL 是这条链上唯一的契约，端口只是 Protocol 的一个副作用。
