# 第2章 SPI：Dubbo 的扩展点体系

**一句话**：每个可变行为都是一个 @SPI 接口，配置只决定取哪个名字。

**为什么需要它**：

先算一笔账。给 Dubbo 加一种序列化方式，要改多少处？

如果按"工厂 + if/else"的常规写法：改工厂的 switch、改序列化 ID 枚举、改配置项校验、改文档；更麻烦的是每个用到序列化的地方——`ExchangeCodec` 编码、`DecodeableRpcInvocation` 解码、泛化调用、telnet 的 `invoke` 命令、`PermittedSerializationKeeper` 的白名单校验——都得同步加分支。少改一处，就是一个只在特定入口复现的 `SerializationException`。

Dubbo 的实际做法是两个文件：

```java
// 新增一个类
public class Hessian2Serialization implements Serialization { ... }
```

```properties
# dubbo-serialization-hessian2/src/main/resources/META-INF/dubbo/internal/org.apache.dubbo.common.serialize.Serialization
hessian2=org.apache.dubbo.common.serialize.hessian2.Hessian2Serialization
```

框架代码零改动。全仓 3915 个 java 文件里，这种"接口 + 目录里的 `name=class`"构成了框架全部的可替换面：`Protocol`、`ProxyFactory`、`Cluster`、`LoadBalance`、`Router`、`Filter`、`Serialization`、`Transporter`、`Codec`、`Dispatcher`、`ThreadPool`、`Registry`、`Configurator`、`Compiler`……都是同一个机制。

这一章要讲清这套机制的四个部件，以及它们各自的代价：**按名查找**（和 JDK SPI 的第一个差别）、**IoC 注入**、**Wrapper 自动包装**、**Adaptive 运行时生成类**。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `@SPI` | 标在接口上，声明这是扩展点。`value()` 是默认扩展名，`scope()` 决定实例共享范围（FRAMEWORK / APPLICATION / MODULE / SELF） |
| `ExtensionLoader<T>` | 一个扩展点一个 loader。读目录、缓存 Class、造实例、注入依赖、套 Wrapper、生成 Adaptive，全在这一个类里 |
| `ExtensionDirector` | loader 的容器，按 Framework → Application → Module 三级持有，子级找不到向父级要 |
| `@Adaptive` | 标在方法上：运行时按 URL 参数选实现；标在类上：手写自适应类，跳过生成 |
| `AdaptiveClassCodeGenerator` | 拼出 `Xxx$Adaptive` 的 Java 源码，交给 `Compiler` 扩展点编译 |
| `@Activate` | 声明"什么条件下自动启用"，给 Filter 这类集合型扩展点用。有 `group` / `value` / `order` / `onClass` |
| `@Wrapper` | 可选，限制某个 wrapper 只包装哪些名字（`matches` / `mismatches` / `order`） |
| `@DisableInject` | 标在 setter 上，跳过 IoC 注入 |
| `LoadingStrategy` | 目录发现策略，本身也是 SPI（用 JDK `ServiceLoader` 加载） |
| `ExtensionPostProcessor` | 实例创建前后的钩子，`postProcessBeforeInitialization` / `postProcessAfterInitialization` |
| `cachedClasses` | 名字 → Class，`Holder<Map>` 持有，一次加载终身使用 |
| `cachedInstances` | 名字 → 实例，`ConcurrentMap<String, Holder<Object>>` |
| `cachedAdaptiveInstance` | 自适应实例，进程内一个 |
| `unacceptableExceptions` | 加载期就判定"这个名字废了"的集合，命中直接抛，不再尝试 |

**关键代码**：

第一段是取实例的主路径。两层缓存、一把细粒度锁，加一个容易忽略的特殊值：

```java
// ExtensionLoader.java:557
public T getExtension(String name, boolean wrap) {
    checkDestroyed();
    if (StringUtils.isEmpty(name)) {
        throw new IllegalArgumentException("Extension name == null");
    }
    if ("true".equals(name)) {
        return getDefaultExtension();                 // 562: "true" 是"取默认实现"的暗号，不是名字
    }
    String cacheKey = name;
    if (!wrap) {
        cacheKey += "_origin";                        // 565-568: 包装版与裸版分开缓存，避免互相污染
    }
    final Holder<Object> holder = getOrCreateHolder(cacheKey);
    Object instance = holder.get();
    if (instance == null) {
        synchronized (holder) {                       // 570: 锁在 holder 上，不同名字的创建互不阻塞
            instance = holder.get();
            if (instance == null) {
                instance = createExtension(name, wrap);
                holder.set(instance);
            }
        }
    }
    return (T) instance;
}
```

第二段是 `createExtension`。注意执行顺序：**先注入，再包装，最后初始化**。顺序错了会拿到没注入依赖的 wrapper；而且 `initExtension` 是对最外层实例调用的，最外层如果不是 `Lifecycle`，钩子压根不会执行——源码在 811-812 留了这条警告注释：

```java
// ExtensionLoader.java:772
private T createExtension(String name, boolean wrap) {
    Class<?> clazz = getExtensionClasses().get(name);          // 773: 只查 cachedClasses，不重复扫目录
    if (clazz == null || unacceptableExceptions.contains(name)) {
        throw findException(name);                             // 775: 带上所有加载期异常，告诉你为什么没找到
    }
    try {
        T instance = (T) extensionInstances.get(clazz);         // 778: 按 Class 缓存的"裸实例"
        if (instance == null) {
            extensionInstances.putIfAbsent(clazz, createExtensionInstance(clazz));
            instance = (T) extensionInstances.get(clazz);
            instance = postProcessBeforeInitialization(instance, name);
            injectExtension(instance);                          // 783: IoC 在这里，注入的是"自适应"依赖
            instance = postProcessAfterInitialization(instance, name);
        }

        if (wrap) {
            List<Class<?>> wrapperClassesList = new ArrayList<>();
            if (cachedWrapperClasses != null) {
                wrapperClassesList.addAll(cachedWrapperClasses);
                wrapperClassesList.sort(WrapperComparator.COMPARATOR);   // 791: 按 order 升序
                Collections.reverse(wrapperClassesList);                 // 792: 反转后逐层包
            }
            for (Class<?> wrapperClass : wrapperClassesList) {
                // 798-801: @Wrapper 的 matches/mismatches 在这里生效
                instance = injectExtension(
                        (T) wrapperClass.getConstructor(type).newInstance(instance));  // 804: wrapper 自己也要注入
            }
        }

        initExtension(instance);                                // 813: Lifecycle.initialize，在最外层实例上
        return instance;
    } catch (Throwable t) { ... }
}
```

**拆解**：

### 2.1 和 JDK SPI 的三个差别

**差别一：按名取，而且有默认值。**

JDK 的 `ServiceLoader` 有三个具体缺陷：一是"全量迭代"，一个接口的所有实现全造出来，你只能拿第一个；二是没有"默认实现"的概念；三是没法只取指定名字的那一个，要么全遍历，要么自己过滤。Dubbo 的目录文件是键值对：

```
# META-INF/dubbo/internal/org.apache.dubbo.rpc.Filter
echo=org.apache.dubbo.rpc.filter.EchoFilter
generic=org.apache.dubbo.rpc.filter.GenericFilter
timeout=org.apache.dubbo.rpc.filter.TimeoutFilter
```

`@SPI` 的 javadoc 专门解释了为什么要从"一行一个全限定名"改成键值对：老格式下如果某个实现的静态字段引用了不存在的第三方库，类初始化失败时框架根本不知道该报哪个名字，只能笼统说"扩展加载失败"。改成键值对后，`name` 在解析阶段就拿到了，异常能精确定位到 `exceptions.put(line, e)`（`loadResource:1176`），最后由 `findException`（747）拼成"possible causes"列表。这就是为什么你看到的错误长这样：

```
No such extension org.apache.dubbo.rpc.Protocol by name dubbo, possible causes:
(1) dubbo=org.apache.dubbo.rpc.protocol.dubbo.DubboProtocol: ... ClassNotFoundException ...
```

**差别二：IoC 注入。**

`injectExtension`（854）扫描实例的所有 `set*` 方法，`isSetter`（937）的判定是：`public`、单参数、以 `set` 开头。然后逐条过滤：

- 标了 `@DisableInject` 的跳过（867）
- 声明类是 `ScopeModelAware` 的跳过（873）
- 参数是基本类型/包装类型的跳过（883）

剩下的，用 `getSetterProperty`（921）把 `setProtocol` 转成 `protocol`，调 `injector.getInstance(pt, property)`。这个 `injector` 是 `ExtensionInjector` 的自适应实例，而 `SpiExtensionInjector.getInstance`（37）的实现只有一句关键逻辑：

```java
if (!loader.getSupportedExtensions().isEmpty()) {
    return loader.getAdaptiveExtension();     // 永远返回自适应实例，不按 name 找
}
```

**注入进来的永远是自适应扩展，不是某个具体实现。** 这就是为什么 `RegistryProtocol` 里那个 `protected Protocol protocol;`（`RegistryProtocol.java:185`）能用同一个字段既 `refer` 出 dubbo 又 `refer` 出 tri——它拿到的是 `Protocol$Adaptive`，按 url 的协议在调用时分发。也是为什么你在自定义扩展里 `setXxx` 注入进来的东西，`getClass().getName()` 会带一个 `$Adaptive` 后缀。

**差别三：Wrapper 自动包装。**

只要一个类有 `(T)` 单参构造，`isWrapperClass`（1408）就判它是 wrapper，它不进名字空间，而是进 `cachedWrapperClasses`，被套在所有实现外面。`Protocol` 有五个 wrapper，注册在三个不同的模块里：

```
dubbo-rpc-api/META-INF/dubbo/internal/org.apache.dubbo.rpc.Protocol
  listener=ProtocolListenerWrapper
  serializationwrapper=ProtocolSerializationWrapper
  securitywrapper=ProtocolSecurityWrapper
  invokercount=InvokerCountWrapper
dubbo-cluster/META-INF/dubbo/internal/org.apache.dubbo.rpc.Protocol
  filter=ProtocolFilterWrapper
```

这就是 Dubbo 的 AOP。代价是：wrapper 对**所有**实现生效，`getExtension("dubbo")` 拿到的不是 `DubboProtocol`，而是一个五层嵌套的壳。想 debug 到真正的 `DubboProtocol.export`，得在栈里往下穿五层；想确认某个 wrapper 有没有生效，看栈比看配置快。

### 2.2 Adaptive：为什么要运行时生成一个类

矛盾很具体：`Protocol.refer(Class, URL)` 在**调用时**才知道用哪个协议，而 IoC 需要先有一个 `Protocol` 实例才能注入。如果 `DubboProtocol` 在启动时被注入进去，那 tri 协议就没法用了。

解决办法是引入一个"延迟决策"的中间人：`Protocol$Adaptive`。它实现 `Protocol`，每个方法内部读 url、算扩展名、`getExtension`、转发。

问题在于：这个中间人如果手写，就要为每个 SPI 接口手写一个分发类。Dubbo 有几十个 SPI 接口，其中带 `@Adaptive` 方法的十几个，手写不可维护。所以：**运行时按接口的方法签名生成这个类**。

生成规则（`AdaptiveClassCodeGenerator`）：

- 类名固定 `接口简单名$Adaptive`（`CODE_CLASS_DECLARATION:49`），包名跟接口一致
- 没有 `@Adaptive` 的方法，直接 `throw new UnsupportedOperationException`（`CODE_UNSUPPORTED:57`）——所以自适应实例只能调用标了 `@Adaptive` 的方法，调别的直接炸
- 找 URL：方法参数里直接有 `URL` 类型就用它（`getUrlTypeIndex:167`）；没有就找参数上返回 `URL` 的 getter（`generateUrlAssignmentIndirectly:382`），优先 `getUrl`
- 空值检查是双重：先查参数本身，再查 `getUrl()` 的返回值（`generateGetUrlNullCheck:421-433`）
- 取名字的 key 来自 `@Adaptive.value()`；为空则把接口名驼峰拆点（`getMethodAdaptiveValue:365`），`Protocol` → `protocol`
- `protocol` 是特例：不读 `url.getParameter("protocol")`，直接读 `url.getProtocol()`（`generateExtNameAssignment:278-288`）
- 最后 `scopeModel.getExtensionLoader(X.class).getExtension(extName)`（`CODE_EXTENSION_ASSIGNMENT:74`）

产物长这样。这份文件在仓库里作为"期望产物"保存，测试拿它跟生成结果逐字比对：

```java
// dubbo-common/src/test/resources/org/apache/dubbo/common/extension/adaptive/HasAdaptiveExt$Adaptive
public class HasAdaptiveExt$Adaptive implements org.apache.dubbo.common.extension.adaptive.HasAdaptiveExt {
  public java.lang.String echo(org.apache.dubbo.common.URL arg0, java.lang.String arg1) {
    if (arg0 == null) throw new IllegalArgumentException("url == null");
    org.apache.dubbo.common.URL url = arg0;
    String extName = url.getParameter("has.adaptive.ext", "adaptive");   // 带默认值，来自 @SPI("adaptive")
    if (extName == null) throw new IllegalStateException("Failed to get extension ...");
    ScopeModel scopeModel = ScopeModelUtil.getOrDefault(url.getScopeModel(), ...HasAdaptiveExt.class);
    ...HasAdaptiveExt extension = (...HasAdaptiveExt) scopeModel.getExtensionLoader(...).getExtension(extName);
    return extension.echo(arg0, arg1);
  }
}
```

编译用的是 `Compiler` 扩展点，默认 javassist：

```java
// ExtensionLoader.java:1465
String code = new AdaptiveClassCodeGenerator(type, cachedDefaultName).generate();
org.apache.dubbo.common.compiler.Compiler compiler = extensionDirector
        .getExtensionLoader(org.apache.dubbo.common.compiler.Compiler.class)
        .getAdaptiveExtension();
return compiler.compile(type, code, classLoader);     // 1469: ClassLoader 必须是接口的，不能是框架的
```

三个细节值得单独记：

**native image 下不生成。** 1459-1461 先试 `classLoader.loadClass(type.getName() + "$Adaptive")`，AOT 阶段已经把类编好了，运行时只是加载。

**分发发生在每次调用。** 不是启动时决定一次。所以同一个 `Protocol$Adaptive` 实例，第一次调用 url 协议是 dubbo 就走 `DubboProtocol`，第二次是 tri 就走 `TripleProtocol`。这是 Dubbo 能同时消费多种协议的根本原因，也是为什么 `getExtension` 的缓存键必须是名字而不是 URL。

**生成失败会连坐。** 一个接口里只要有一个 `@Adaptive` 方法既没有 URL 参数、也没有返回 URL 的 getter，`generateUrlAssignmentIndirectly` 就抛 `IllegalStateException`（402），整个接口的自适应类都生成不出来。写自定义 SPI 时这条最容易踩。

### 2.3 Activate：排序与条件

`getActivateExtension(url, key, group)`（343）是 Filter 链的唯一来源。一个扩展要进链，三个条件同时成立：

**条件一，group 匹配**（`isMatchGroup:460`）。`@Activate(group = PROVIDER)` 只在 group 参数是 `provider` 时启用；调用方传 `null` 则匹配所有。`ProtocolFilterWrapper` 里两次调用分别是 `SERVICE_FILTER_KEY, "provider"`（61）和 `REFERENCE_FILTER_KEY, "consumer"`（75）。

**条件二，没被点名排除。** 如果 `names` 里出现 `-name`，该扩展不启用（397）；出现 `-default`，整批默认扩展都不启用（351）。这就是配置里 `filter="-monitor"` 的实现。

**条件三，URL 条件**（`isActive:470`）。`@Activate(value = "cache")` 要求 url 里有 `cache` 参数；`value = "k:v"` 还要求值等于 `v`——`keyPairs` 的解析在 377-386。真实的例子：`ExecuteLimitFilter` 是 `@Activate(group = PROVIDER, value = EXECUTES_KEY)`，不配 `executes` 就根本不加载。

要分清 `value` 和 `order` 是两套独立的过滤：`value` 决定**启不启用**，`order` 只决定**排第几**。写了 `value = "cache"` 但 url 里没有 `cache`，`order` 再小也不会出现——排查"filter 顺序不对"之前，先确认它到底有没有被启用。

排序靠 `TreeMap<Class<?>, T>(activateComparator)`（346）。`ActivateComparator` 的规则是：先比 `@Activate(before/after)`（2.7.0 起废弃），再比 `order`，`order` 相等则比类简单名（117-123）——注释写得很直白，为了避免不同加载顺序下两个 order 相同的 filter 顺序不一致。

真实数值可以拿来校准直觉：`ProfilerServerFilter` 的 `order = Integer.MIN_VALUE`，`ClassLoaderFilter` 是 `-30000`，`ProtocolFilterWrapper` 是 `100`，`ProtocolSecurityWrapper` 和 `ProtocolListenerWrapper` 都是 `200`。

**order 小的在外层。** `DefaultFilterChainBuilder.buildInvokerChain`（68）从 `filters.size() - 1` 往 `0` 遍历，每轮用当前累积结果当 `next`，所以 `filters.get(0)` 最终在最外面。Wrapper 那边同理：升序排完再 `reverse`，倒序遍历时第一个（order 最大的）被最先包，落在最内层。

还有一个容易忽略的语法：`names` 里出现 `default` 时，返回顺序按 `names` 的字面顺序排，`default` 那一项展开成整批默认扩展（405-422）。源码注释的原话是 "`ext1,default,ext2` means ext1 will happens before all of the default extensions while ext2 will after them"。所以 `filter="a,default,b"` 的语义不是"a、b、再加默认"，而是"a 在最前，默认那批居中，b 在最后"。这是精确控制 Filter 位置的唯一手段。

`onClass` 是另一套机制，作用在**加载期**而不是启用期：`loadClassIfActive`（1325-1329）要求 `onClass` 里列的类**全部**存在于 classpath，否则这个扩展连名字都不会出现。用来避免 optional 依赖——比如某个 filter 依赖 Sentinel，没引 Sentinel 时它就不该出现在 `getSupportedExtensions()` 里。

### 2.4 缓存与目录优先级

文档常说"三级缓存"，实际是四层，第四层是 3.x 才加的：

| 缓存 | 声明位置 | 键 | 说明 |
|---|---|---|---|
| `cachedClasses` | 125 | 名字 | `Holder<Map<String, Class<?>>>`，`loadExtensionClassesLock` 保护（953-979） |
| `cachedInstances` | 130 | 名字 | `ConcurrentMap<String, Holder<Object>>`，锁在 Holder 上，不同名字并行创建 |
| `cachedAdaptiveInstance` | 131 | 无 | 单例。失败时异常存进 `createAdaptiveInstanceError`（134） |
| `extensionInstances` | 116 | Class | `ConcurrentMap<Class<?>, Object>`，**裸实例**，别名共享 |

`extensionInstances` 的存在解释了一个常见困惑：配置里写 `a=X, b=X`，`getExtension("a")` 和 `getExtension("b")` 拿到的是**同一个对象**（778 先按 Class 查），但 `cachedInstances` 里是两个 Holder，各自包了不同的 Wrapper 链。所以"两个名字两个实例"只对包装层成立。如果你的扩展是有状态的，别名会让两个名字共享状态——这通常不是你想要的。

`Holder` 本身很简单，一个 `volatile` 字段加 getter/setter（`Holder.java:24-31`）。它的作用不是"原子性"，而是"提供一个可以加锁的对象"——`synchronized (holder)` 锁的是这个 Holder，不同名字的 Holder 不同，所以并发取不同扩展不互斥。

目录优先级由 `LoadingStrategy` 的 `getPriority()` 决定**加载顺序**，由 `overridden()` 决定**能不能覆盖**：

| 目录 | 优先级 | overridden | 撞名时 |
|---|---|---|---|
| `META-INF/dubbo/internal/` | `MAX_PRIORITY`（最先加载） | `false` | 抛 `IllegalStateException`，并加入 `unacceptableExceptions` |
| `META-INF/dubbo/` | `NORMAL_PRIORITY` | `true` | 覆盖已加载的 |
| `META-INF/services/` | `MIN_PRIORITY`（最后加载） | `true` | 覆盖已加载的 |

注意 `Prioritized.MAX_PRIORITY` 的值是 `Integer.MIN_VALUE`——**数值越小优先级越高**，和 Spring 的 `Ordered` 相反，第一次看容易读反。

覆盖逻辑在 `saveInExtensionClass`（1345）：已有同名且 `overridden` 为真就 `put` 覆盖；`overridden` 为假且不是同一个类，直接抛异常。所以覆盖权是 **services > dubbo > internal**，跟"internal 最优先"的直觉正好相反。internal 目录的设计意图是"框架自己的实现，不许被悄悄替换"，所以它 `overridden()` 返回默认的 `false`——这不是疏忽，是策略。

还有一处副作用：撞名时 `unacceptableExceptions.add(name)`（1352）会把这个名字永久拉黑，之后 `createExtension`（774）看到它就直接抛，哪怕另一个模块后来提供了合法实现。多 ClassLoader 容器里"扩展明明在，就是加载不到"，通常就是这个集合在起作用。

`META-INF/services` 走的是 JDK 格式（一行一个全限定名，没有 key），名字由 `findAnnotationName`（1419）推出来：去掉接口简单名后缀再转小写。三个策略本身也是通过 `META-INF/services/org.apache.dubbo.common.extension.LoadingStrategy` 注册的，`loadLoadingStrategies`（173）用 JDK `ServiceLoader` 加载后按优先级排序——这里恰好是"用 JDK SPI 加载 SPI 加载器"，套了一层。

另外两个性能相关的细节：

- `special_spi.properties`（114、183-188）。某些 SPI 只由框架实现，扫多个 ClassLoader 会让启动慢几百毫秒，所以用这个文件把它们钉死成"只从框架 ClassLoader 加载"（1058-1066）。
- `urlListMapCache`（144-145）。SPI 文件内容按 URL 缓存在 `SoftReference<Map<URL, List<String>>>` 里（1190-1223），避免同一个 jar 里的目录被反复读盘。内存紧张时整块被回收，不会 OOM。

**容易误解的地方**：

1. **`getAdaptiveExtension()` 的失败是"粘住"的。** `createAdaptiveInstanceError` 一旦被赋值（737），后续每次调用都直接抛同一个异常（724-728），不会重试。本地改了代码、确认编译通过、重启 Spring 上下文都没用——必须重启 JVM。排查"明明改对了还报旧错"时，先怀疑这里。

2. **`@Adaptive` 标在类上会"关掉"生成。** `getAdaptiveExtensionClass`（1447）先看 `cachedAdaptiveClass`，有就直接用，压根不走代码生成。`AdaptiveExtensionInjector`（33）就是这么手写的——它要在多个 `ExtensionInjector` 之间做"第一个返回非 null 的赢"（56-60），这个逻辑没法用模板生成。

3. **`@SPI` 不写 value 就没有默认实现。** `cacheDefaultExtensionName`（1024）只在 `@SPI` 的 `value()` 非空时才给 `cachedDefaultName` 赋值，否则一直是 null，`getDefaultExtension`（598）因此返回 null；生成的自适应类里也退化成 `url.getParameter("x")`，不带默认值（295），取不到就抛 `IllegalStateException`。`Serialization` 就是这种：`@SPI(scope = FRAMEWORK)` 没有 value，所以必须显式配 `serialization=hessian2`，或者由上层 URL 补上这个参数。

4. **Wrapper 不占名字，`getExtension` 也管不到它。** `loadClass`（1283-1287）的分支顺序是：先看 `@Adaptive`，再看是不是 wrapper，都不是才进名字空间。所以注册文件里那行 `filter=...ProtocolFilterWrapper` 的 key 被直接丢弃，`getExtension("filter")` 会抛 "Not find extension"；你只能 `getExtension("dubbo")`，拿到的是被五层 wrapper 包好的实例。想让某个 wrapper 不生效，只有两条路：改代码，或者从注册文件里删掉那一行。

**记住这一句**：SPI 把"用哪个实现"从编译期搬到了 URL 参数里，Adaptive 就是这次搬运的执行者。
