# 第5章 和 Spring 的接缝：@DubboService 是怎么被扫描到的

**一句话**：注解扫成 ServiceBean，Deployer 统一导出。

**为什么需要它**：一个标了 `@DubboService` 的实现类，Spring 容器里明明只是一个普通 Bean，为什么它会自动监听端口、注册到注册中心？更具体的麻烦是：同一个接口、两个不同 `group` 的实现放在一个 JVM 里，为什么不会互相覆盖？为什么在 Spring Boot 里只加一个 starter、配一行 `dubbo.scan.base-packages`，服务就起来了，而在纯 Spring XML 里却要写 `<dubbo:annotation package="..."/>`？还有，为什么有人给 `ServiceBean` 加 `ApplicationListener` 想监听"导出完成"，时序总是对不上？这些问题的答案都不在业务代码里，而在 Spring 的 `BeanDefinition` 生命周期和 Dubbo 的部署器之间那道接缝里。这章把这条链路拆开：XML 时代靠 NamespaceHandler 把标签翻译成 BeanDefinition，注解时代靠 `BeanDefinitionRegistryPostProcessor` 抢在 Bean 实例化之前注册 `ServiceBean`，`@DubboReference` 靠两段式后处理器注入懒加载代理，最后由 `DefaultModuleDeployer` 统一触发 `export()`；而 3.x 引入的 `FrameworkModel / ApplicationModel / ModuleModel` 三层，决定了这些 Bean 到底归谁管、配置往哪放、什么时候销毁。

**核心结构**：

| 概念 | 作用 |
|---|---|
| `spring.handlers` / `spring.schemas` | 把 `http://dubbo.apache.org/schema/dubbo` 命名空间映射到 `DubboNamespaceHandler`，XSD 落到 `META-INF/dubbo.xsd` |
| `DubboNamespaceHandler` | 继承 `NamespaceHandlerSupport`，`init()` 里为 `service` / `reference` / `application` 等标签注册 `DubboBeanDefinitionParser`；`parse()` 里额外调 `DubboSpringInitializer.initialize(registry)` |
| `DubboBeanDefinitionParser` | 实现 `BeanDefinitionParser`，把 XML 元素解析成 `RootBeanDefinition`；`beanClass` 为 `ServiceBean` 时设 `AUTOWIRE_CONSTRUCTOR` 并把 `<dubbo:service class="...">` 变成 `ref` 属性 |
| `ServiceBean` / `ReferenceBean` | `ServiceBean<T> extends ServiceConfig<T>` 且实现 `InitializingBean`；`ReferenceBean<T>` 实现 `FactoryBean<T>`，产出消费端代理 |
| `DubboComponentScanRegistrar` | `ImportBeanDefinitionRegistrar`，解析 `@DubboComponentScan` / `@EnableDubbo` 的 `basePackages`，注册 `ServiceAnnotationPostProcessor` 的 BeanDefinition |
| `ServiceAnnotationPostProcessor` | `BeanDefinitionRegistryPostProcessor`，用 `DubboClassPathBeanDefinitionScanner` 扫 `@DubboService`，为每个命中类注册一个 `ServiceBean` BeanDefinition |
| `ReferenceAnnotationBeanPostProcessor` | `BeanFactoryPostProcessor` + `InstantiationAwareBeanPostProcessor`：先注册 `ReferenceBean` 定义，后在属性注入阶段把代理灌进 `@DubboReference` 字段 |
| `DubboAutoConfiguration` | Spring Boot 自动配置，类级 `@ConditionalOnProperty(prefix="dubbo", name="enabled", matchIfMissing=true)`，方法级再按 `dubbo.scan.base-packages` 决定要不要注册扫描器 |
| `DubboSpringInitializer` | 按 `BeanDefinitionRegistry` 缓存 `DubboSpringInitContext`，创建/复用 `ApplicationModel` 与 `ModuleModel`，并作为单例注册进 BeanFactory |
| `DefaultModuleDeployer` | 每个 `ModuleModel` 一个，`start()` → `exportServices()` → 遍历 `configManager.getServices()` 调 `sc.export(...)` |

**关键代码**：

```java
// ServiceAnnotationPostProcessor.java:106
public class ServiceAnnotationPostProcessor implements BeanDefinitionRegistryPostProcessor, ... {

    // ServiceAnnotationPostProcessor.java:169
    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        this.registry = registry;
        scanServiceBeans(resolvedPackagesToScan, registry);   // 为什么：Registry 后处理器先于普通 Bean 实例化，此时才允许往容器里塞新 BD
    }

    // ServiceAnnotationPostProcessor.java:205（骨架节选）
    private void scanServiceBeans(Set<String> packagesToScan, BeanDefinitionRegistry registry) {
        scanned = true;
        // ... 空包校验、ServicePackagesHolder.isPackageScanned 去重
        DubboClassPathBeanDefinitionScanner scanner =
                new DubboClassPathBeanDefinitionScanner(registry, environment, resourceLoader);
        BeanNameGenerator beanNameGenerator = resolveBeanNameGenerator(registry);
        scanner.setBeanNameGenerator(beanNameGenerator);
        for (Class<? extends Annotation> annotationType : serviceAnnotationTypes) {
            scanner.addIncludeFilter(new AnnotationTypeFilter(annotationType));  // @DubboService / 旧 @Service / 兼容 @com.alibaba...@Service
        }
        for (String packageToScan : packagesToScan) {
            scanner.scan(packageToScan);                       // 为什么：先把实现类本身注册成 Spring Bean
            Set<BeanDefinitionHolder> beanDefinitionHolders =
                    findServiceBeanDefinitionHolders(scanner, packageToScan, registry, beanNameGenerator);
            for (BeanDefinitionHolder beanDefinitionHolder : beanDefinitionHolders) {
                processScannedBeanDefinition(beanDefinitionHolder);  // 为什么：每个 @DubboService 类再额外生成一个 ServiceBean BD
            }
        }
    }

    // ServiceAnnotationPostProcessor.java:453（骨架节选）
    private AbstractBeanDefinition buildServiceBeanDefinition(Map<String, Object> serviceAnnotationAttributes,
                                                             String serviceInterface, String refServiceBeanName) {
        BeanDefinitionBuilder builder = rootBeanDefinition(ServiceBean.class);
        AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
        beanDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);  // 为什么：让 ModuleModel 走构造器注入
        // ... AnnotationPropertyValuesAdapter 把注解属性灌进 propertyValues
        addPropertyReference(builder, "ref", refServiceBeanName);   // 为什么：ref 指向真正的实现类 Bean，不 new 一个实例
        builder.addPropertyValue("interface", serviceInterface);
        // ... parameters / methods / providerIds / registryIds / protocolIds
        builder.setLazyInit(false);
        return builder.getBeanDefinition();
    }

    // ServiceAnnotationPostProcessor.java:641（骨架节选）
    private void registerServiceBeanDefinition(String serviceBeanName,
                                              AbstractBeanDefinition serviceBeanDefinition,
                                              String serviceInterface) {
        if (registry.containsBeanDefinition(serviceBeanName)) {
            // ... 同名但内容不同 → 抛 BeanDefinitionStoreException（CONFIG_DUPLICATED_BEAN_DEFINITION）
        }
        registry.registerBeanDefinition(serviceBeanName, serviceBeanDefinition);  // 为什么：真正落地到 Spring 容器
    }
}
```

```java
// ServiceBean.java:111  —— Spring 的 InitializingBean 钩子：只"登记"，不导出
@Override
public void afterPropertiesSet() throws Exception {
    if (StringUtils.isEmpty(getPath())) {
        if (StringUtils.isNotEmpty(getInterface())) {
            setPath(getInterface());
        }
    }
    ModuleModel moduleModel = DubboBeanUtils.getModuleModel(applicationContext);
    moduleModel.getConfigManager().addService(this);   // 为什么：交给模块级 ConfigManager 统一托管，Spring 不再持有导出主动权
    moduleModel.getDeployer().setPending();            // 为什么：标记"有待导出服务"，等 deployer 统一 start
}

// DefaultModuleDeployer.java:440
private void exportServices() {
    for (ServiceConfigBase sc : configManager.getServices()) {
        exportServiceInternal(sc);
    }
}

// DefaultModuleDeployer.java:463（骨架节选）
private void exportServiceInternal(ServiceConfigBase sc) {
    ServiceConfig<?> serviceConfig = (ServiceConfig<?>) sc;
    if (!serviceConfig.isRefreshed()) { serviceConfig.refresh(); }
    if (sc.isExported()) { return; }
    if (exportAsync || sc.shouldExportAsync()) {
        // ... CompletableFuture.runAsync(() -> sc.export(), getServiceExportExecutor())
    } else {
        if (!sc.isExported()) {
            sc.export(RegisterTypeEnum.AUTO_REGISTER_BY_DEPLOYER);  // 为什么：由 deployer 统一触发，而非 Spring 自己调
            exportedServices.add(sc);
        }
    }
}
```

**拆解**：

### 5.1 XML 时代：NamespaceHandler 只是把标签翻译成 BeanDefinition

`dubbo-config-spring` 的 `META-INF/spring.handlers` 里写着 `http\://dubbo.apache.org/schema/dubbo=org.apache.dubbo.config.spring.schema.DubboNamespaceHandler`（兼容老命名空间 `code.alibabatech.com` 也指向同一个类），`spring.schemas` 把 XSD 映射到 `META-INF/dubbo.xsd`。Spring 解析 `<dubbo:service>` 时按命名空间找到 `DubboNamespaceHandler`，`init()`（`DubboNamespaceHandler.java:51`）逐个注册解析器：`application`、`module`、`registry`、`protocol`、`provider`、`consumer` 都用 `new DubboBeanDefinitionParser(XxxConfig.class)`，而 `service` / `reference` 分别绑到 `ServiceBean.class` / `ReferenceBean.class`（第 64-65 行），`annotation` 绑到独立的 `AnnotationBeanDefinitionParser`。

注意 `DubboNamespaceHandler.parse()` 不是直接 `super.parse()`：它先 `registerAnnotationConfigProcessors(registry)`（第 97-100 行，`AnnotationConfigUtils.registerAnnotationConfigProcessors`），再调 `DubboSpringInitializer.initialize(parserContext.getRegistry())`（第 83 行），最后才 `super.parse()` 并 `setSource(beanDefinition)`。也就是说，即便你用的是纯 XML，Dubbo 也会顺手把注解驱动的基础设施初始化好——这是 2.7.5 之后两套机制合流的痕迹。

`DubboBeanDefinitionParser.parse()`（`DubboBeanDefinitionParser.java:97`）的产物永远是 `RootBeanDefinition`：`beanDefinition.setBeanClass(beanClass)`、`setLazyInit(false)`（第 99-101 行）。关键分支在第 102-103 行：`ServiceBean.class.equals(beanClass)` 时设 `AUTOWIRE_CONSTRUCTOR`；第 142-152 行，`<dubbo:service class="...">` 会额外造一个 `RootBeanDefinition` 装实现类，再以 `new BeanDefinitionHolder(classDefinition, beanName + "Impl")` 的形式塞进 `ref` 属性。beanName 的规则在第 117-126 行：有 `id` 用 `id`，否则 `beanClass.getName() + "#" + counter` 自增避重；第 127 行还把这个名字 `beanDefinition.setAttribute(BEAN_NAME, beanName)` 存进 BD 元数据，后面 `ServiceBean` 的 `setBeanName()` 才能拿回去。类里那几个常量（第 81-86 行）暴露了 `parseProperties` 的特殊分支：`ONRETURN` / `ONTHROW` / `ONINVOKE` / `EXECUTOR` 要转成 `RuntimeBeanReference`，`METHOD` 要拼成 `ManagedList` / `ManagedMap`，其余按 `beanPropsCache`（第 90 行，按 beanClass 缓存属性类型 Map）反射找 setter；类里还预置了 `GROUP_AND_VERSION`（匹配 `xxx:yyy` 形式）与 `resolvePlaceholdersEnabled` 开关，不过这份文件里该正则只声明、未见后续引用。也就是说 XML 时代没有注解驱动，全靠"标签 → 配置类属性"的硬映射加 `resolveAttribute` 做占位符替换。理解这一点后再看注解时代的 `AnnotationPropertyValuesAdapter`（`ServiceAnnotationPostProcessor.java:476`），会发现它干的是同一件事，只是输入从 `Element` 换成了注解属性 Map。

### 5.2 注解时代：为什么非得是 BeanDefinitionRegistryPostProcessor

`@DubboComponentScan` 的定义（`DubboComponentScan.java:42`）是 `@Import(DubboComponentScanRegistrar.class)`。`DubboComponentScanRegistrar implements ImportBeanDefinitionRegistrar`，`registerBeanDefinitions()`（第 55 行）第一件事同样是 `DubboSpringInitializer.initialize(registry)`，然后 `getPackagesToScan(importingClassMetadata)`：先读 `@DubboComponentScan.basePackages / basePackageClasses`，读不到再退回 `@EnableDubbo.scanBasePackages / scanBasePackageClasses`（第 86-90 行），都没有就用注解所在类的包名（第 93 行）。拿到包集合后，`registerServiceAnnotationPostProcessor()`（第 72 行）用 `rootBeanDefinition(SpringCompatUtils.serviceAnnotationPostProcessor())` 构造一个 BD，把 `packagesToScan` 作为**构造器参数**传进去，`setRole(ROLE_INFRASTRUCTURE)`，再 `BeanDefinitionReaderUtils.registerWithGeneratedName` 注册。

`ServiceAnnotationPostProcessor` 的类声明很清楚（`ServiceAnnotationPostProcessor.java:106-112`）：`implements BeanDefinitionRegistryPostProcessor, EnvironmentAware, ResourceLoaderAware, BeanClassLoaderAware, ApplicationContextAware, InitializingBean`。为什么必须是 `BeanDefinitionRegistryPostProcessor` 而不是普通的 `BeanFactoryPostProcessor`？因为 Spring 的调用顺序是 `postProcessBeanDefinitionRegistry()` 先于 `postProcessBeanFactory()`，只有前者在"所有 BeanDefinition 已就绪、但普通 Bean 尚未实例化"的窗口里允许注册**新的** BeanDefinition；新注册的 BD 还能继续被后续的后处理器和实例化流程处理。它的 `postProcessBeanDefinitionRegistry`（第 169 行）只做一件事：`scanServiceBeans(resolvedPackagesToScan, registry)`；`afterPropertiesSet()`（第 164 行）负责 `resolvePlaceholders` 解析包名的占位符。

扫描用的是 `DubboClassPathBeanDefinitionScanner`（继承 Spring 的 `ClassPathBeanDefinitionScanner`），include filter 是 `serviceAnnotationTypes` 里三种注解的 `AnnotationTypeFilter`（第 224-226 行；`loadServiceAnnotationTypes()` 在第 118-135 行，按 `Dubbo2CompactUtils` 是否启用决定是否兼容老 `@com.alibaba.dubbo.config.annotation.Service`）。`scanner.scan(packageToScan)`（第 245 行）把命中的实现类注册成 Spring Bean，`findServiceBeanDefinitionHolders()`（第 332 行）再 `scanner.findCandidateComponents(packageToScan)` 把它们找回来，逐个 `processScannedBeanDefinition()`（第 359 行）。这里有 doubly-register 的关键：一个 `@DubboService` 类最终在容器里对应**两个** BeanDefinition——实现类 Bean 和 `ServiceBean`；后者 `ref` 用 `addPropertyReference(builder, "ref", annotatedServiceBeanName)`（第 481 行）指回前者。

`buildServiceBeanDefinition()`（第 453 行）里几个容易被跳过的细节：`rootBeanDefinition(ServiceBean.class)`；`AUTOWIRE_CONSTRUCTOR` 是为了让 `ServiceBean` 的 `@Autowired ServiceBean(ModuleModel moduleModel)` 构造器（第 59-63 行）拿到正确的模块；`ignoreAttributeNames`（第 463 行）把 `provider/monitor/application/module/registry/protocol/methods/interfaceName/parameters/executor` 排除在"直接灌属性"之外，因为它们要单独转成 `xxxIds` 或 `MethodConfig`；`builder.setLazyInit(false)`（第 538 行）保证不会因懒加载漏掉导出。bean 名由 `generateServiceBeanName()`（第 412 行）用 `ServiceBeanNameBuilder.create(serviceInterface, environment).group(...).version(...).build()` 生成——`group` / `version` 参与命名，所以同接口不同分组/版本能共存；`registerServiceBeanDefinition()`（第 641 行）遇到同名但内容不同的 BD 会抛 `BeanDefinitionStoreException`，而不是静默覆盖。另外 `postProcessBeanFactory()`（第 175 行）还有一条支线：遍历已有 BD，用 `getServiceAnnotationAttributes()` 识别 `@Bean` 方法上的 `@DubboService`（第 575 行），走 `processAnnotatedBeanDefinition()`（第 616 行），让 Java Config 风格也能用，并额外把 `Constants.ID` 写进属性（第 636 行）。

`resolveBeanNameGenerator()`（第 296 行）也别跳过：它先尝试从 `SingletonBeanRegistry` 里取名为 `CONFIGURATION_BEAN_NAME_GENERATOR`（即 `AnnotationConfigUtils` 那个常量）的 `BeanNameGenerator`，取不到才退化到 `new AnnotationBeanNameGenerator()`，并且打日志提醒"it maybe a potential problem on bean name generation"（第 306-317 行）。源码注释（第 284-294 行）说得很直白：本该复用 `ConfigurationClassPostProcessor#componentScanBeanNameGenerator`，否则 Dubbo 扫描出来的 bean 名可能和 Spring `@ComponentScan` 扫出来的不一致。这正是"实现类 Bean"与 `ServiceBean.ref` 指引能否对上的关键——`buildServiceBeanDefinition` 里 `addPropertyReference(builder, "ref", refServiceBeanName)`（第 481 行）传的是 `beanDefinitionHolder.getBeanName()`（第 370 行），名字一旦算错，运行时就是 `NoSuchBeanDefinitionException`。这也解释了为什么 Dubbo 不自己写扫描器，而是继承 Spring 的 `ClassPathBeanDefinitionScanner`：bean 名生成、条件注解、`@Scope` 这些语义必须和 Spring 保持一致。

还有一层防重复机制容易被忽略：`scanServiceBeans()` 在每个包扫描前都问 `servicePackagesHolder.isPackageScanned(packageToScan)`（第 234 行），扫完 `addScannedPackage` / `addScannedClass`（第 265、280 行）；scanner 上还挂了 `ScanExcludeFilter`（内部类，第 686 行），它的 `match()` 用 `servicePackagesHolder.isClassScanned(className)` 把已注册过的类剔掉。这是为了应对同一个包被 `@DubboComponentScan` 与 Spring 自己的 `@ComponentScan` 重复扫、或者父子容器各扫一遍的情况——否则同一个接口会注册出两份 `ServiceBean`。扫描器类本身也不是写死的：`DubboComponentScanRegistrar` 用 `rootBeanDefinition(SpringCompatUtils.serviceAnnotationPostProcessor())`（第 74 行，`DubboAutoConfiguration` 第 70 行同样），而 `SpringCompatUtils.serviceAnnotationPostProcessor()`（`SpringCompatUtils.java:144`）在 Spring 6 / AOT 条件下会解析到 `org.apache.dubbo.config.spring6.beans.factory.annotation.ServiceAnnotationWithAotPostProcessor`，消费端还有对应的 `ReferenceAnnotationWithAotBeanPostProcessor`（第 138 行）；`AotWithSpringDetector.useGeneratedArtifacts()`（`DubboNamespaceHandler.java:98`、`ServiceAnnotationPostProcessor.java:241`）则控制 AOT 阶段跳过注解配置处理器、改用生成好的元数据。这些分支说明：扫描器不是"扫一次就完事"，它对"重复扫描"和"原生镜像"都做了显式让步。

### 5.3 @DubboReference 的注入时机：两段式，注入的是懒代理

先补一个前面跳过的环节：这个后处理器是谁注册进容器的？答案是 `DubboInfraBeanRegisterPostProcessor`（`dubbo-config-spring/.../context/`，`implements BeanDefinitionRegistryPostProcessor`，第 35 行），它由 `DubboSpringInitializer.initialize()` 一并注册。它的 `postProcessBeanFactory()`（第 50 行）做两件事：`beanFactory.getBean(ReferenceAnnotationBeanPostProcessor.BEAN_NAME, ...)` 后 `beanFactory.addBeanPostProcessor(referenceAnnotationBeanPostProcessor)`（第 58-60 行），注释写明"register ... early before PropertySourcesPlaceholderConfigurer/PropertyPlaceholderConfigurer for processing early init ReferenceBean"——必须抢在占位符配置器之前，否则 `@DubboReference` 里 `${...}` 还没解析就把 `ReferenceBean` 建好了；然后 `registry.removeBeanDefinition(BEAN_NAME)`（第 68 行）把自己从 BD 列表里删掉，避免重复执行。`ReferenceAnnotationBeanPostProcessor.postProcessBeanFactory()` 结尾（第 173-185 行）还有对称处理：发现自己已经被注册成 `BeanPostProcessor` 时，主动 `beanDefinitionRegistry.removeBeanDefinition(BEAN_NAME)`，注释点名是为避免 `BeanPostProcessorChecker` 报"bean 不适资格被所有 BPP 处理"的告警。换句话说，Dubbo 在 Spring 后处理器的**注册顺序**上做了两处手工干预，这不是普通 starter 会遇到的场景。

`ReferenceAnnotationBeanPostProcessor`（第 94 行）`extends AbstractAnnotationBeanPostProcessor implements ApplicationContextAware, BeanFactoryPostProcessor`；父类（`AbstractAnnotationBeanPostProcessor.java:66-68`）`implements InstantiationAwareBeanPostProcessor, MergedBeanDefinitionPostProcessor`。类上的 Javadoc（第 78-88 行）把两段写得明明白白：Step 1 实现 `BeanFactoryPostProcessor` 是为了"更早地注册 reference bean definition，以便与 XML 配置共享"；Step 2 实现 `InstantiationAwareBeanPostProcessor` 才是把实例注入 `@DubboReference` 的字段和 setter。

Step 1：`postProcessBeanFactory()`（第 140 行）遍历 `beanFactory.getBeanDefinitionNames()`，对每个能解析出 `beanType` 的定义调 `findInjectionMetadata(beanName, beanType, null)` → `prepareInjection(metadata)`（第 379 行）。`prepareInjection` 对字段/方法元素调 `registerReferenceBean(...)`（第 414 行），后者最终 `new RootBeanDefinition()`、`setBeanClassName(ReferenceBean.class.getName())`（第 538-539 行），把注解属性放进 `beanDefinition.setAttribute(Constants.REFERENCE_PROPS, attributes)`，再 `beanDefinitionRegistry.registerBeanDefinition(referenceBeanName, beanDefinition)`（第 564 行）。去重靠 `ReferenceBeanSupport.generateReferenceKey(attributes, applicationContext)`（第 441 行）与 `referenceBeanManager.getBeanNamesByKey(referenceKey)`（第 444 行）：命中已注册的 key 就 `registerAlias` 复用（第 528 行），不再新建。第 553-559 行还挂了一个 `GenericBeanDefinition` 作为 `setDecoratedDefinition`，注释说明是为了避免 `AbstractBeanFactory#getTypeForFactoryBean()` 时把 `ReferenceBean` 提前实例化。`postProcessMergedBeanDefinition()`（第 318 行）也会再 `prepareInjection` 一次作为兜底。方法最后 `applicationContext.publishEvent(new DubboConfigInitEvent(...))`（第 190 行）是个 early event。

Step 2：真正的字段赋值发生在 Spring 的 `populateBean` 阶段，`postProcessProperties(PropertyValues pvs, Object bean, String beanName)`（第 360 行）里 `findInjectionMetadata` → `prepareInjection` → `metadata.inject(bean, beanName, pvs)`；`AbstractAnnotationBeanPostProcessor` 的注入元素最终调 `doGetInjectedBean(...)`，这里的实现（第 571 行）只有一句核心：`return getBeanFactory().getBean((String) injectedElement.injectedObject);`——拿到的不是连接好的 Invoker，而是一个 `ReferenceBean`（`FactoryBean<T>`，`ReferenceBean.java:110-115`）产出的代理。`ReferenceBean.getObject()`（第 211 行）在 3.3 里返回懒代理 `DubboReferenceLazyInitTargetSource`（第 471 行），真正的 `referenceConfig.get()` 在 `LazyTargetSource.getTarget()` 里（第 464、467 行）才发生。所以结论是：`@DubboReference` 字段在属性注入阶段被填成一个代理对象，`check=false` 不会因为"注入得早"就报错；而同一个 `@DubboReference` 配置在多个类里出现，容器里也只有一个 `ReferenceBean` BD。

### 5.4 Spring Boot 自动配置与 ModuleModel/ApplicationModel 分层

3.3 的自动配置分两条线：`dubbo-spring-boot/dubbo-spring-boot-compatible/autoconfigure` 的 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 列了 `DubboAutoConfiguration`、`DubboRelaxedBindingAutoConfiguration`、`DubboListenerAutoConfiguration`，`spring.factories` 里用 `EnableAutoConfiguration=` 再写一遍（兼容 Boot 2.x）；而 Boot 3 的 `dubbo-spring-boot-autoconfigure` 的 `AutoConfiguration.imports` 只有 `DubboRelaxedBinding2AutoConfiguration` 和 `DubboTripleAutoConfiguration`。所以"自动配置类"具体在哪个 jar，取决于你引的是哪条 starter 线，别看到源码里有两份就以为是重复。

`DubboAutoConfiguration`（`DubboAutoConfiguration.java:50`）类上是 `@ConditionalOnProperty(prefix = DUBBO_PREFIX, name = "enabled", matchIfMissing = true)`，`DUBBO_PREFIX = "dubbo"`（`DubboUtils.java:52`）——`dubbo.enabled=false` 直接关掉整个配置类；`matchIfMissing = true` 意味着不配也生效。类上还有 `@AutoConfigureAfter(DubboRelaxedBindingAutoConfiguration.class)` 和 `@EnableDubboConfig`（第 52-53 行），后者负责把 `dubbo.application.*` / `dubbo.registry.*` 这些外部配置绑定成 Dubbo 的配置 Bean。唯一的 `@Bean` 方法 `serviceAnnotationBeanProcessor` 上叠了两个条件（第 62-63 行）：`@ConditionalOnProperty(prefix = DUBBO_SCAN_PREFIX, name = BASE_PACKAGES_PROPERTY_NAME)`（`dubbo.scan.base-packages`）和 `@ConditionalOnBean(name = BASE_PACKAGES_BEAN_NAME)`；`DUBBO_SCAN_PREFIX = "dubbo.scan."`（`DubboUtils.java:57-58`），`BASE_PACKAGES_BEAN_NAME = "dubbo-service-class-base-packages"`（第 147 行）。那个 `Set<String>` Bean 由 `DubboRelaxedBindingAutoConfiguration.dubboBasePackages(Environment)`（第 62 行）通过 `propertyResolver.getProperty(BASE_PACKAGES_PROPERTY_NAME, Set.class, emptySet())` 提供。于是逻辑闭环：不配 `dubbo.scan.base-packages` → 没有 base-packages Bean → 不注册 `ServiceAnnotationPostProcessor` → 一个类都不扫；但 `@EnableDubboConfig` 仍在，全局配置照绑。这解释了为什么纯注解项目里漏配这一行，服务"静默不暴露"。

`@EnableDubboConfig` 本身（`EnableDubboConfig.java:69`）是 `@Import(DubboConfigConfigurationRegistrar.class)`，而这个 Registrar 的 `registerBeanDefinitions()`（`DubboConfigConfigurationRegistrar.java:38`）只有一句 `DubboSpringInitializer.initialize(registry)`。真正的属性绑定在 `DubboConfigConfiguration` 里：内部类 `Single` 用一组 `@EnableConfigurationBeanBinding`（第 57-69 行）把 `dubbo.application` → `ApplicationConfig`、`dubbo.module` → `ModuleConfig`、`dubbo.registry` → `RegistryConfig`、`dubbo.protocol` → `ProtocolConfig`、`dubbo.provider` / `dubbo.consumer` / `dubbo.monitor` / `dubbo.config-center` / `dubbo.metadata-report` / `dubbo.metrics` / `dubbo.tracing` / `dubbo.ssl` 逐一映射；内部类 `Multiple`（`public static class Multiple {}`，第 95 行，绑定注解在第 76-93 行）则是 `dubbo.applications` / `dubbo.registries` / `dubbo.protocols` / `dubbo.metricses` 等复数前缀并带 `multiple = true`。`EnableDubboConfig.multiple` 默认 `true`（第 72-77 行）。这条线解释了 Spring Boot 里为什么写 `application.yml` 的 `dubbo.application.name` 就能变成 Dubbo 的 `ApplicationConfig`：它不是 `DubboAutoConfiguration` 里的 `@Bean`，而是 Registrar 注册进来的配置绑定基础设施。于是"自动配置"其实分成两半——配置绑定（一定有）和服务扫描（要看 `dubbo.scan.base-packages`），两者条件完全不同。

为什么要 `ModuleModel` / `ApplicationModel` / `FrameworkModel` 三层？看 `DubboSpringInitializer.initialize()`（第 51 行）：它用 `Map<BeanDefinitionRegistry, DubboSpringInitContext> REGISTRY_CONTEXT_MAP`（第 46 行）按 Spring 容器缓存初始化上下文；`initContext()`（第 103 行）判断若 `ApplicationModel.defaultModel()` 尚未绑定 context 就复用它，否则 `FrameworkModel.defaultModel().newApplication()`（第 121 行），再 `applicationModel.newModule()` 得到 `ModuleModel`（第 125-127 行），最后 `registerSingleton(beanFactory, context.getApplicationModel())` 与 `registerSingleton(beanFactory, context.getModuleModel())`（第 180-183 行）把两者作为单例放进 Spring BeanFactory。这么做的收益是**作用域隔离**：`Filter` 是 `@SPI(scope = ExtensionScope.MODULE)`（`Filter.java:68`），而 `FilterChainBuilder` 是 `@SPI(scope = APPLICATION)`（`FilterChainBuilder.java:43`）——不同模块可以有自己的 Filter 配置、自己的 `ConfigManager`、自己的 `DefaultModuleDeployer`，生命周期可以独立 `destroy()`；应用级的东西（如 `ExecutorRepository`、`FilterChainBuilder`）由 `ApplicationModel` 共享。Spring 侧的直接体现就是 `ServiceBean` 的 `@Autowired ServiceBean(ModuleModel moduleModel)`（第 59-63 行）和 `afterPropertiesSet()` 里的 `moduleModel.getConfigManager().addService(this)`（第 118-119 行）：一个 `ServiceBean` 从出生起就被钉在某个 `ModuleModel` 上。`DefaultModuleDeployer.start()`（第 155 行）里还专门判断 `moduleModel != moduleModel.getApplicationModel().getInternalModule()` 才 `applicationDeployer.prepareInternalModule()`（第 181-183 行）——内部模块是给元数据、配置中心等基础设施用的，不参与业务服务导出。第 6 章会看到，`DefaultFilterChainBuilder.getModuleModelsFromUrl()`（`DefaultFilterChainBuilder.java:138`）正是为了把"应用级服务发现产生的 `InstanceAddressURL`（持有 `ApplicationModel`）"反查回它下面所有 `ModuleModel`，才能取到模块级的 Filter 配置。

最后把链路收尾：`ServiceConfigBase.export()`（`ServiceConfigBase.java:430`）是 `final`，只做 `export(RegisterTypeEnum.AUTO_REGISTER)`；`ServiceConfig.export(RegisterTypeEnum)`（第 310 行）先 `getScopeModel().getDeployer().start()`（第 320 行，注释说"export service and auto start application instance"），再 `refresh()`、`shouldExport()`、`init()`，按 `shouldDelay()` / `MANUAL_REGISTER` 分支落到 `doExport(registerType)`（第 343 行），`doExport` 里 `doExportUrls(registerType)` 后调 `exported()`（第 555-556 行）。所以 `ServiceBean.exported()` 覆写（第 138 行）发出的 `ServiceBeanExportedEvent`，是在 deployer 驱动 `export` 之后才发布的——这就是"给 `ServiceBean` 加监听器监听导出完成却对不上时序"的根因。

**容易误解的地方**：

1. **`@DubboService` 不是 Spring 的组件注解**。`scanner.scan(packageToScan)`（`ServiceAnnotationPostProcessor.java:245`）先把实现类注册成一个 Spring Bean，`processScannedBeanDefinition()` 再为它注册第二个 `ServiceBean` BeanDefinition，两者靠 `ref` 属性关联（`buildServiceBeanDefinition` 第 481 行）。以为"一个类 = 一个 Bean"是后面所有困惑的起点：你在容器里按实现类类型 `getBean()` 拿到的是业务对象，按 `ServiceBean` 类型拿到的才是导出配置。
2. **`ServiceBean.afterPropertiesSet()` 不导出服务**。它只 `moduleModel.getConfigManager().addService(this)` + `moduleModel.getDeployer().setPending()`（第 118-120 行）。真正的 `export()` 由 `DefaultModuleDeployer.start()` → `exportServices()` → `exportServiceInternal()` 统一触发，还可能走 `CompletableFuture.runAsync` 异步分支（第 473-492 行）。想靠 `InitializingBean` 或 `@PostConstruct` 判断"端口已经开了"必然踩空。
3. **`@DubboReference` 注入的不是活连接，是懒代理**。`doGetInjectedBean()`（第 571 行）只是 `getBeanFactory().getBean(referenceBeanName)`，得到 `ReferenceBean.getObject()` 的代理；`referenceConfig.get()` 在 `DubboReferenceLazyInitTargetSource.getTarget()`（第 471-473 行）首次调用时才执行。而且 `ReferenceBean` 的 BD 早在 `BeanFactoryPostProcessor` 阶段就注册了（第 140 行），比字段注入早一个阶段；`postProcessProperties`（第 360 行）才做 `metadata.inject`。把两段当成一个阶段，容易误判 `check=false`、循环依赖和 `@Bean` 方法上 `@DubboReference` 的行为。
4. **`dubbo.enabled=false` 与"没配 `dubbo.scan.base-packages`"是两件事**，而且 3.3 里 `DubboAutoConfiguration` 在 `dubbo-spring-boot-compatible/autoconfigure`，不在主 `dubbo-spring-boot-autoconfigure` 模块。前者靠类级 `@ConditionalOnProperty`（第 50 行）整体关闭；后者靠方法级 `@ConditionalOnProperty(prefix="dubbo.scan.", name="base-packages")` + `@ConditionalOnBean`（第 62-63 行）只不注册扫描器，`@EnableDubboConfig` 依旧生效。另外别把 `ModuleModel` 当全局单例：`DubboSpringInitializer` 是按 `BeanDefinitionRegistry` 缓存 context 的（`REGISTRY_CONTEXT_MAP`，第 46 行），父子容器、多 Spring 上下文完全可能绑定到不同的 `ApplicationModel`。

**记住这一句**：扫描器负责"把注解变成 BeanDefinition"，Deployer 负责"把 BeanDefinition 变成网络端口"，中间靠 ModuleModel 的 ConfigManager 对接。
