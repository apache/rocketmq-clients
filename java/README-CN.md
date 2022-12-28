# Java 客户端

[English](README.md) | 简体中文

不同于[基于 RemotingCommand 协议的客户端](https://github.com/apache/rocketmq/tree/develop/client)，当前的客户端是基于 RocektMQ 5.0 存算分离架构，是 RocketMQ 社区目前推荐的接入方式，也是未来客户端演进的主方向。

在开始客户端的部分之前，可能需要的一些前期准备工作：

1. Java 8 是确保客户端运行的最小版本，Java 11 是确保客户端编译的最小版本；
2. 提前部署好 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

根据构建系统增加对应的客户端依赖，并将 `${rocketmq.version}` 替换成中央仓库中[最新的版本](https://search.maven.org/search?q=g:org.apache.rocketmq%20AND%20a:rocketmq-client-java)。

```xml
<!-- For Apache Maven -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java</artifactId>
    <version>${rocketmq.version}</version>
</dependency>

<!-- Groovy Kotlin DSL for Gradle -->
implementation("org.apache.rocketmq:rocketmq-client-java:${rocketmq.version}")

<!-- Groovy DSL for Gradle -->
implementation 'org.apache.rocketmq:rocketmq-client-java:${rocketmq.version}'
```

`rocketmq-client-java` 是一个经过 shade 之后的版本，这意味着它本身的依赖是不能被手动替换的。通常情况下，shade 是规避多个类库之间依赖冲突的有效方式。对于一些特殊场景，我们仍然提供了未经 shade 的版本。

```xml
<!-- For Apache Maven -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java-noshade</artifactId>
    <version>${rocketmq.version}</version>
</dependency>

<!-- Groovy Kotlin DSL for Gradle -->
implementation("org.apache.rocketmq:rocketmq-client-java-noshade:${rocketmq.version}")

<!-- Groovy DSL for Gradle -->
implementation 'org.apache.rocketmq:rocketmq-client-java-noshade:${rocketmq.version}'
```

如果不确定是否应该使用哪个版本，那么请使用 shade 之后的版本，即 `rocketmq-client-java`。

我们在这里提供了[代码示例](./client/src/main/java/org/apache/rocketmq/client/java/example)来帮助使用不同的客户端和不同的消息类型。

## 日志

我们使用 [Logback](https://logback.qos.ch/) 来作为日志实现，为了保证日志能够始终被有效地持久化，我们将 Logback 集成到了内部。`rocketmq-client-java` 中使用了一个 shade 之后的 Logback，并会使用单独的配置文件。

在满足日志被有效持久化的前提下，以下是一些可以灵活配置的日志参数，所有参数均支持 JVM 系统参数（e.g. `java -Drocketmq.log.level=INFO -jar foobar.jar`）或环境变量进行指定。

* `rocketmq.log.level`: 日志输出级别，默认为 INFO。
* `rocketmq.log.root`: 日志输出根目录，默认为当前用户的 HOME 目录。
* `rocketmq.log.file.maxIndex`: 日志文件最大保留个数，默认为 10（单个日志文件大小限制为 64 MB，暂不支持调整）。

特别地，如果有调试的需求，可以通过把 `mq.consoleAppender.enabled` 设置成 `true` 将客户端的日志同时输出到控制台。
