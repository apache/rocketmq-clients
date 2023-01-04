# Java 客户端

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

不同于[基于 RemotingCommand 协议的版本](https://github.com/apache/rocketmq/tree/develop/client)，当前的客户端基于 RocektMQ 5.0 存算分离架构进行设计开发，是 RocketMQ 社区目前推荐的接入方式，也是未来客户端演进的主要方向。

在开始客户端的部分之前，所需的一些前期工作（或者参照[这里](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart/)）：

1. 准备 Java 环境。Java 8 是确保客户端运行的最小版本，Java 11 是确保客户端编译的最小版本；
2. 部署 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

根据构建系统增加对应的客户端依赖，并将 `${rocketmq.version}` 替换成中央仓库中[最新的版本](https://search.maven.org/search?q=g:org.apache.rocketmq%20AND%20a:rocketmq-client-java)。

```xml
<!-- For Apache Maven -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

```kotlin
// Kotlin DSL for Gradle
implementation("org.apache.rocketmq:rocketmq-client-java:${rocketmq.version}")
```

```groovy
// Groovy DSL for Gradle
implementation 'org.apache.rocketmq:rocketmq-client-java:${rocketmq.version}'
```

`rocketmq-client-java` 是一个经过 shade 的版本，这意味着它本身的依赖是不能被手动替换的。

通常情况下，shade 是规避多个类库之间依赖冲突的有效方式。由于一些特殊场景的存在，客户端仍然提供未经 shade 的版本。如果不确定具体应该使用哪个版本，shade 之后的版本是首选。

```xml
<!-- For Apache Maven -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java-noshade</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

```kotlin
// Kotlin DSL for Gradle
implementation("org.apache.rocketmq:rocketmq-client-java-noshade:${rocketmq.version}")
```

```groovy
// Groovy DSL for Gradle
implementation 'org.apache.rocketmq:rocketmq-client-java-noshade:${rocketmq.version}'
```

内部的[代码示例](./client/src/main/java/org/apache/rocketmq/client/java/example)可以帮助你上手不同的客户端和消息类型。

## 日志系统

采用 [Logback](https://logback.qos.ch/) 来作为实现，为了保证日志能够始终被有效地持久化，`rocketmq-client-java` 中集成了一个 shade 之后的 Logback，并会为其使用单独的配置文件。

客户端提供了一些可配置的日志参数，均支持 JVM 系统参数（示例： `java -Drocketmq.log.level=INFO -jar foobar.jar`）或环境变量指定：

* `rocketmq.log.level`: 日志输出级别，默认为 INFO。
* `rocketmq.log.root`: 日志输出根目录，默认为 `$HOME/logs/rocketmq`，此时日志输出路径为 `$HOME/logs/rocketmq/rocketmq-client.log`。注意：文件名 `rocketmq-client.log` 不可改。
* `rocketmq.log.file.maxIndex`: 日志文件最大保留个数，默认为 10（单个日志文件大小限制为 64 MB，暂不支持调整）。

特别地，如果有调试的需求，可以通过把 `mq.consoleAppender.enabled` 设置成 `true` 将客户端的日志同时输出到控制台。
