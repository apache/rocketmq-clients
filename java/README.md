# The Java Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the java implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/02quickstart/https://rocketmq.apache.org/docs/quickStart/02quickstart/)).

1. Java 8+ for runtime, Java 11+ for the build;
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

Dependencies must be included in accordance with your build automation tools, and replace the `${rocketmq.version}` with the [latest version](https://search.maven.org/search?q=g:org.apache.rocketmq%20AND%20a:rocketmq-client-java).

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

The `rocketmq-client-java` is a shaded jar, which means its dependencies can not be manually changed. While we still offer the no-shaded jar for exceptional situations, use the shaded one if you are unsure which version to use. In most cases, this is a good technique for dealing with library dependencies that clash with one another.

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

More code examples are provided [here](./client/src/main/java/org/apache/rocketmq/client/java/example) to assist you in working with various clients and different message types.

## Logging System

We picked [Logback](https://logback.qos.ch/) and shaded it into the client implementation to guarantee that logging is reliably persistent. Because RocketMQ utilizes a distinct configuration file, you shouldn't be concerned that the Logback configuration file will clash with yours.

The following logging parameters are all supported for specification by JVM system parameters (for example, `java -Drocketmq.log.level=INFO -jar foobar.jar`) or environment variables.

* `rocketmq.log.level`: log output level, default is INFO.
* `rocketmq.log.root`: the root directory of the log output, default is `$HOME/logs/rocketmq`, so the full path is `$HOME/logs/rocketmq/rocketmq-client.log`.
* `rocketmq.log.file.maxIndex`: the maximum number of log files to keep, default is 10 (the size of a single log file is limited to 64 MB, no adjustment is supported now).

Specifically, by setting `mq.consoleAppender.enabled` to 'true,' you can output client logs to the console simultaneously if you need debugging.
