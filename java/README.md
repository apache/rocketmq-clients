# The Java Implementation of Apache RocketMQ Client

[![Codecov-java][codecov-java-image]][codecov-url] [![Maven Central][maven-image]][maven-url]

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the java implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer
to [quick start](https://rocketmq.apache.org/docs/quickStart/02quickstart/)).

1. Java 8+ for runtime, Java 11+ for the build;
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Build from Source

The `rocketmq-proto` module is compiled from the `protos/` submodule ([rocketmq-apis](https://github.com/apache/rocketmq-apis)) rather than downloaded from Maven Central. Before building, initialize the submodule:

```bash
git submodule update --init protos
```

Then build with Maven:

```bash
cd java
mvn -B package -DskipTests
```

The build order is: `proto` (from submodule) → `client-apis` → `client` → `client-shade` → `test`.

The proto version is defined in `protos/java/VERSION` and must match the `<version>` in `java/proto/pom.xml`. When updating proto, advance the submodule and update both files accordingly.

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

More code examples are provided [example](./client/src/main/java/org/apache/rocketmq/client/java/example) to assist you
in working with various clients and different message types.

## Asynchronous Push Consumption

Use `setAsyncMessageListener` when processing finishes outside the listener invocation. Return a
`CompletionStage<ConsumeResult>` which represents the actual processing outcome:

```java
PushConsumer consumer = provider.newPushConsumerBuilder()
    .setClientConfiguration(configuration)
    .setConsumerGroup(consumerGroup)
    .setSubscriptionExpressions(subscriptions)
    .setConsumptionThreadCount(20)
    .setAsyncMessageListener(message ->
        CompletableFuture.supplyAsync(() -> process(message), applicationExecutor))
    .build();
```

The last synchronous or asynchronous listener setter takes precedence. An outstanding processing stage retains a
consumption concurrency slot, but does not occupy a client worker thread. SUCCESS starts acknowledgement only after
processing finishes; exceptions, cancellation and null stages/results trigger the existing failure path. Completing the
stage reports processing completion, not receipt of a server acknowledgement. Consumption hooks and metrics also follow
the eventual processing result. FIFO messages retain the existing ordering through processing and acknowledgement.

Push receive requests retain the existing server-side auto-renew protocol and its server-configured duration limits.
No additional client-side renewal loop is introduced. Messages continue to count towards the local cache limits while
processing or confirmation is pending, and applications must still handle duplicate deliveries idempotently.

`consumer.close()` drains processing stages and their terminal operations before shutting down client executors. Returned
stages must eventually finish; keep the application executor available until the consumer closes. See
[AsyncPushConsumerExample](client/src/main/java/org/apache/rocketmq/client/java/example/AsyncPushConsumerExample.java).

Local cache limits and consumption concurrency can be updated on the same instance:

```java
consumer.updateRuntimeTuning(4096, 64 * 1024 * 1024, 40);
```

All three values must be positive. A lower cache limit governs subsequent receiving without discarding cached messages.
A lower concurrency limit allows existing tasks to finish and postpones new work until the active count is below that
limit. The same method supports synchronous, asynchronous and virtual-thread consumption.

## Logging System

We picked [Logback](https://logback.qos.ch/) and shaded it into the client implementation to guarantee that logging is reliably persistent. Because RocketMQ utilizes a distinct configuration file, you shouldn't be concerned that the Logback configuration file will clash with yours.

The following logging parameters are all supported for specification by JVM system parameters (for example, `java -Drocketmq.log.level=INFO -jar foobar.jar`) or environment variables.

* `rocketmq.log.level`: the log output level, default is INFO.
* `rocketmq.log.root`: the root directory of the log output, default is `$HOME/logs/rocketmq`, so the full path is `$HOME/logs/rocketmq/rocketmq-client.log`.
* `rocketmq.log.file.maxIndex`: the maximum number of log files to keep, default is 10 (the size of a single log file is limited to 64 MB, no adjustment is supported now).

Specifically, by setting `mq.consoleAppender.enabled` to true, you can output client logs to the console simultaneously if you need debugging.

[codecov-java-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=java&label=Java%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
[maven-image]: https://img.shields.io/maven-central/v/org.apache.rocketmq/rocketmq-client-java
[maven-url]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-client-java
