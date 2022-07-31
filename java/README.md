# The Java Implementation

Here is the java implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).

## Prerequisites

| Stage   | Requirements |
| ------- | ------------ |
| Build   | JDK 11+      |
| Runtime | JRE 8+       |

## Getting Started

Firstly, add the dependency to your `pom.xml`, and replace the `${rocketmq.version}` with the latest version.

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

Note: `rocketmq-client-java` is a shaded jar, which means you could not substitute its dependencies.
From the perspective of avoiding dependency conflicts, you may need a shaded client in most cases, but we also provided
the no-shaded client.

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java-noshade</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

You can see more code examples [here](./client/src/main/java/org/apache/rocketmq/client/java/example).

## Logging System

We use [logback](https://logback.qos.ch/) as our logging system and redirect the log of gRPC to [SLF4j](https://www.slf4j.org/) as well.

To prevent the conflict of configuration file while both of rocketmq client and standard logback is introduced in the same project, we shaded a new logback using `rocketmq.logback.xml/rocketmq.logback-test.xml/rocketmq.logback.groovy` instead of `logback.xml/logback-test.xml/logback.groovy` as its configuration file in the shaded jar.

You can adjust the log level by the environment parameter or the java system property - `rocketmq.log.level`. See [here](https://logback.qos.ch/manual/architecture.html#effectiveLevel) for more details about logback log level.
