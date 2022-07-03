# The Java Implementation

[![Java](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml)

Here is the java implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).

## Prerequisites

This project guarantees the same runtime compatibility with [grpc-java](https://github.com/grpc/grpc-java).

* Java 11 or higher is required to build this project.
* The built artifacts can be used on Java 8 or higher.

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

You can see more code examples [here](./example.md).