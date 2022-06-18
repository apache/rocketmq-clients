# RocketMQ Clients for Java

[![Java](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml)

The java implementation of client for [Apache RocketMQ](https://rocketmq.apache.org/).

## Prerequisites

Java 11 or higher is required to build this project. The built artifacts can be used on Java 8 or
higher.

<table>
  <tr>
    <td><b>Build required:</b></td>
    <td><b>Java 11 or later</b></td>
  </tr>
  <tr>
    <td><b>Runtime required:</b></td>
    <td><b>Java 8 or later</b></td>
  </tr>
</table>

## Getting Started

Add dependency to your `pom.xml`, and replace the `${rocketmq.version}` by the latest version.

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```

It is worth noting that `rocketmq-client-java` is a shaded jar, which means you could not substitute its dependencies.
From the perspective of avoiding dependency conflicts, you may need a shaded client in most case, but we also provided
the no-shaded client.

```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client-java-noshade</artifactId>
    <version>${rocketmq.version}</version>
</dependency>
```


