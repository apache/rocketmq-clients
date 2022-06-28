# RocketMQ Clients for Java

[![Java](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml)

The java client implementation of [Apache RocketMQ](https://rocketmq.apache.org/).

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

There is a provider based on the Java SPI mechanism, the provider here can derive specific implementations.

```java
// Find the implementation of APIs according to SPI mechanism.
final ClientServiceProvider provider = ClientServiceProvider.loadService();
StaticSessionCredentialsProvider staticSessionCredentialsProvider =
    new StaticSessionCredentialsProvider(accessKey, secretKey);
ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
    .setEndpoints(endpoints)
    .setCredentialProvider(staticSessionCredentialsProvider)
    .build();
```

### Producer

```java
// Build your message.
final Message message = provider.newMessageBuilder()
    .setTopic(topic)
    .setBody(body)
    .setTag(tag)
    .build();
// Build your producer.
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(topic)
    .build();
for (int i = 0; i < 1024; i++) {
    final SendReceipt sendReceipt = producer.send(message);
}
// Close it when you don't need the producer any more.
producer.close();
```

### PushConsumer

```java
// Build your push consumer.
PushConsumer pushConsumer = provider.newPushConsumerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setConsumerGroup(consumerGroup)
    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
    .setMessageListener(messageView -> {
    // Handle the received message and return the consume result.
    return ConsumeResult.SUCCESS;
    })
    .build();
// Close it when you don't need the consumer any more.
pushConsumer.close();
```

### SimpleConsumer

```java
// Build your simple consumer.
SimpleConsumer simpleConsumer = provider.newSimpleConsumerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setConsumerGroup(consumerGroup)
    .setAwaitDuration(awaitDuration)
    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
    .build();
// Try to receive message from server.
final List<MessageView> messageViews = simpleConsumer.receive(1, invisibleDuration);
for (MessageView messageView : messageViews) {
    // Ack or change invisible time according to your needs.
    simpleConsumer.ack(messageView);
}
// Close it when you don't need the consumer any more.
simpleConsumer.close();
```
