# Code Example

## Provider

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

## Producer

There are examples for 4 types of message.

### Publish NORMAL Message

```java
// Build your message.
final Message message = provider.newMessageBuilder()
    .setTopic(normalTopic)
    .setBody(body)
    .setTag(tag)
    .build();
// Build your producer.
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(topic)
    .build();
final SendReceipt sendReceipt = producer.send(message);
// Close it when you don't need the producer anymore.
producer.close();
```

### Publish FIFO Message

```java
// Build your message.
final Message message = provider.newMessageBuilder()
    .setTopic(fifoTopic)
    .setBody(body)
    // Set the message group for FIFO message.
    .setMessageGroup(messageGroup)
    .setTag(tag)
    .build();
// Build your producer.
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(topic)
    .build();
final SendReceipt sendReceipt = producer.send(message);
// Close it when you don't need the producer anymore.
producer.close();
```

### Publish DELAY Message

```java
// Build your message.
final Message message = provider.newMessageBuilder()
    .setTopic(delayTopic)
    .setBody(body)
    // Set the delivery timestamp to 3 seconds later (Time unit: milliseconds).
    .setDeliveryTimestamp(System.currentTimeMillis() + 3 * 1000)
    .setTag(tag)
    .build();
// Build your producer.
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(topic)
    .build();
final SendReceipt sendReceipt = producer.send(message);
// Close it when you don't need the producer anymore.
producer.close();
```

### Publish TRANSACTIONAL Message

```java
// Build your message.
final Message message = provider.newMessageBuilder()
    .setTopic(transactionalTopic)
    .setBody(body)
    .setTag(tag)
    .build();
// Build your producer.
Producer producer = provider.newProducerBuilder()
    .setClientConfiguration(clientConfiguration)
    .setTopics(transactionalTopic)
    // Set transactional checker.
    .setTransactionChecker(messageView -> TransactionResolution.COMMIT)
    .build();
Transaction transaction = producer.beginTransaction();
SendReceipt receipt = producer.send(message, transaction);
transaction.commit();
// Close it when you don't need the producer anymore.
producer.close();
```

## PushConsumer

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
// Close it when you don't need the consumer anymore.
pushConsumer.close();
```

## SimpleConsumer

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
// Close it when you don't need the consumer anymore.
simpleConsumer.close();
```
