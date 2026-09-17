# The Node.js Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here are some preparations you may need to know (or refer
to [quick start](https://rocketmq.apache.org/docs/quickStart/01quickstart/)).

1. [Node.js](https://nodejs.dev/en/download/) 16.19.0 is the minimum version required, Node.js >= 18.17.0 is the recommended version.
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

We are using npm as the dependency management & publishing tool. You can find out more details about npm from its [website](https://npmjs.com/). Here is the related command of npm you may use for development.

```shell
# Installs the project dependencies.
npm install
# Init grpc codes.
npm run init
# Run the unit tests.
npm test
# Installs rocketmq nodejs client
npm i rocketmq-client-nodejs
```

Enable trace debug log for grpc-js:

```bash
GRPC_TRACE=compression GRPC_VERBOSITY=debug GRPC_TRACE=all npm test
```

## Publishing Steps

To publish a package to npm, please register an account in advance, then execute the following command.

```shell
# Builds a package and publishes it to the npm repository.
npm publish
```

## Examples

### Normal Message

Producer

```ts
import { Producer } from 'rocketmq-client-nodejs';

const producer = new Producer({
  endpoints: '127.0.0.1:8081',
});
await producer.startup();

const receipt = await producer.send({
  topic: 'TopicTest',
  tag: 'nodejs-demo',
  body: Buffer.from(JSON.stringify({
    hello: 'rocketmq-client-nodejs world 😄',
    now: Date(),
  })),
});
console.log(receipt);
```

### Delay Message with Recall

Send and recall a delayed message:

```ts
import { Producer } from 'rocketmq-client-nodejs';

const producer = new Producer({
  endpoints: '127.0.0.1:8081',
});
await producer.startup();

// Send a delay message (will be delivered after 10 seconds)
const receipt = await producer.send({
  topic: 'DelayTopic',
  tag: 'delay-recall',
  delay: 10000, // 10 seconds delay
  body: Buffer.from('This is a delayed message'),
});

console.log('Message sent:', {
  messageId: receipt.messageId,
  recallHandle: receipt.recallHandle, // Handle for recalling the message
});

// Recall the message before it's delivered (within 10 seconds)
try {
  const recallReceipt = await producer.recallMessage(
    'DelayTopic',
    receipt.recallHandle
  );
  console.log('Message recalled successfully:', recallReceipt.messageId);
} catch (error) {
  console.error('Failed to recall message:', error);
}

await producer.shutdown();
```

SimpleConsumer

```ts
import { SimpleConsumer } from 'rocketmq-client-nodejs';

const simpleConsumer = new SimpleConsumer({
  consumerGroup: 'nodejs-demo-group',
  endpoints: '127.0.0.1:8081',
  subscriptions: new Map().set('TopicTest', 'nodejs-demo'),
});
await simpleConsumer.startup();

const messages = await simpleConsumer.receive(20);
console.log('got %d messages', messages.length);
for (const message of messages) {
  console.log(message);
  console.log('body=%o', message.body.toString());
  await simpleConsumer.ack(message);
}
```

### Push Consumer

PushConsumer actively pulls messages from the server and pushes them to the listener for processing:

```ts
import { PushConsumer, ConsumeResult, type MessageView } from 'rocketmq-client-nodejs';

// Create PushConsumer instance
const pushConsumer = new PushConsumer({
  namespace: '', // Namespace, can be empty string
  endpoints: '127.0.0.1:8081',
  consumerGroup: 'yourConsumerGroup',
  
  // Subscribe to topic and TAG
  subscriptions: new Map([
    ['yourTopic', '*'],  // Subscribe to yourTopic, receive all TAGs
  ]),
  
  // Message listener - core processing logic
  messageListener: {
    async consume(messageView: MessageView): Promise<ConsumeResult> {
      console.log('Received message:', messageView.body.toString('utf-8'));
      
      // TODO: Process your business logic here
      
      return ConsumeResult.SUCCESS; // Return SUCCESS after successful processing
    },
  },
});

try {
  // Start consumer
  await pushConsumer.startup();
  console.log('PushConsumer started, waiting for messages...');
  
  // Keep running, waiting for messages
  await new Promise(() => {});
} catch (error) {
  console.error('Error:', error);
  await pushConsumer.shutdown();
  throw error;
}
```

### Lite Topic

Lite topics are light-weight subscriptions on top of one parent topic. A lite consumer binds to the
parent topic and (un)subscribes lite topics at runtime, which makes it cheap to serve a large number
of short-lived topics.

Server prerequisites: broker with `enableLmq=true` and `enableMultiDispatch=true`, a parent topic
created with `message.type=LITE`, and a consumer group created with the attribute
`+lite.bind.topic=<parentTopic>`.

Send with lite topic

```ts
import { Producer } from 'rocketmq-client-nodejs';

const producer = new Producer({
  endpoints: '127.0.0.1:8081',
});
await producer.startup();

await producer.send({
  topic: 'yourParentTopic',
  liteTopic: 'lite-topic-1', // The lite topic the message belongs to
  body: Buffer.from('This is a lite message'),
});

await producer.shutdown();
```

LiteSimpleConsumer (pull and ack explicitly)

```ts
import { LiteSimpleConsumerBuilder, OffsetOption } from 'rocketmq-client-nodejs';

// Bind to the parent topic, the consumer group must be bound to it as well
const consumer = await new LiteSimpleConsumerBuilder()
  .setClientConfiguration({ endpoints: '127.0.0.1:8081' })
  .setConsumerGroup('yourConsumerGroup')
  .bindTopic('yourParentTopic')
  .setAwaitDuration(5000)
  .build();

// Subscribe lite topics, with or without a consume-from offset
await consumer.subscribeLite('lite-topic-1', OffsetOption.MIN_OFFSET);
await consumer.subscribeLite('lite-topic-2');

// Pull a batch and ack every message
const messages = await consumer.receive(16, 15000);
for (const message of messages) {
  console.log(message.liteTopic, message.body.toString());
  await consumer.ack(message);
}

// Release the subscription when it's no longer needed
await consumer.unsubscribeLite('lite-topic-2');
await consumer.close();
```

A runnable version lives in `examples/LiteSimpleConsumerExample.ts` (see also
`examples/LiteProducerExample.ts` and `examples/LitePushConsumerExample.ts`).

## Current Progress

### Message Type

- [x] NORMAL
- [x] FIFO
- [x] DELAY
- [x] TRANSACTION

### Client Type

- [x] PRODUCER
- [x] SIMPLE_CONSUMER
- [x] PUSH_CONSUMER
- [ ] PULL_CONSUMER
