# The Node.js Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/01quickstart/)).

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

## Current Progress

### Message Type

- [x] NORMAL
- [x] FIFO
- [x] DELAY
- [x] TRANSACTION

### Client Type

- [x] PRODUCER
- [x] SIMPLE_CONSUMER
- [ ] PULL_CONSUMER
- [ ] PUSH_CONSUMER
