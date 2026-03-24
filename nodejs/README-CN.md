# Apache RocketMQ Node.js 客户端

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

在开始客户端的部分之前，所需的一些前期工作（或者参照[这里](https://rocketmq.apache.org/zh/docs/quickStart/01quickstart/)）：

1. 准备 [Node.js](https://nodejs.dev/zh-cn/download/) 环境。Node.js 16.19.0 是确保客户端运行的最小版本，Node.js >= 18.17.0 是推荐版本；
2. 部署 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

我们使用 npm 作为依赖管理和发布的工具。你可以在 npm 的[官方网站](https://npmjs.com/)了解到关于它的更多信息。
这里是一些在开发阶段你会使用到的 npm 命令：

```shell
# 自动安装工程相关的依赖
npm install
# 初始化 grpc 代码
npm run init
# 运行单元测试
npm test
# 安装rocketmq nodejs 客户端
npm i rocketmq-client-nodejs
```

开启 grpc-js 的调试日志：

```bash
GRPC_TRACE=compression GRPC_VERBOSITY=debug GRPC_TRACE=all npm test
```

## 发布步骤

执行以下命令：

```shell
# 构建包并将包发布到远程 npm 仓库
npm publish
```

## 示例

### 普通消息

发送消息

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

### 延时消息与回查

发送并召回延时消息：

```ts
import { Producer } from 'rocketmq-client-nodejs';

const producer = new Producer({
  endpoints: '127.0.0.1:8081',
});
await producer.startup();

// 发送一条延时消息（10 秒后投递）
const receipt = await producer.send({
  topic: 'DelayTopic',
  tag: 'delay-recall',
  delay: 10000, // 10 秒延时
  body: Buffer.from('这是一条延时消息'),
});

console.log('消息已发送:', {
  messageId: receipt.messageId,
  recallHandle: receipt.recallHandle, // 用于召回消息的句柄
});

// 在消息投递前召回（10 秒内）
try {
  const recallReceipt = await producer.recallMessage(
    'DelayTopic',
    receipt.recallHandle
  );
  console.log('消息召回成功:', recallReceipt.messageId);
} catch (error) {
  console.error('召回失败:', error);
}

await producer.shutdown();
```

消费消息

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

PushConsumer 主动从服务器拉取消息并推送给监听器处理：

```ts
import { PushConsumer, ConsumeResult, type MessageView } from 'rocketmq-client-nodejs';

// 创建 PushConsumer 实例
const pushConsumer = new PushConsumer({
  namespace: '', // 命名空间，可以为空字符串
  endpoints: '127.0.0.1:8081',
  consumerGroup: 'yourConsumerGroup',
  
  // 订阅主题和 TAG
  subscriptions: new Map([
    ['yourTopic', '*'],  // 订阅 yourTopic，接收所有 TAG
  ]),
  
  // 消息监听器 - 核心处理逻辑
  messageListener: {
    async consume(messageView: MessageView): Promise<ConsumeResult> {
      console.log('收到消息:', messageView.body.toString('utf-8'));
      
      // TODO: 在这里处理你的业务逻辑
      
      return ConsumeResult.SUCCESS; // 处理成功返回 SUCCESS
    },
  },
});

try {
  // 启动消费者
  await pushConsumer.startup();
  console.log('PushConsumer 已启动，等待消息...');
  
  // 保持运行，等待消息
  await new Promise(() => {});
} catch (error) {
  console.error('错误:', error);
  await pushConsumer.shutdown();
  throw error;
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
- [x] PUSH_CONSUMER
- [ ] PULL_CONSUMER
