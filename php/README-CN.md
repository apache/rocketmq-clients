# Apache RocketMQ PHP 客户端

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D7.4-blue)](https://www.php.net/)
[![RocketMQ Version](https://img.shields.io/badge/RocketMQ-5.x-orange)](https://rocketmq.apache.org/)

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

不同于传统的客户端版本，当前的 PHP 客户端基于 RocketMQ 5.x 存算分离架构进行设计开发，是 RocketMQ 社区目前推荐的接入方式。

在开始使用客户端之前，所需的一些前期准备（或者参照[快速开始](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart/)）：

1. 准备 PHP 环境。PHP 7.4 是确保客户端运行的最小版本；
2. 部署 namesrv、broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

### 安装依赖

使用 Composer 安装依赖：

```bash
composer install
```

### 生成 gRPC 代码

在使用客户端之前，需要从 proto 文件生成 gRPC 代码：

```bash
cd php
bash scripts/grpc_tool.sh generate
```

详细说明请参见下方的[生成 gRPC 代码](#生成-grpc-代码)章节。

### 快速示例

#### 生产者

```php
use Apache\Rocketmq\ClientServiceProvider;
use Apache\Rocketmq\ClientConfiguration;

$provider = ClientServiceProvider::getInstance();
$config = new ClientConfiguration('your-endpoints:8080');

$producer = $provider->newProducerBuilder()
    ->setClientConfiguration($config)
    ->setTopics('your-topic')
    ->build();

$result = $producer->sendNormalMessage("Hello RocketMQ", "TagA", "key1");
echo "消息发送成功: " . $result->getMessageId() . "\n";

$producer->close();
```

#### 推送消费者

```php
use Apache\Rocketmq\ClientServiceProvider;
use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Consumer\ConsumeResult;

$provider = ClientServiceProvider::getInstance();
$config = new ClientConfiguration('your-endpoints:8080');

$consumer = $provider->newPushConsumerBuilder()
    ->setClientConfiguration($config)
    ->setConsumerGroup('GID-test-group')
    ->setTopic('your-topic')
    ->setMessageListener(function($message) {
        echo "收到消息: " . $message->getBody() . "\n";
        return ConsumeResult::SUCCESS;
    })
    ->build();

$consumer->start();
```

更多示例请参考 [examples](./examples) 目录，涵盖了不同的消息类型：
- 普通消息
- FIFO 顺序消息
- 延时/定时消息
- 事务消息
- 优先级消息
- Lite Topic 消息

## 日志系统

PHP 客户端使用自定义的 Logger 类，采用 SLF4J 风格的格式化（与 Java 的日志格式兼容）。

日志输出路径：`$HOME/logs/rocketmq/rocketmq-client.log`

您可以通过设置环境变量来配置日志级别：

```bash
export ROCKETMQ_LOG_LEVEL=DEBUG  # 可选值: DEBUG, INFO, WARN, ERROR
```

## 功能特性

### 核心功能

- ✅ 普通消息发送和消费
- ✅ FIFO 顺序消息（支持消费加速器）
- ✅ 延时/定时消息
- ✅ 事务消息
- ✅ 优先级消息
- ✅ Lite Topic 消息
- ✅ 消息标签和键
- ✅ SimpleConsumer 模式（主动拉取）
- ✅ PushConsumer 模式（监听推送）
- ✅ 异步消息发送
- ✅ 消息确认（ACK）

### 高级功能

- ✅ 批量消息发送
- ✅ 消息拦截器
- ✅ 连接健康检查
- ✅ 指标监控（兼容 Prometheus）
- ✅ 指数退避重试策略
- ✅ 路由缓存
- ✅ Builder 模式 API
- ✅ 自定义异常

## 生成 gRPC 代码

### 前置要求

1. **Protocol Buffer 编译器**

   macOS:
   ```bash
   brew install protobuf
   ```

   Ubuntu/Debian:
   ```bash
   apt-get install protobuf-compiler
   ```

2. **gRPC PHP 插件**

   方法一 - 使用自动化脚本（推荐）：
   ```bash
   bash scripts/grpc_tool.sh install
   ```

   方法二 - 从源码编译：
   ```bash
   git clone --recursive https://github.com/grpc/grpc.git
   cd grpc
   mkdir cmake/build && cd cmake/build
   cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_CODEGEN=ON ../..
   make grpc_php_plugin
   sudo make install
   ```

### 生成代码

```bash
bash scripts/grpc_tool.sh generate
```

这将在 `grpc/` 目录中生成所有必要的 gRPC 类。

## 示例代码

查看 [examples](./examples) 目录获取完整的可运行示例：

- [ProducerNormalMessageExample.php](./examples/ProducerNormalMessageExample.php) - 普通消息
- [ProducerFifoMessageExample.php](./examples/ProducerFifoMessageExample.php) - FIFO 顺序消息
- [ProducerDelayMessageExample.php](./examples/ProducerDelayMessageExample.php) - 延时消息
- [ProducerTransactionMessageExample.php](./examples/ProducerTransactionMessageExample.php) - 事务消息
- [ProducerPriorityMessageExample.php](./examples/ProducerPriorityMessageExample.php) - 优先级消息
- [PushConsumerExample.php](./examples/PushConsumerExample.php) - 推送消费者
- [SimpleConsumerExample.php](./examples/SimpleConsumerExample.php) - 简单消费者

## 测试

运行测试验证您的环境：

```bash
cd php
php tests/test_producer_consumer.php
```

## 常见问题

### 问题：找不到 grpc_php_plugin

**解决方案：** 使用以下命令安装插件：
```bash
bash scripts/grpc_tool.sh install
```

### 问题：清理 /tmp/grpc 时权限被拒绝

**解决方案：** 使用 sudo 删除旧文件：
```bash
sudo rm -rf /tmp/grpc
```

### 问题：Swoole 协程 API 必须在协程中调用

**解决方案：** 确保像 `\Swoole\Coroutine::sleep()` 这样的 Swoole API 在由 `\Swoole\Coroutine::create()` 或 `go()` 创建的协程上下文中调用。

## 参考资料

- [Apache RocketMQ 文档](https://rocketmq.apache.org/zh/docs/)
- [RocketMQ 5.x 架构](https://rocketmq.apache.org/zh/docs/domainModel/01overview/)
- [gRPC PHP](https://grpc.io/docs/languages/php/)
