# PHP Client for Apache RocketMQ

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D7.4-blue)](https://www.php.net/)
[![RocketMQ Version](https://img.shields.io/badge/RocketMQ-5.x-orange)](https://rocketmq.apache.org/)

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the PHP implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the legacy clients, this implementation is based on the computing-storage separation architecture of RocketMQ 5.x, which is the recommended approach for accessing RocketMQ services.

Prerequisites you need to know (or refer to [quick start](https://rocketmq.apache.org/docs/quickStart/02quickstart/)):

1. PHP 7.4+ for runtime;
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

### Installation

Install dependencies using Composer:

```bash
composer install
```

### Generate gRPC Code

Before using the client, you need to generate gRPC code from proto files:

```bash
cd php
bash scripts/grpc_tool.sh generate
```

For detailed instructions, see [Generate gRPC Code](#generate-grpc-code) section below.

### Quick Example

#### Producer

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
echo "Message sent: " . $result->getMessageId() . "\n";

$producer->close();
```

#### PushConsumer

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
        echo "Received: " . $message->getBody() . "\n";
        return ConsumeResult::SUCCESS;
    })
    ->build();

$consumer->start();
```

More examples are available in the [examples](./examples) directory to help you work with different message types:
- Normal messages
- FIFO ordered messages
- Delayed/scheduled messages
- Transaction messages
- Priority messages
- Lite topic messages

## Logging System

The PHP client uses a custom Logger class that follows SLF4J-style formatting (compatible with Java's logging pattern).

Log output path: `$HOME/logs/rocketmq/rocketmq-client.log`

You can configure log level by setting environment variable:

```bash
export ROCKETMQ_LOG_LEVEL=DEBUG  # Options: DEBUG, INFO, WARN, ERROR
```

## Features

### Core Features

- ✅ Normal message send and consume
- ✅ FIFO ordered messages (with consume accelerator)
- ✅ Scheduled/delayed messages
- ✅ Transaction messages
- ✅ Priority messages
- ✅ Lite topic messages
- ✅ Message tags and keys
- ✅ SimpleConsumer mode (active pull)
- ✅ PushConsumer mode (listener push)
- ✅ Async message sending
- ✅ Message acknowledgment (ACK)

### Advanced Features

- ✅ Batch message sending
- ✅ Message interceptor
- ✅ Connection health check
- ✅ Metrics monitoring (Prometheus compatible)
- ✅ Exponential backoff retry policy
- ✅ Route cache
- ✅ Builder pattern API
- ✅ Custom exceptions

## Generate gRPC Code

### Prerequisites

1. **Protocol Buffer Compiler**

   macOS:
   ```bash
   brew install protobuf
   ```

   Ubuntu/Debian:
   ```bash
   apt-get install protobuf-compiler
   ```

2. **gRPC PHP Plugin**

   Method 1 - Using automated script (recommended):
   ```bash
   bash scripts/grpc_tool.sh install
   ```

   Method 2 - Compile from source:
   ```bash
   git clone --recursive https://github.com/grpc/grpc.git
   cd grpc
   mkdir cmake/build && cd cmake/build
   cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_CODEGEN=ON ../..
   make grpc_php_plugin
   sudo make install
   ```

### Generate Code

```bash
bash scripts/grpc_tool.sh generate
```

This will generate all necessary gRPC classes in the `grpc/` directory.

## Examples

See the [examples](./examples) directory for complete working examples:

- [ProducerNormalMessageExample.php](./examples/ProducerNormalMessageExample.php)
- [ProducerFifoMessageExample.php](./examples/ProducerFifoMessageExample.php)
- [ProducerDelayMessageExample.php](./examples/ProducerDelayMessageExample.php)
- [ProducerTransactionMessageExample.php](./examples/ProducerTransactionMessageExample.php)
- [ProducerPriorityMessageExample.php](./examples/ProducerPriorityMessageExample.php)
- [PushConsumerExample.php](./examples/PushConsumerExample.php)
- [SimpleConsumerExample.php](./examples/SimpleConsumerExample.php)

## Testing

Run tests to verify your setup:

```bash
cd php
php tests/test_producer_consumer.php
```

## Troubleshooting

### Issue: grpc_php_plugin not found

**Solution:** Install the plugin using:
```bash
bash scripts/grpc_tool.sh install
```

### Issue: Permission denied when cleaning /tmp/grpc

**Solution:** Use sudo to remove old files:
```bash
sudo rm -rf /tmp/grpc
```

### Issue: Swoole coroutine API must be called in coroutine

**Solution:** Ensure Swoole APIs like `\Swoole\Coroutine::sleep()` are called within a coroutine context created by `\Swoole\Coroutine::create()` or `go()`.

## References

- [Apache RocketMQ Documentation](https://rocketmq.apache.org/docs/)
- [RocketMQ 5.x Architecture](https://rocketmq.apache.org/docs/domainModel/01overview/)
- [gRPC PHP](https://grpc.io/docs/languages/php/)
