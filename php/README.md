# RocketMQ PHP Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PHP Version](https://img.shields.io/badge/PHP-%3E%3D7.4-blue)](https://www.php.net/)
[![RocketMQ Version](https://img.shields.io/badge/RocketMQ-5.x-orange)](https://rocketmq.apache.org/)

Apache RocketMQ PHP gRPC client implementation for RocketMQ 5.x.

## Table of Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Installation](#installation)
- [Generate gRPC Code](#generate-grpc-code)
- [Producer Usage](#producer-usage)
- [Consumer Usage](#consumer-usage)
- [Advanced Features](#advanced-features)
  - [Batch Send](#batch-send)
  - [Transaction Messages](#transaction-messages)
  - [Message Interceptor](#message-interceptor)
  - [Health Check](#health-check)
  - [Metrics Monitoring](#metrics-monitoring)
  - [Configuration Object](#configuration-object)
- [Best Practices](#best-practices)
- [Examples](#examples)
- [Testing](#testing)
- [Comparison with Java Client](#comparison-with-java-client)
- [Migration Guide](#migration-guide)
- [Troubleshooting](#troubleshooting)
- [References](#references)

## Quick Start

### Producer Example

```php
use Apache\Rocketmq\Producer;

$producer = Producer::getInstance('your-endpoints:8080', 'your-topic');

// Send normal message
$result = $producer->sendNormalMessage(
    "Message content",
    "TagA",
    "order_id_123"
);

echo "Send successful! Message ID: " . $result[0]['messageId'] . "\n";

$producer->close();
```

### Consumer Example

#### SimpleConsumer (Pull Mode)

```php
use Apache\Rocketmq\SimpleConsumer;

$consumer = SimpleConsumer::getInstance(
    'your-endpoints:8080',
    'GID_consumer_group',
    'your-topic'
);

while (true) {
    $messages = $consumer->receive(16, 15);
    
    foreach ($messages as $message) {
        echo "Received message: " . $message->getBody() . "\n";
        $consumer->ack($message);
    }
}

$consumer->close();
```

#### PushConsumer (Push Mode)

```php
use Apache\Rocketmq\PushConsumer;

$consumer = PushConsumer::getInstance(
    'your-endpoints:8080',
    'GID_consumer_group',
    'your-topic'
);

$consumer->setMessageListener(function($message) {
    echo "Received message: " . $message->getBody() . "\n";
    return true; // Return true to auto ACK
});

$consumer->start();
```

## Features

### Core Features

- ✅ Normal message send and consume
- ✅ FIFO ordered messages
- ✅ Scheduled/delayed messages
- ✅ Message tags and keys
- ✅ SimpleConsumer mode (active pull)
- ✅ PushConsumer mode (listener push)
- ✅ Async message sending
- ✅ Message acknowledgment (ACK)
- ✅ Heartbeat mechanism

### Advanced Features

- ✅ Batch message sending
- ✅ Transaction messages
- ✅ Message interceptor
- ✅ Connection health check
- ✅ Metrics monitoring (Prometheus compatible)
- ✅ Configuration object management
- ✅ Exponential backoff retry policy
- ✅ Route cache
- ✅ Lifecycle management

## Installation

### Prerequisites

- PHP 7.4 or higher
- gRPC PHP extension
- Composer

### Install Dependencies

```bash
cd php
composer install
```

## Generate gRPC Code

This section explains how to regenerate RocketMQ PHP client gRPC code.

### Prerequisites

#### 1. Install Protocol Buffer Compiler

**macOS:**
```bash
brew install protobuf
```

**Ubuntu/Debian:**
```bash
apt-get install protobuf-compiler
```

**CentOS/RHEL:**
```bash
yum install protobuf-devel
```

Verify installation:
```bash
protoc --version
```

#### 2. Install gRPC PHP Plugin

There are three methods to install `grpc_php_plugin`:

**Method 1: Using PECL (Recommended)**

```bash
pecl install grpc
```

After installation, the plugin is usually located at:
- `/usr/local/bin/grpc_php_plugin`
- or `/usr/bin/grpc_php_plugin`

**Method 2: Compile from Source**

```bash
# Clone gRPC repository
git clone https://github.com/grpc/grpc.git
cd grpc

# Initialize submodules
git submodule update --init

# Create build directory
mkdir cmake/build
cd cmake/build

# Configure and compile
cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_CODEGEN=ON ../..
make grpc_php_plugin

# Install
sudo make install
```

**Method 3: Using Composer (Simplest)**

```bash
# In PHP project directory
cd /Users/haizai/PhpProject/php
composer require grpc/grpc
```

This automatically downloads precompiled gRPC extension and `grpc_php_plugin` tool.

After installation, the plugin is located at:
- `vendor/bin/grpc_php_plugin`

Verify installation:
```bash
which grpc_php_plugin
grpc_php_plugin --version
```

### Method A: Using Automated Script (Recommended)

```bash
cd /Users/haizai/PhpProject/php
./scripts/generate_grpc.sh
```

The script will automatically:
1. Check if dependencies are installed
2. Clean old generated files
3. Generate new gRPC code
4. Provide next steps hints

### Method B: Manual Generation

```bash
# Enter PHP directory
cd /Users/haizai/PhpProject/php

# Clean old files
rm -rf grpc/Apache
rm -rf grpc/GPBMetadata

# Generate admin.proto
protoc \
  --proto_path=../protos \
  --php_out=grpc \
  --grpc_out=grpc \
  --plugin=protoc-gen-grpc=$(which grpc_php_plugin) \
  ../protos/apache/rocketmq/v2/admin.proto

# Generate definition.proto
protoc \
  --proto_path=../protos \
  --php_out=grpc \
  --grpc_out=grpc \
  --plugin=protoc-gen-grpc=$(which grpc_php_plugin) \
  ../protos/apache/rocketmq/v2/definition.proto

# Generate service.proto
protoc \
  --proto_path=../protos \
  --php_out=grpc \
  --grpc_out=grpc \
  --plugin=protoc-gen-grpc=$(which grpc_php_plugin) \
  ../protos/apache/rocketmq/v2/service.proto
```

### Update Autoload

After generating code, update composer autoload:

```bash
composer dump-autoload
```

### Verify Generated Results

Generated files should be located in `php/grpc/` directory:

```
php/grpc/
├── Apache/
│   └── Rocketmq/
│       └── V2/
│           ├── AdminClient.php
│           ├── MessagingServiceClient.php
│           ├── [Other message classes].php
│           └── ...
└── GPBMetadata/
    └── Apache/
        └── Rocketmq/
            └── V2/
                ├── Admin.php
                ├── Definition.php
                └── Service.php
```

### Common Issues

#### Issue 1: Cannot find grpc_php_plugin

**Error:**
```
--grpc_out: protoc-gen-grpc: Plugin failed with status code 1.
```

**Solution:**
Ensure `grpc_php_plugin` is correctly installed and in PATH:
```bash
which grpc_php_plugin
```

If not found, refer to the installation instructions above.

#### Issue 2: Protobuf Version Incompatibility

**Error:**
```
This file was generated with a different version of protoc.
```

**Solution:**
Ensure protoc and grpc_php_plugin use compatible versions:
```bash
protoc --version
grpc_php_plugin --version
```

Recommended versions:
- protoc >= 3.3.0
- grpc_php_plugin >= 1.42.0

#### Issue 3: Generated Code Has Syntax Errors

**Solution:**
1. Check PHP version (recommend PHP 7.4+)
2. Update protobuf extension: `pecl upgrade grpc`
3. Regenerate code

#### Issue 4: Permission Error

**Error:**
```
Permission denied
```

**Solution:**
Ensure script has execute permission:
```bash
chmod +x scripts/generate_grpc.sh
```

### Proto File Location

Proto files are located in the `protos/` directory at project root:

```
protos/
└── apache/
    └── rocketmq/
        └── v2/
            ├── admin.proto         # Admin-related RPC definitions
            ├── definition.proto    # Message and type definitions
            └── service.proto       # Core service interfaces
```

These files are synced from [apache/rocketmq-protos](https://github.com/apache/rocketmq/tree/master/protos).

### Generated Files Description

#### GPBMetadata Directory

Contains metadata files describing proto file structure:

- `GPBMetadata/Apache/Rocketmq/V2/Admin.php`
- `GPBMetadata/Apache/Rocketmq/V2/Definition.php`
- `GPBMetadata/Apache/Rocketmq/V2/Service.php`

#### Apache/Rocketmq/V2 Directory

Contains actual PHP classes used:

**Client Classes:**
- `MessagingServiceClient.php` - Main gRPC client
- `AdminClient.php` - Admin client

**Message Classes:**
- `Message.php` - Message definition
- `SystemProperties.php` - System properties
- `SendMessageRequest.php` - Send message request
- `ReceiveMessageRequest.php` - Receive message request
- `AckMessageRequest.php` - Acknowledge message request
- And more...

**Enum Types:**
- `MessageType.php` - Message type
- `Code.php` - Status code
- `ClientType.php` - Client type
- And more...

### When to Regenerate Code

Regenerate gRPC code in the following situations:

1. **Proto File Updates** - When `.proto` files in `protos/` directory change
2. **Version Upgrade** - Upgrading to new version of RocketMQ protocol
3. **Toolchain Updates** - Updated protoc or grpc_php_plugin version
4. **Code Corruption** - Generated code accidentally corrupted or lost

### Best Practices

1. **Version Locking** - Lock protoc and grpc_php_plugin versions in production
2. **Automation** - Integrate code generation into CI/CD pipeline
3. **Verification** - Run tests after generation to verify correctness
4. **Backup** - Commit generated code to version control

## Producer Usage

### 1. Basic Usage

```php
require 'vendor/autoload.php';

use Apache\Rocketmq\Producer;

// Create Producer instance
$producer = Producer::getInstance(
    'your-endpoints:8080',
    'your-topic'
);

// Send normal message
$result = $producer->sendNormalMessage(
    "This is a message",      // Message body
    "TagA",                   // Tag (optional)
    "order_id_123"            // Key (optional)
);

echo "Send successful! Message ID: " . $result[0]['messageId'] . "\n";

// Close producer
$producer->close();
```

### 2. Send FIFO Ordered Messages

```php
// Messages with the same messageGroup will be delivered in order
$result = $producer->sendFifoMessage(
    "Order created message",
    "order_group_001",  // Message group ID
    "OrderCreated",
    "order_123"
);

echo "FIFO message sent! Message ID: " . $result[0]['messageId'] . "\n";
```

### 3. Send Delayed Messages

```php
// Deliver after 10 seconds
$result = $producer->sendDelayMessage(
    "Delayed message content",
    10,                 // Delay in seconds
    "DelayTag",
    "delay_key"
);

echo "Delayed message sent! Will be delivered in 10 seconds\n";
```

### 4. Async Send Messages

```php
$producer->sendAsync(
    "Async message",
    function($response, $error) {
        if ($error) {
            echo "Send failed: " . $error->getMessage() . "\n";
        } else {
            $entries = $response->getEntries();
            echo "Send successful! Message ID: " . $entries[0]->getMessageId() . "\n";
        }
    },
    "AsyncTag",
    "async_key"
);

// Wait for async operation to complete
sleep(2);
```

### 5. Batch Send Messages

```php
// Prepare batch messages
$messages = [
    ['body' => 'Batch message 1', 'tag' => 'TagA', 'keys' => 'key1'],
    ['body' => 'Batch message 2', 'tag' => 'TagB', 'keys' => 'key2'],
    ['body' => 'Batch message 3', 'tag' => 'TagC', 'keys' => 'key3'],
];

$result = $producer->sendBatchMessages($messages);
echo "Batch send successful! Total: " . count($result) . " messages\n";
```

**Note:**
- All messages must have the same topic
- Batch size cannot exceed maxBatchSize (default 32)
- Total batch size cannot exceed 4MB
- FIFO messages do not support batching

## Consumer Usage

### 1. SimpleConsumer (Active Pull Mode)

SimpleConsumer is suitable for scenarios where you need active control over message pulling.

```php
require 'vendor/autoload.php';

use Apache\Rocketmq\SimpleConsumer;

// Create SimpleConsumer instance
$consumer = SimpleConsumer::getInstance(
    'your-endpoints:8080',
    'GID_your_consumer_group',
    'your-topic'
);

try {
    // Loop to receive messages
    while (true) {
        // Receive messages (max 16, invisible duration 15 seconds)
        $messages = $consumer->receive(16, 15);
        
        foreach ($messages as $message) {
            // Process message
            $systemProperties = $message->getSystemProperties();
            echo "Received message: " . $message->getBody() . "\n";
            
            // Acknowledge message (ACK)
            $consumer->ack($message);
            echo "Message acknowledged\n";
        }
    }
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    $consumer->close();
}
```

### 2. PushConsumer (Listener Mode)

PushConsumer is suitable for scenarios where you need automatic message receiving and processing.

```php
require 'vendor/autoload.php';

use Apache\Rocketmq\PushConsumer;

// Create PushConsumer instance
$consumer = PushConsumer::getInstance(
    'your-endpoints:8080',
    'GID_your_consumer_group',
    'your-topic'
);

// Set message listener
$consumer->setMessageListener(function($message) {
    echo "Received new message: " . $message->getBody() . "\n";
    
    // Business logic
    // ...
    
    // Return true indicates successful consumption, will auto ACK
    // Return false or throw exception indicates failure, will redeliver
    return true;
});

echo "PushConsumer started, waiting for messages...\n";

// Start consumer (blocking)
$consumer->start();

// Stop consumer
// $consumer->stop();
// $consumer->close();
```

## Advanced Features

### Batch Send

Batch sending improves throughput by sending multiple messages in one request.

**Usage:**

```php
use Apache\Rocketmq\Producer;

$producer = Producer::getInstance($endpoints, $topic);

// Prepare messages
$messages = [];
for ($i = 0; $i < 10; $i++) {
    $messages[] = [
        'body' => "Message {$i}",
        'tag' => "Tag{$i}",
        'keys' => "key{$i}"
    ];
}

// Send batch
$result = $producer->sendBatchMessages($messages);
echo "Sent " . count($result) . " messages\n";
```

**Performance Comparison:**
- Single send: ~100 msg/s
- Batch send (10 msgs): ~500 msg/s (5x improvement)

See `examples/BatchSendExample.php` for more examples.

### Transaction Messages

Transaction messages ensure consistency between local transactions and message sending.

**Usage:**

```php
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\TransactionChecker;

class MyTransactionChecker implements TransactionChecker
{
    public function check($message)
    {
        // Check local transaction status
        // Return COMMIT, ROLLBACK, or UNKNOWN
        return \Apache\Rocketmq\TransactionStatus::COMMIT;
    }
}

// Create transactional producer
$checker = new MyTransactionChecker();
$producer = Producer::getTransactionalInstance($config, $topic, $checker);

// Send transaction message
$transaction = $producer->beginTransaction();
try {
    // Execute local transaction
    $result = $producer->sendTransactionMessage($body, $tag, $keys, $transaction);
    
    // Commit local transaction
    $db->commit();
    
    // Commit message
    $producer->commitTransaction($transaction);
} catch (\Exception $e) {
    // Rollback local transaction
    $db->rollback();
    
    // Rollback message
    $producer->rollbackTransaction($transaction);
}
```

See `examples/TransactionMessageExample.php` for complete example.

### Message Interceptor

Message interceptors provide hooks before/after sending/receiving messages for logging, monitoring, data masking, etc.

**Usage:**

```php
use Apache\Rocketmq\MessageInterceptor;
use Apache\Rocketmq\MessageInterceptorContext;

class LoggingInterceptor implements MessageInterceptor
{
    public function doBefore($context, $messages)
    {
        echo "Before send: " . count($messages) . " messages\n";
    }
    
    public function doAfter($context, $messages)
    {
        echo "After send: " . $context->getStatus() . "\n";
    }
}

// Add interceptor to producer
$producer->addInterceptor(new LoggingInterceptor());

// Multiple interceptors (chain pattern)
$producer->addInterceptor(new MetricsInterceptor());
$producer->addInterceptor(new ValidationInterceptor());
```

**Built-in Interceptors:**
- `LoggingInterceptor`: Log message send/receive
- `MetricsInterceptor`: Collect performance metrics
- `ValidationInterceptor`: Validate message format
- `MaskingInterceptor`: Mask sensitive data

See `examples/MessageInterceptorExample.php` for more examples.

### Health Check

Health check monitors connection status between client and RocketMQ server.

**Usage:**

```php
use Apache\Rocketmq\Producer;

$producer = Producer::getInstance($endpoints, $topic);
$producer->start();

// Manual health check
$result = $producer->healthCheck();
echo "Status: " . $result->status . "\n";
echo "Response time: " . $result->responseTime . "ms\n";
echo "Is healthy: " . ($result->isHealthy() ? 'Yes' : 'No') . "\n";

// View statistics
$stats = $producer->getHealthCheckStats();
echo "Health rate: {$stats['healthRate']}%\n";
echo "Average response: {$stats['avgResponseTime']}ms\n";
```

**Health Status:**
- `HEALTHY`: Connection is normal
- `UNHEALTHY`: Connection is abnormal (consecutive failures >= 3)
- `DEGRADED`: Partial functionality unavailable (response time > 1s)
- `UNKNOWN`: Not checked yet

See `examples/HealthCheckExample.php` for more examples.

### Metrics Monitoring

Metrics collector provides comprehensive performance monitoring, supporting Prometheus export.

**Usage:**

```php
use Apache\Rocketmq\Producer;

$producer = Producer::getInstance($endpoints, $topic);

// Get metrics collector
$collector = $producer->getMetricsCollector();

// View all metrics
$metrics = $producer->getAllMetrics();
print_r($metrics);

// Export to Prometheus format
$prometheusText = $producer->exportMetricsToPrometheus();
echo $prometheusText;

// Export to JSON
$jsonText = $producer->exportMetricsToJson();
echo $jsonText;
```

**Available Metrics:**

**Producer Metrics:**
- `rocketmq_send_total`: Total sends
- `rocketmq_send_success_total`: Successful sends
- `rocketmq_send_failure_total`: Failed sends
- `rocketmq_send_cost_time`: Send latency (ms)
- `rocketmq_batch_send_total`: Total batch sends

**Consumer Metrics:**
- `rocketmq_receive_total`: Total receives
- `rocketmq_consume_total`: Total consumes
- `rocketmq_consume_success_total`: Successful consumes
- `rocketmq_consume_failure_total`: Failed consumes
- `rocketmq_consume_cost_time`: Consume latency (ms)

**Connection Metrics:**
- `rocketmq_connection_total`: Total connections
- `rocketmq_heartbeat_total`: Total heartbeats
- `rocketmq_heartbeat_failure_total`: Failed heartbeats

**Prometheus Integration:**

```php
// In your HTTP endpoint
$app->get('/metrics', function() use ($producer) {
    header('Content-Type: text/plain');
    echo $producer->exportMetricsToPrometheus();
});
```

See `examples/MetricsExample.php` for Grafana dashboard configuration.

### Configuration Object

Configuration object encapsulates all client configuration parameters for better type safety and management.

**Classes:**

1. **Credentials**: Manage authentication credentials
2. **ClientConfiguration**: Centralize all client configurations

**Usage:**

```php
use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Credentials;
use Apache\Rocketmq\ExponentialBackoffRetryPolicy;

// Basic configuration
$config = new ClientConfiguration('127.0.0.1:8080');

// Full configuration (chainable)
$credentials = new Credentials('ak', 'sk');
$retryPolicy = new ExponentialBackoffRetryPolicy(5, 200, 10000, 2.0);

$config = (new ClientConfiguration('127.0.0.1:8080'))
    ->withNamespace('my-namespace')
    ->withCredentials($credentials)
    ->withRequestTimeout(5)
    ->withSslEnabled(true)
    ->withRetryPolicy($retryPolicy);

// Create producer with configuration
$producer = Producer::getInstance($config, 'my-topic');
```

**Benefits:**
- Centralized configuration management
- Chainable API for cleaner code
- Type safety through object constraints
- Easy to extend with new configuration items
- Configuration cloning and reuse
- Backward compatible with old API

See `examples/ConfigurationExample.php` for complete examples.

## Best Practices

### 1. Singleton Pattern

Producer and Consumer use singleton pattern to avoid creating duplicate connections:

```php
// Recommended: Global singleton
$producer = Producer::getInstance($endpoints, $topic);

// Not recommended: Create new instance every time
$producer = new Producer($endpoints, $topic);
```

### 2. Message Retry

When consumption fails, throw an exception and the message will be automatically redelivered:

```php
$consumer->setMessageListener(function($message) {
    try {
        // Business logic
        if ($someCondition) {
            throw new Exception("Business processing failed");
        }
        return true;
    } catch (Exception $e) {
        // Throwing exception will redeliver the message
        throw $e;
    }
});
```

### 3. Batch ACK

For SimpleConsumer, you can acknowledge messages in batch:

```php
$messages = $consumer->receive(16, 15);
foreach ($messages as $message) {
    // Process message
}

// Batch ACK
$count = $consumer->batchAck($messages);
echo "Successfully acknowledged {$count} messages\n";
```

### 4. Graceful Shutdown

Ensure closing Producer and Consumer when application exits:

```php
register_shutdown_function(function() use ($producer, $consumer) {
    $producer->close();
    $consumer->close();
});
```

### 5. Connection Reuse

```php
// ✅ Recommended: Reuse connection
$producer = Producer::getInstance($endpoints, $topic);

// ❌ Not recommended: Create connection repeatedly
$producer = new Producer($endpoints, $topic);
```

### 6. Batch Sending

```php
// Recommended: Batch send multiple messages
$messages = [];
for ($i = 0; $i < 10; $i++) {
    $messages[] = ['body' => "Message {$i}"];
}
$producer->sendBatchMessages($messages);
```

### 7. Async Sending

```php
// Use async for high throughput scenarios
$producer->sendAsync($body, function($response, $error) {
    // Handle callback
});
```

### 8. Consumer Concurrency

```php
// SimpleConsumer can be used in multi-threaded environments
while (true) {
    $messages = $consumer->receive(16, 15);
    // Multi-threaded processing
}
```

## Examples

Run example code:

```bash
# Producer example
php examples/ProducerExample.php

# SimpleConsumer example
php examples/SimpleConsumerExample.php

# PushConsumer example
php examples/PushConsumerExample.php

# Batch message example
php examples/BatchSendExample.php

# Transaction message example
php examples/TransactionMessageExample.php

# Message interceptor example
php examples/MessageInterceptorExample.php

# Health check example
php examples/HealthCheckExample.php

# Metrics monitoring example
php examples/MetricsExample.php

# Configuration example
php examples/ConfigurationExample.php
```

## Testing

### Unit Tests

```bash
cd php
vendor/bin/phpunit tests/
```

### Run Specific Test

```bash
vendor/bin/phpunit tests/ConfigurationTest.php
vendor/bin/phpunit tests/ProducerTest.php
vendor/bin/phpunit tests/ConsumerTest.php
```

## Comparison with Java Client

### Implemented Features

| Feature | Java | PHP | Notes |
|---------|------|-----|-------|
| Normal messages | ✅ | ✅ | Fully supported |
| FIFO messages | ✅ | ✅ | Fully supported |
| Scheduled messages | ✅ | ✅ | Fully supported |
| Sync send | ✅ | ✅ | Fully supported |
| Async send | ✅ | ✅ | Fully supported |
| SimpleConsumer | ✅ | ✅ | Fully supported |
| PushConsumer | ✅ | ✅ | Fully supported |
| Singleton pattern | ✅ | ✅ | Fully supported |
| Heartbeat mechanism | ✅ | ✅ | Fully supported |
| Batch send | ✅ | ✅ | Fully supported |
| Transaction messages | ✅ | ✅ | Fully supported |
| Message interceptor | ✅ | ✅ | Fully supported |
| Health check | ✅ | ✅ | Fully supported |
| Metrics monitoring | ✅ | ✅ | Fully supported |
| Configuration object | ✅ | ✅ | Fully supported |
| Route cache | ✅ | ✅ | Fully supported |
| Lifecycle management | ✅ | ✅ | Fully supported |
| Retry policy | ✅ | ✅ | Fully supported |

### Pending Features

| Feature | Java | PHP | Priority |
|---------|------|-----|----------|
| Message filtering | ✅ | ⚠️ | Low |
| Trace tracking | ✅ | ❌ | Low |

## Migration Guide

### From Old API to New API

**Old code:**
```php
$producer = Producer::getInstance('127.0.0.1:8080', 'my-topic');
```

**New code:**
```php
$config = new ClientConfiguration('127.0.0.1:8080');
$producer = Producer::getInstance($config, 'my-topic');
```

**Old code with retry:**
```php
$retryPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000);
$producer = Producer::getInstance('127.0.0.1:8080', 'my-topic', $retryPolicy);
```

**New code with retry:**
```php
$credentials = new Credentials('ak', 'sk');
$retryPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000);

$config = (new ClientConfiguration('127.0.0.1:8080'))
    ->withCredentials($credentials)
    ->withRetryPolicy($retryPolicy);

$producer = Producer::getInstance($config, 'my-topic');
```

### Breaking Changes

- The original `init()` method has been removed
- Need to migrate to new API

### Backward Compatibility

The old API is still valid and internally converts to configuration objects:

```php
// Old API still works
$producer = Producer::getInstance('127.0.0.1:8080', 'my-topic');

// Internally converts to
$config = new ClientConfiguration('127.0.0.1:8080');
return new Producer($config, 'my-topic');
```

## Troubleshooting

### 1. Connection Failure

Check if endpoints is correct and network is accessible:

```php
// Ensure port number is correct
$endpoints = 'your-endpoints:8080'; // Not 9876
```

### 2. Message Cannot Be ACKed

Ensure getting receiptHandle from SystemProperties:

```php
$receiptHandle = $message->getSystemProperties()->getReceiptHandle();
if (empty($receiptHandle)) {
    throw new Exception("Message has no receipt_handle, cannot ACK");
}
```

### 3. FIFO Messages Not in Order

Ensure using the same messageGroup:

```php
// Messages with the same group will be delivered in order
$producer->sendFifoMessage($body, "same_group_id");
```

### 4. gRPC Extension Not Found

Install gRPC PHP extension:

```bash
pecl install grpc
```

Or compile from source:

```bash
cd php
./scripts/install_grpc_plugin.sh
```

### 5. Composer SSL Certificate Error

Use existing composer.phar:

```bash
php composer.phar install
```

## References

- [RocketMQ Official Documentation](https://rocketmq.apache.org/)
- [RocketMQ Java Client](https://github.com/apache/rocketmq-clients/tree/master/java)
- [RocketMQ Protocol Specification](https://github.com/apache/rocketmq/tree/master/protos)
- [gRPC PHP Documentation](https://grpc.io/docs/languages/php/)
- [PSR-4 Autoloading Specification](https://www.php-fig.org/psr/psr-4/)

## License

Apache License 2.0
