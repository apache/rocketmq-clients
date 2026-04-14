<?php
/**
 * Example: PushConsumer with Concurrent Message Listener
 * 
 * This example demonstrates how to use the optimized PushConsumer
 * with concurrent message consumption using Swoole coroutines.
 * 
 * Features:
 * - Concurrent message processing with configurable worker count
 * - Flow control based on cache thresholds (count and size)
 * - Detailed logging with SLF4J-style placeholders
 * - Metrics collection for monitoring
 * - Automatic ACK/NACK based on consumption result
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Consumer\ConsumeResult;

// Configuration
$endpoints = '127.0.0.1:8080';
$consumerGroup = 'test-consumer-group';
$topic = 'test-topic';

echo "=== RocketMQ PushConsumer with Concurrent Consumption ===\n\n";

// Create client configuration
$config = new ClientConfiguration($endpoints);

// Create PushConsumer instance
$consumer = PushConsumer::getInstance($config, $consumerGroup, $topic);

// Configure concurrent consumption
$consumer->setConsumptionThreadCount(20);  // Number of concurrent workers (coroutines)
$consumer->setMaxCacheMessageCount(1024);   // Maximum cached messages
$consumer->setMaxCacheMessageSizeInBytes(64 * 1024 * 1024); // 64MB cache limit
$consumer->setMaxMessageNum(16);            // Messages per receive batch
$consumer->setInvisibleDuration(15);        // Message visibility timeout

// Set message listener
$consumer->setMessageListener(function($message) {
    try {
        // Extract message information
        $messageId = $message->getMessageId();
        $topic = $message->getTopic();
        $body = $message->getBody();
        $tag = $message->getTag();
        $keys = $message->getKeys();
        
        echo "[Worker] Processing message:\n";
        echo "  Message ID: {$messageId}\n";
        echo "  Topic: {$topic}\n";
        echo "  Tag: " . ($tag ?? 'null') . "\n";
        echo "  Keys: " . implode(', ', $keys ?? []) . "\n";
        echo "  Body: {$body}\n";
        echo "  Born Timestamp: " . date('Y-m-d H:i:s', $message->getBornTimestamp() / 1000) . "\n\n";
        
        // Simulate message processing
        usleep(rand(10000, 50000)); // 10-50ms
        
        // Return consumption result
        return ConsumeResult::SUCCESS;
        
    } catch (\Exception $e) {
        echo "[ERROR] Failed to process message: " . $e->getMessage() . "\n\n";
        return ConsumeResult::FAILURE;
    }
});

echo "Starting PushConsumer with concurrent consumption...\n";
echo "Configuration:\n";
echo "  Endpoints: {$endpoints}\n";
echo "  Consumer Group: {$consumerGroup}\n";
echo "  Topic: {$topic}\n";
echo "  Consumption Workers: 20\n";
echo "  Max Cache Count: 1024 messages\n";
echo "  Max Cache Size: 64 MB\n";
echo "  Batch Size: 16 messages\n\n";

// Start consumer (blocking mode with concurrent consumption)
try {
    $consumer->start();
} catch (\Exception $e) {
    echo "Fatal error: " . $e->getMessage() . "\n";
    exit(1);
}
