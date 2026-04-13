<?php
/**
 * Test SimpleConsumer with Swoole AsyncTelemetrySession
 */

// Check if Swoole is available
if (!extension_loaded('swoole')) {
    echo "✗ Swoole extension is not installed\n";
    exit(1);
}

echo "✓ Swoole " . SWOOLE_VERSION . " detected\n\n";

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/Consumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Builder\MessageBuilder as MsgBuilder;

$endpoints = '127.0.0.1:8080';
$topic = 'topic-php';
$consumerGroup = 'GID-php';
$producerGroup = 'GID-php';

echo "=== Testing SimpleConsumer with Swoole AsyncTelemetry ===\n\n";

// Step 1: Send test messages
echo "Step 1: Sending 5 test messages...\n";
try {
    $config = new ClientConfiguration($endpoints);
    $config->withSslEnabled(false);
    
    $producer = Producer::getInstance($config, $producerGroup);
    $producer->start();
    
    for ($i = 1; $i <= 5; $i++) {
        $msgBuilder = new MsgBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Swoole async test message #{$i}")
            ->setTag("test-tag")
            ->setKeys(["key-{$i}"])
            ->build();
        
        $receipt = $producer->send($message);
        echo "  [{$i}/5] ✓ Sent - ID: {$receipt->getMessageId()}\n";
    }
    
    $producer->shutdown();
    echo "✓ All messages sent successfully\n\n";
} catch (\Exception $e) {
    echo "✗ Failed to send messages: " . $e->getMessage() . "\n\n";
    exit(1);
}

// Step 2: Create SimpleConsumer with AsyncTelemetrySession
echo "Step 2: Creating SimpleConsumer (with Swoole AsyncTelemetry)...\n";

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

$consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
$consumer->start();

echo "✓ Consumer started\n\n";

$maxMessages = 5;
$timeout = 30;

// Step 3: Receive messages (in coroutine)
go(function() use ($consumer, $maxMessages, $timeout) {
    echo "Step 3: Receiving messages...\n";
    echo "Polling for up to {$timeout} seconds...\n\n";
    
    $startTime = time();
    $receivedCount = 0;
    
    while ($receivedCount < $maxMessages && (time() - $startTime) < $timeout) {
        try {
            $messages = $consumer->receive(10, 30);
            
            if (!empty($messages)) {
                foreach ($messages as $grpcMessage) {
                    $receivedCount++;
                    $sysProps = $grpcMessage->getSystemProperties();
                    $msgId = $sysProps->getMessageId();
                    $body = $grpcMessage->getBody();
                    
                    echo "\n✓ Received message #{$receivedCount}\n";
                    echo "  - Message ID: {$msgId}\n";
                    echo "  - Body: {$body}\n";
                    
                    // Acknowledge the message
                    try {
                        $consumer->ack($grpcMessage);
                        echo "  - ACK: Success\n";
                    } catch (\Exception $e) {
                        echo "  - ACK: Failed - " . $e->getMessage() . "\n";
                    }
                }
            } else {
                echo ".";
                flush();
                \Swoole\Coroutine::sleep(0.5);  // Use Swoole's non-blocking sleep
            }
        } catch (\Exception $e) {
            echo "\n✗ Error: " . $e->getMessage() . "\n";
            break;
        }
    }
    
    echo "\n\n";
    echo "=== Test Summary ===\n";
    echo "Total messages received: {$receivedCount}\n";
    echo "Expected messages: {$maxMessages}\n";
    
    if ($receivedCount >= $maxMessages) {
        echo "✓ Test PASSED - All messages received successfully!\n";
    } elseif ($receivedCount > 0) {
        echo "⚠ Partial success - Received {$receivedCount}/{$maxMessages} messages\n";
    } else {
        echo "✗ Test FAILED - No messages received\n";
        echo "\nNote: This may be due to RocketMQ Proxy limitations with PHP gRPC.\n";
        echo "The AsyncTelemetrySession is running, but the Proxy may not properly\n";
        echo "associate it with ReceiveMessage requests.\n";
    }
    
    $consumer->close();
    echo "\n✓ Consumer closed\n";
    
    echo "\n=== Test Complete ===\n";
    
    // Exit the event loop
    \Swoole\Event::exit();
});

// Start the event loop
\Swoole\Event::wait();
