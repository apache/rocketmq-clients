<?php
/**
 * Test PushConsumer-style message consumption using Swoole coroutines
 * 
 * This simulates PushConsumer behavior using SimpleConsumer + Swoole coroutines
 */

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

echo "=== Testing PushConsumer (Swoole Coroutine Mode) ===\n\n";

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
            ->setBody("PushConsumer test message #{$i}")
            ->setTag("push-test")
            ->setKeys(["push-key-{$i}"])
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

// Step 2: Create SimpleConsumer
echo "Step 2: Creating consumer...\n";
$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

$consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
$consumer->start();
echo "✓ Consumer started\n\n";

// Message counter
$messageCount = 0;
$receivedMessages = [];

// Step 3: Define message listener (callback function)
echo "Step 3: Defining message listener...\n";
$messageListener = function($message) use (&$messageCount, &$receivedMessages) {
    $messageCount++;
    $sysProps = $message->getSystemProperties();
    $msgId = $sysProps->getMessageId();
    $body = $message->getBody();
    $tag = $sysProps->getTag() ?: 'N/A';
    
    // Convert RepeatedField to array
    $keysObj = $sysProps->getKeys();
    $keys = [];
    if ($keysObj) {
        foreach ($keysObj as $key) {
            $keys[] = $key;
        }
    }
    
    echo "\n📨 Received message #{$messageCount}";
    echo "\n   Message ID: {$msgId}";
    echo "\n   Body: {$body}";
    echo "\n   Tag: {$tag}";
    echo "\n   Keys: " . (empty($keys) ? 'N/A' : implode(', ', $keys));
    
    // Store message info
    $receivedMessages[] = [
        'id' => $msgId,
        'body' => $body,
        'tag' => $tag,
        'timestamp' => time()
    ];
    
    return true; // Success
};

echo "✓ Message listener defined\n\n";

// Step 4: Start consuming in coroutine (simulating PushConsumer)
echo "Step 4: Starting message consumption (PushConsumer mode)...\n";
echo "Waiting for messages (timeout: 30 seconds)...\n\n";

go(function() use ($consumer, $messageListener, &$messageCount, $topic) {
    $startTime = time();
    $maxWaitTime = 30;
    $pollInterval = 1; // seconds
    
    while ((time() - $startTime) < $maxWaitTime) {
        try {
            // Poll for messages
            $messages = $consumer->receive(10, 5);
            
            if (!empty($messages)) {
                foreach ($messages as $message) {
                    // Call message listener
                    $result = $messageListener($message);
                    
                    // Auto ACK on success
                    if ($result === true) {
                        try {
                            $consumer->ack($message);
                            echo "\n   ✓ ACK: Success\n";
                        } catch (\Exception $e) {
                            echo "\n   ✗ ACK: Failed - " . $e->getMessage() . "\n";
                        }
                    }
                }
            } else {
                // No messages, wait a bit
                \Swoole\Coroutine::sleep($pollInterval);
            }
        } catch (\Exception $e) {
            echo "\n✗ Error: " . $e->getMessage() . "\n";
            \Swoole\Coroutine::sleep(1);
        }
    }
    
    echo "\n\n⏱ Timeout reached (30s)\n";
    
    // Print summary
    echo "\n=== Test Summary ===\n";
    echo "Total messages received: {$messageCount}\n";
    
    if ($messageCount > 0) {
        echo "✓ Test PASSED - Successfully consumed messages!\n";
        echo "\nReceived messages:\n";
        foreach ($receivedMessages as $idx => $msg) {
            echo "  " . ($idx + 1) . ". [{$msg['id']}] {$msg['body']}\n";
        }
    } else {
        echo "⚠ No messages received\n";
    }
    
    echo "\n=== Test Complete ===\n";
    
    // Close consumer
    $consumer->close();
    echo "✓ Consumer closed\n";
    
    // Exit event loop
    \Swoole\Event::exit();
});

// Start the event loop
\Swoole\Event::wait();
