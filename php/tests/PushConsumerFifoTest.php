<?php
/**
 * Test script for PushConsumer FIFO message consumption
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../Consumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Consumer\ConsumeResult;

echo "=== PushConsumer FIFO Message Test ===\n\n";

$endpoints = '127.0.0.1:8080';
$topic = 'topic-order';
$consumerGroup = 'GID-order-consumer';

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Step 1: Send FIFO messages with different message groups
echo "[Step 1] Sending FIFO messages...\n";
$producer = Producer::getInstance($config, $topic);
$producer->start();

$messageGroups = ['group-A', 'group-B', 'group-C'];
$messagesPerGroup = 5;

foreach ($messageGroups as $groupIndex => $messageGroup) {
    echo "  → Sending {$messagesPerGroup} messages to message group: {$messageGroup}\n";
    
    for ($i = 1; $i <= $messagesPerGroup; $i++) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Message #{$i} from {$messageGroup}")
            ->setTag("fifo-tag")
            ->setKeys(["key-{$messageGroup}-{$i}"])
            ->setMessageGroup($messageGroup)
            ->build();
        
        $receipt = $producer->send($message);
        echo "    Sent: {$receipt->getMessageId()} (Group: {$messageGroup}, Order: {$i})\n";
    }
    echo "\n";
}

$producer->shutdown();
echo "✓ All FIFO messages sent successfully\n\n";

// Step 2: Consume FIFO messages using PushConsumer
echo "[Step 2] Starting PushConsumer to consume FIFO messages...\n";

$receivedMessages = [];
$consumedCount = 0;
$maxWaitTime = 30; // seconds
$startTime = time();

$consumer = PushConsumer::getInstance($config, $consumerGroup, $topic);

// Configure FIFO consumption
$consumer->setEnableFifoConsumeAccelerator(true);
$consumer->setConsumptionThreadCount(10);
$consumer->setMaxCacheMessageCount(512);

$consumer->setMessageListener(function($messageView) use (&$receivedMessages, &$consumedCount) {
    $messageId = $messageView->getMessageId();
    $body = $messageView->getBody();
    $messageGroup = $messageView->getMessageGroup() ?? 'N/A';
    
    if (!isset($receivedMessages[$messageGroup])) {
        $receivedMessages[$messageGroup] = [];
    }
    
    $receivedMessages[$messageGroup][] = [
        'messageId' => $messageId,
        'body' => $body,
        'timestamp' => microtime(true),
    ];
    
    $consumedCount++;
    
    echo "[Consumed] Group: {$messageGroup}, Order: #" . count($receivedMessages[$messageGroup]) . ", Body: {$body}\n";
    
    return ConsumeResult::SUCCESS;
});

echo "Consumer started, waiting for messages...\n\n";

// Run consumer in a Swoole coroutine
go(function() use ($consumer, $maxWaitTime, $startTime, &$consumedCount, $messagesPerGroup, $messageGroups) {
    $expectedTotal = $messagesPerGroup * count($messageGroups);
    
    // Start the consumer (it will run in its own coroutine internally)
    $consumer->start();
});

// Wait for all coroutines to finish
\Swoole\Event::wait();

// Step 3: Verify FIFO order
echo "\n[Step 3] Verifying FIFO order...\n";

$fifoOrderCorrect = true;
foreach ($messageGroups as $messageGroup) {
    if (!isset($receivedMessages[$messageGroup])) {
        echo "  ✗ No messages received for group: {$messageGroup}\n";
        $fifoOrderCorrect = false;
        continue;
    }
    
    $messages = $receivedMessages[$messageGroup];
    echo "  → Checking group: {$messageGroup} ({$messagesPerGroup} messages)\n";
    
    // Verify that messages are in order by checking the sequence number in body
    for ($i = 0; $i < count($messages); $i++) {
        $expectedOrder = $i + 1;
        $body = $messages[$i]['body'];
        
        // Extract order number from body: "Message #X from group-Y"
        if (preg_match('/Message #(\d+) from (.+)/', $body, $matches)) {
            $actualOrder = intval($matches[1]);
            $actualGroup = $matches[2];
            
            if ($actualOrder !== $expectedOrder) {
                echo "    ✗ Order mismatch at position {$i}: expected #{$expectedOrder}, got #{$actualOrder}\n";
                $fifoOrderCorrect = false;
            } else {
                echo "    ✓ Position {$i}: Message #{$actualOrder} (correct order)\n";
            }
        }
    }
}

echo "\n";
if ($fifoOrderCorrect) {
    echo "✅ FIFO order verification PASSED\n";
} else {
    echo "❌ FIFO order verification FAILED\n";
}

echo "\n=== Test Complete ===\n";
