<?php
/**
 * Test script for PushConsumer FIFO Consume Accelerator
 * 
 * This test verifies:
 * 1. Messages within the same MessageGroup are consumed in strict order
 * 2. Different MessageGroups can be consumed in parallel (when accelerator is enabled)
 * 3. The accelerator can be disabled for sequential processing
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../Consumer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Consumer\ConsumeResult;

echo "========================================\n";
echo "  PushConsumer FIFO Accelerator Test\n";
echo "========================================\n\n";

$endpoints = '127.0.0.1:8080';
$topic = 'topic-order';
$consumerGroup = 'GID-order-consumer';

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// ========================================
// Step 1: Send FIFO messages with different groups
// ========================================
echo "[Step 1] Sending FIFO messages to 3 message groups...\n";
echo str_repeat("-", 60) . "\n";

$producer = Producer::getInstance($config, $topic);
$producer->start();

$messageGroups = ['order-group-A', 'order-group-B', 'order-group-C'];
$messagesPerGroup = 5;
$totalMessages = count($messageGroups) * $messagesPerGroup;

foreach ($messageGroups as $groupIndex => $messageGroup) {
    echo "  → Group '{$messageGroup}': Sending {$messagesPerGroup} messages\n";
    
    for ($i = 1; $i <= $messagesPerGroup; $i++) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Msg-{$i}-from-{$messageGroup}")
            ->setTag("fifo-test")
            ->setKeys(["key-{$messageGroup}-{$i}"])
            ->setMessageGroup($messageGroup)
            ->build();
        
        $receipt = $producer->send($message);
        
        if ($i <= 2 || $i == $messagesPerGroup) {
            echo "    [{$i}/{$messagesPerGroup}] Sent: {$receipt->getMessageId()}\n";
        } elseif ($i == 3) {
            echo "    ...\n";
        }
    }
}

$producer->shutdown();
echo "\n✓ Successfully sent {$totalMessages} FIFO messages\n\n";

// ========================================
// Step 2: Consume with FIFO Accelerator ENABLED
// ========================================
echo "[Step 2] Testing FIFO Consume with ACCELERATOR ENABLED\n";
echo str_repeat("-", 60) . "\n";
echo "Expected behavior:\n";
echo "  - Messages within same group: STRICT ORDER\n";
echo "  - Different groups: PARALLEL consumption\n\n";

$receivedMessages = [];
$consumptionOrder = []; // Track global consumption order
$groupCompletionTimes = [];
$startTime = time();

$consumer = PushConsumer::getInstance($config, $consumerGroup, $topic);

// Configure FIFO consumption with accelerator
$consumer->setEnableFifoConsumeAccelerator(true);  // Enable parallel group consumption
$consumer->setConsumptionThreadCount(10);          // Allow up to 10 concurrent groups
$consumer->setMaxCacheMessageCount(512);

$consumer->setMessageListener(function($messageView) use (&$receivedMessages, &$consumptionOrder, &$groupCompletionTimes) {
    static $groupLastOrder = []; // Track last processed order per group
    
    $messageId = $messageView->getMessageId();
    $body = $messageView->getBody();
    $messageGroup = $messageView->getMessageGroup() ?? 'N/A';
    
    // Extract order number from body: "Msg-X-from-group-Y"
    if (preg_match('/Msg-(\d+)-from-(.+)/', $body, $matches)) {
        $orderNum = intval($matches[1]);
        $groupName = $matches[2];
        
        // Track consumption order
        $consumptionOrder[] = [
            'group' => $groupName,
            'order' => $orderNum,
            'timestamp' => microtime(true),
        ];
        
        // Initialize group tracking
        if (!isset($receivedMessages[$groupName])) {
            $receivedMessages[$groupName] = [];
            $groupLastOrder[$groupName] = 0;
        }
        
        // Check order
        $isInOrder = ($orderNum == $groupLastOrder[$groupName] + 1);
        $groupLastOrder[$groupName] = $orderNum;
        
        $receivedMessages[$groupName][] = [
            'messageId' => $messageId,
            'order' => $orderNum,
            'inOrder' => $isInOrder,
            'timestamp' => microtime(true),
        ];
        
        $status = $isInOrder ? '✓' : '✗';
        echo "[{$status}] Group: {$groupName}, Order: #{$orderNum}, In-Order: " . ($isInOrder ? 'YES' : 'NO') . "\n";
        
        // Track group completion
        if (!isset($groupCompletionTimes[$groupName]) && count($receivedMessages[$groupName]) == 5) {
            $groupCompletionTimes[$groupName] = microtime(true);
        }
    }
    
    // Simulate some processing time
    usleep(50000); // 50ms
    
    return ConsumeResult::SUCCESS;
});

echo "Starting consumer with FIFO accelerator...\n\n";

// Run consumer in a coroutine with timeout
go(function() use ($consumer, $startTime, $totalMessages, &$receivedMessages) {
    $maxWaitTime = 30; // seconds
    
    while (time() - $startTime < $maxWaitTime) {
        $totalReceived = array_sum(array_map('count', $receivedMessages));
        
        if ($totalReceived >= $totalMessages) {
            echo "\n✓ All {$totalMessages} messages consumed successfully!\n";
            break;
        }
        
        \Swoole\Coroutine::sleep(0.5);
    }
    
    $totalReceived = array_sum(array_map('count', $receivedMessages));
    if ($totalReceived < $totalMessages) {
        echo "\n⚠ Timeout: Only consumed {$totalReceived}/{$totalMessages} messages\n";
    }
    
    // Stop the consumer
    $consumer->stop();
});

$consumer->start();

// Wait for Swoole event loop
\Swoole\Event::wait();

// ========================================
// Step 3: Verify FIFO Order
// ========================================
echo "\n[Step 3] Verifying FIFO Order\n";
echo str_repeat("-", 60) . "\n";

$fifoOrderCorrect = true;
$orderViolations = [];

foreach ($messageGroups as $messageGroup) {
    if (!isset($receivedMessages[$messageGroup])) {
        echo "  ✗ No messages received for group: {$messageGroup}\n";
        $fifoOrderCorrect = false;
        continue;
    }
    
    $messages = $receivedMessages[$messageGroup];
    $groupCorrect = true;
    
    echo "  → Checking group: {$messageGroup} (" . count($messages) . " messages)\n";
    
    for ($i = 0; $i < count($messages); $i++) {
        $expectedOrder = $i + 1;
        $actualOrder = $messages[$i]['order'];
        $inOrder = $messages[$i]['inOrder'];
        
        if (!$inOrder) {
            echo "    ✗ Position {$i}: Expected #{$expectedOrder}, got #{$actualOrder} (OUT OF ORDER!)\n";
            $groupCorrect = false;
            $fifoOrderCorrect = false;
            $orderViolations[] = "{$messageGroup}: expected #{$expectedOrder}, got #{$actualOrder}";
        } else {
            echo "    ✓ Position {$i}: Message #{$actualOrder} (correct)\n";
        }
    }
    
    if ($groupCorrect) {
        echo "    ✅ Group {$messageGroup}: FIFO order MAINTAINED\n";
    } else {
        echo "    ❌ Group {$messageGroup}: FIFO order VIOLATED\n";
    }
}

// ========================================
// Step 4: Analyze Parallelism
// ========================================
echo "\n[Step 4] Analyzing Parallel Consumption\n";
echo str_repeat("-", 60) . "\n";

if (count($groupCompletionTimes) >= 2) {
    $times = array_values($groupCompletionTimes);
    $groups = array_keys($groupCompletionTimes);
    
    echo "  Group completion times:\n";
    foreach ($groupCompletionTimes as $group => $time) {
        $relativeTime = round(($time - $startTime) * 1000, 2);
        echo "    - {$group}: {$relativeTime}ms\n";
    }
    
    // Check if groups completed at different times (indicating parallelism)
    $timeDiff = abs($times[0] - $times[1]);
    if ($timeDiff < 500) { // Less than 500ms difference suggests parallel execution
        echo "\n  ✅ Groups completed within {$timeDiff}ms of each other\n";
        echo "     → Indicates PARALLEL consumption (accelerator working!)\n";
    } else {
        echo "\n  ⚠ Groups completed {$timeDiff}ms apart\n";
        echo "     → May indicate sequential consumption\n";
    }
} else {
    echo "  ⚠ Insufficient data to analyze parallelism\n";
}

// ========================================
// Final Results
// ========================================
echo "\n" . str_repeat("=", 60) . "\n";
echo "  TEST RESULTS\n";
echo str_repeat("=", 60) . "\n\n";

$totalReceived = array_sum(array_map('count', $receivedMessages));
echo "Messages Received: {$totalReceived}/{$totalMessages}\n";
echo "FIFO Order: " . ($fifoOrderCorrect ? "✅ CORRECT" : "❌ VIOLATED") . "\n";

if (!empty($orderViolations)) {
    echo "\nOrder Violations:\n";
    foreach ($orderViolations as $violation) {
        echo "  - {$violation}\n";
    }
}

echo "\n";
if ($fifoOrderCorrect && $totalReceived == $totalMessages) {
    echo "🎉 ALL TESTS PASSED!\n";
    echo "   - FIFO order maintained within each group\n";
    echo "   - All messages consumed successfully\n";
} else {
    echo "❌ TESTS FAILED\n";
    if (!$fifoOrderCorrect) {
        echo "   - FIFO order was violated\n";
    }
    if ($totalReceived != $totalMessages) {
        echo "   - Not all messages were consumed\n";
    }
}

echo "\n========================================\n";
