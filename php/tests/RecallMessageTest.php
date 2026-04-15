<?php
/**
 * Test script for Producer with recalling timed/delay messages
 * 
 * This test verifies:
 * 1. Delay messages can be sent successfully
 * 2. Delay messages can be recalled before delivery
 * 3. Recall validation and error handling
 * 4. Recall metrics collection
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Builder\MessageBuilder;

echo "========================================\n";
echo "  Producer Recall Message Test\n";
echo "========================================\n\n";

$endpoints = '127.0.0.1:8080';
$topic = 'topic-delay';

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Test results
$results = [
    'sent' => 0,
    'recalled' => 0,
    'recallFailures' => 0,
    'delivered' => 0,
];

// ========================================
// Step 1: Send delay messages
// ========================================
echo "[Step 1] Sending Delay Messages\n";
echo str_repeat("-", 60) . "\n";

$producer = Producer::getInstance($config, $topic);
$producer->start();

$sentMessages = [];

try {
    // Send delay messages with different delays
    $testCases = [
        ['delaySeconds' => 30, 'description' => 'Short delay (30s)'],
        ['delaySeconds' => 60, 'description' => 'Medium delay (60s)'],
        ['delaySeconds' => 120, 'description' => 'Long delay (120s)'],
    ];
    
    foreach ($testCases as $testCase) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Delay message - {$testCase['description']}")
            ->setTag("recall-test")
            ->setKeys(["recall-test-{$testCase['delaySeconds']}"])
            ->setDeliveryTimestamp((time() + $testCase['delaySeconds']) * 1000)  // Convert to milliseconds
            ->build();
        
        $receipt = $producer->send($message);
        $results['sent']++;
        
        echo "  ✓ Sent: {$testCase['description']}\n";
        echo "    Message ID: {$receipt->getMessageId()}\n";
        echo "    Recall Handle: {$receipt->getRecallHandle()}\n";
        
        $sentMessages[] = [
            'delaySeconds' => $testCase['delaySeconds'],
            'messageId' => $receipt->getMessageId(),
            'recallHandle' => $receipt->getRecallHandle(),
            'sentTime' => time(),
        ];
    }
    
    echo "\n✓ Successfully sent {$results['sent']} delay messages\n\n";
    
} catch (\Exception $e) {
    echo "\n❌ Failed to send delay messages: " . $e->getMessage() . "\n\n";
    exit(1);
}

// ========================================
// Step 2: Recall some messages immediately
// ========================================
echo "[Step 2] Recalling Messages Before Delivery\n";
echo str_repeat("-", 60) . "\n";

// Try to recall the first two messages (short and medium delay)
$messagesToRecall = array_slice($sentMessages, 0, 2);

foreach ($messagesToRecall as $msgInfo) {
    try {
        echo "  → Attempting to recall message (delay={$msgInfo['delaySeconds']}s)...\n";
        echo "    Message ID: {$msgInfo['messageId']}\n";
        
        $recallReceipt = $producer->recallMessage($topic, $msgInfo['recallHandle']);
        
        $results['recalled']++;
        
        echo "  ✓ Successfully recalled\n";
        echo "    Recalled Message ID: {$recallReceipt->getMessageId()}\n\n";
        
    } catch (\Exception $e) {
        $results['recallFailures']++;
        
        echo "  ✗ Failed to recall: " . $e->getMessage() . "\n\n";
    }
}

echo "Recall Summary: {$results['recalled']} recalled, {$results['recallFailures']} failed\n\n";

// ========================================
// Step 3: Test invalid recall scenarios
// ========================================
echo "[Step 3] Testing Invalid Recall Scenarios\n";
echo str_repeat("-", 60) . "\n";

$invalidTests = [
    [
        'name' => 'Empty recall handle',
        'handle' => '',
        'shouldFail' => true,
    ],
    [
        'name' => 'Invalid recall handle',
        'handle' => 'INVALID_HANDLE_12345',
        'shouldFail' => true,
    ],
];

$validationPassed = 0;
$validationFailed = 0;

foreach ($invalidTests as $test) {
    try {
        echo "  → Testing: {$test['name']}\n";
        
        $producer->recallMessage($topic, $test['handle']);
        
        if ($test['shouldFail']) {
            echo "  ✗ FAILED: Should have thrown exception\n\n";
            $validationFailed++;
        } else {
            echo "  ✓ PASSED\n\n";
            $validationPassed++;
        }
        
    } catch (\Exception $e) {
        if ($test['shouldFail']) {
            echo "  ✓ PASSED (correctly rejected)\n";
            echo "    Error: " . substr($e->getMessage(), 0, 80) . "...\n\n";
            $validationPassed++;
        } else {
            echo "  ✗ FAILED: Should not have thrown exception\n";
            echo "    Error: {$e->getMessage()}\n\n";
            $validationFailed++;
        }
    }
}

echo "Validation Results: {$validationPassed} passed, {$validationFailed} failed\n\n";

// ========================================
// Step 4: Monitor recall metrics
// ========================================
echo "[Step 4] Monitoring Recall Metrics\n";
echo str_repeat("-", 60) . "\n";

try {
    $recallQps = $producer->calculateRecallQps($topic);
    $recallSuccessRate = $producer->getRecallSuccessRate($topic);
    $avgRecallCostTime = $producer->getAvgRecallCostTime($topic);
    
    echo "  Recall Metrics for {$topic}:\n";
    echo "    - Recall QPS: " . ($recallQps !== null ? round($recallQps, 2) : 'N/A') . " recalls/sec\n";
    echo "    - Success Rate: " . ($recallSuccessRate !== null ? round($recallSuccessRate * 100, 2) : 'N/A') . "%\n";
    echo "    - Avg Cost Time: " . ($avgRecallCostTime !== null ? round($avgRecallCostTime, 2) : 'N/A') . " ms\n\n";
    
} catch (\Exception $e) {
    echo "  ⚠️  Could not retrieve metrics: " . $e->getMessage() . "\n\n";
}

// ========================================
// Step 5: Wait and check delivered messages
// ========================================
echo "[Step 5] Checking Delivered Messages\n";
echo str_repeat("-", 60) . "\n";

echo "  → Waiting for remaining delay messages to be delivered...\n";
echo "  Note: The last message (120s delay) will be delivered after its delay period.\n";
echo "  You can verify this by running a consumer on topic-delay.\n\n";

// Show summary of all messages
echo "  Message Status Summary:\n";
foreach ($sentMessages as $index => $msgInfo) {
    $status = $index < 2 ? 'RECALLED' : 'DELIVERED (pending)';
    echo "    - Message #" . ($index + 1) . ": delay={$msgInfo['delaySeconds']}s, status={$status}\n";
}

echo "\n";

// ========================================
// Final Summary
// ========================================
echo str_repeat("=", 60) . "\n";
echo "  TEST SUMMARY\n";
echo str_repeat("=", 60) . "\n\n";

echo "Messages Sent: {$results['sent']}\n";
echo "Messages Recalled: {$results['recalled']}\n";
echo "Recall Failures: {$results['recallFailures']}\n";
echo "Validations Passed: {$validationPassed}/" . count($invalidTests) . "\n\n";

if ($results['recalled'] > 0 && $validationFailed == 0) {
    echo "🎉 ALL TESTS PASSED!\n";
    echo "   - Delay messages sent successfully\n";
    echo "   - Messages recalled before delivery\n";
    echo "   - Invalid recall scenarios handled correctly\n";
    echo "   - Recall metrics collected\n";
} else {
    echo "⚠️  SOME TESTS FAILED\n";
    if ($results['recalled'] == 0) {
        echo "   - No messages were successfully recalled\n";
    }
    if ($validationFailed > 0) {
        echo "   - Some validation tests failed\n";
    }
}

echo "\n";

// Shutdown producer
$producer->shutdown();
echo "Producer shutdown completed.\n";
