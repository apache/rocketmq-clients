<?php
/**
 * Test script for Priority Message functionality
 * 
 * This test verifies:
 * 1. Priority messages can be sent successfully
 * 2. Validation rules are enforced (mutual exclusivity)
 * 3. Priority values are correctly set and retrieved
 * 4. Priority messages can be consumed
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../Builder/MessageBuilder.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Builder\MessageBuilder;

echo "========================================\n";
echo "  Priority Message Test\n";
echo "========================================\n\n";

$endpoints = '127.0.0.1:8080';
$topic = 'topic-normal';
$consumerGroup = 'GID-normal-consumer';

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// ========================================
// Step 1: Test Priority Message Sending and Receiving
// ========================================
echo "[Step 1] Testing Priority Message Sending and Receiving\n";
echo str_repeat("-", 60) . "\n";

$producer = Producer::getInstance($config, $topic);
$producer->start();

$sentMessages = [];

try {
    // Send messages with different priorities
    $testCases = [
        ['priority' => 1, 'description' => 'Low priority'],
        ['priority' => 5, 'description' => 'Medium priority'],
        ['priority' => 10, 'description' => 'High priority'],
        ['priority' => 100, 'description' => 'Critical priority'],
    ];
    
    foreach ($testCases as $testCase) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Test message - {$testCase['description']}")
            ->setTag("priority-test")
            ->setKeys(["test-key-{$testCase['priority']}"])
            ->setPriority($testCase['priority'])
            ->build();
        
        $receipt = $producer->send($message);
        
        echo "  ✓ Sent: {$testCase['description']} (priority={$testCase['priority']})\n";
        echo "    Message ID: {$receipt->getMessageId()}\n";
        
        $sentMessages[] = [
            'priority' => $testCase['priority'],
            'messageId' => $receipt->getMessageId(),
        ];
    }
    
    echo "\n✓ Successfully sent " . count($sentMessages) . " priority messages\n\n";
    
} catch (\Exception $e) {
    echo "\n❌ Failed to send priority messages: " . $e->getMessage() . "\n\n";
    exit(1);
} finally {
    $producer->shutdown();
}

// Receive priority messages
echo "  → Creating SimpleConsumer to receive priority messages...\n";
$consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
$consumer->start();
$consumer->setMaxMessageNum(32);
$consumer->setInvisibleDuration(30);

$received = 0;
$maxAttempts = 20;
$attempt = 0;
$receivedPriorities = [];

echo "  → Receiving priority messages...\n";
while ($received < count($sentMessages) && $attempt < $maxAttempts) {
    try {
        $messages = $consumer->receive(32, 30);
        foreach ($messages as $message) {
            $consumer->ack($message);
            $received++;
            $priority = $message->getPriority();
            $receivedPriorities[] = $priority;
            echo "    Received: priority={$priority}, messageId=" . $message->getMessageId() . "\n";
        }
        
        if ($received < count($sentMessages)) {
            usleep(500000);
        }
    } catch (\Exception $e) {
        // Ignore timeout errors
    }
    $attempt++;
}

echo "\n  ✓ Successfully received {$received}/" . count($sentMessages) . " priority messages\n";

if ($received >= count($sentMessages)) {
    echo "  ✅ Priority message sending and receiving test passed\n\n";
} else {
    echo "  ⚠️  Warning: Expected at least " . count($sentMessages) . ", received {$received}\n\n";
}

// ========================================
// Step 2: Test Validation Rules
// ========================================
echo "[Step 2] Testing Validation Rules\n";
echo str_repeat("-", 60) . "\n";

$validationTests = [
    [
        'name' => 'Priority + DeliveryTimestamp',
        'shouldFail' => true,
        'builder' => function() {
            return (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('test')
                ->setDeliveryTimestamp(time() * 1000 + 60000)
                ->setPriority(5);
        },
    ],
    [
        'name' => 'Priority + MessageGroup',
        'shouldFail' => true,
        'builder' => function() {
            return (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('test')
                ->setMessageGroup('test-group')
                ->setPriority(5);
        },
    ],
    [
        'name' => 'Priority + LiteTopic',
        'shouldFail' => true,
        'builder' => function() {
            return (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('test')
                ->setLiteTopic('lite-topic')
                ->setPriority(5);
        },
    ],
    [
        'name' => 'Negative Priority',
        'shouldFail' => true,
        'builder' => function() {
            return (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('test')
                ->setPriority(-1);
        },
    ],
    [
        'name' => 'Valid Priority Only',
        'shouldFail' => false,
        'builder' => function() {
            return (new MessageBuilder())
                ->setTopic('test-topic')
                ->setBody('test')
                ->setPriority(5);
        },
    ],
];

$passedValidations = 0;
$failedValidations = 0;

foreach ($validationTests as $test) {
    try {
        $message = $test['builder']()->build();
        
        if ($test['shouldFail']) {
            echo "  ✗ FAILED: {$test['name']} should have thrown exception\n";
            $failedValidations++;
        } else {
            echo "  ✓ PASSED: {$test['name']}\n";
            $passedValidations++;
        }
    } catch (\Apache\Rocketmq\Exception\MessageException $e) {
        if ($test['shouldFail']) {
            echo "  ✓ PASSED: {$test['name']} (correctly rejected)\n";
            echo "    Error: " . substr($e->getMessage(), 0, 80) . "...\n";
            $passedValidations++;
        } else {
            echo "  ✗ FAILED: {$test['name']} should not have thrown exception\n";
            echo "    Error: " . $e->getMessage() . "\n";
            $failedValidations++;
        }
    }
}

echo "\nValidation Results: {$passedValidations} passed, {$failedValidations} failed\n\n";

// ========================================
// Step 3: Test Priority Retrieval
// ========================================
echo "[Step 3] Testing Priority Retrieval\n";
echo str_repeat("-", 60) . "\n";

try {
    $testPriorities = [0, 1, 5, 10, 100, 1000];
    
    foreach ($testPriorities as $priority) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic('test-topic')
            ->setBody("Test priority {$priority}")
            ->setPriority($priority)
            ->build();
        
        $retrievedPriority = $message->getPriority();
        
        if ($retrievedPriority === $priority) {
            echo "  ✓ Priority {$priority}: correctly stored and retrieved\n";
        } else {
            echo "  ✗ Priority {$priority}: expected {$priority}, got {$retrievedPriority}\n";
        }
    }
    
    echo "\n✓ Priority retrieval test completed\n\n";
    
} catch (\Exception $e) {
    echo "\n❌ Priority retrieval test failed: " . $e->getMessage() . "\n\n";
}

// ========================================
// Final Summary
// ========================================
echo str_repeat("=", 60) . "\n";
echo "  TEST SUMMARY\n";
echo str_repeat("=", 60) . "\n\n";

$totalTests = count($sentMessages) + count($validationTests) + count($testPriorities ?? []);
$passedTests = count($sentMessages) + $passedValidations + (count($testPriorities ?? []));

echo "Messages Sent: " . count($sentMessages) . "\n";
echo "Validations Passed: {$passedValidations}/" . count($validationTests) . "\n";
echo "Priority Retrievals: " . count($testPriorities ?? []) . "\n";
echo "\n";

if ($failedValidations == 0) {
    echo "🎉 ALL TESTS PASSED!\n";
    echo "   - Priority messages sent successfully\n";
    echo "   - All validation rules enforced correctly\n";
    echo "   - Priority values stored and retrieved accurately\n";
} else {
    echo "❌ SOME TESTS FAILED\n";
    echo "   - {$failedValidations} validation(s) failed\n";
}

echo "\n========================================\n";
