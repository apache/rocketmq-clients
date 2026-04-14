<?php
/**
 * Example: Producer with Priority Message
 * 
 * This example demonstrates how to send priority messages using RocketMQ PHP client.
 * Priority messages are delivered based on their priority level - higher priority
 * messages are consumed before lower priority ones.
 * 
 * Key features:
 * - Priority cannot be combined with: deliveryTimestamp, messageGroup, or liteTopic
 * - Higher priority value means higher priority (e.g., priority 10 > priority 1)
 * - Priority is only meaningful when the topic is configured as a priority topic
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../Builder/MessageBuilder.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Builder\MessageBuilder;

echo "========================================\n";
echo "  Priority Message Producer Example\n";
echo "========================================\n\n";

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'yourPriorityTopic'; // Must be a priority-enabled topic

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Create and start producer
$producer = Producer::getInstance($config, $topic);
$producer->start();

echo "Producer started successfully\n\n";

try {
    // ========================================
    // Example 1: Send messages with different priorities
    // ========================================
    echo "[Example 1] Sending messages with different priorities...\n";
    echo str_repeat("-", 60) . "\n";
    
    $priorities = [1, 3, 5, 8, 10]; // Different priority levels
    
    foreach ($priorities as $priority) {
        $msgBuilder = new MessageBuilder();
        $message = $msgBuilder
            ->setTopic($topic)
            ->setBody("Priority message with level {$priority}")
            ->setTag("priority-tag")
            ->setKeys(["priority-key-{$priority}"])
            ->setPriority($priority)  // Set priority level
            ->build();
        
        $receipt = $producer->send($message);
        
        echo "  ✓ Sent message with priority {$priority}\n";
        echo "    Message ID: {$receipt->getMessageId()}\n";
        echo "    Body: Priority message with level {$priority}\n\n";
    }
    
    // ========================================
    // Example 2: Send high-priority urgent message
    // ========================================
    echo "[Example 2] Sending high-priority urgent message...\n";
    echo str_repeat("-", 60) . "\n";
    
    $urgentMsgBuilder = new MessageBuilder();
    $urgentMessage = $urgentMsgBuilder
        ->setTopic($topic)
        ->setBody("URGENT: System alert - immediate attention required!")
        ->setTag("urgent")
        ->setKeys(["urgent-alert-" . time()])
        ->setPriority(100)  // Very high priority
        ->build();
    
    $urgentReceipt = $producer->send($urgentMessage);
    
    echo "  ✓ Sent URGENT message with priority 100\n";
    echo "    Message ID: {$urgentReceipt->getMessageId()}\n";
    echo "    Body: URGENT: System alert - immediate attention required!\n\n";
    
    // ========================================
    // Example 3: Demonstrate validation errors
    // ========================================
    echo "[Example 3] Demonstrating validation (expected errors)...\n";
    echo str_repeat("-", 60) . "\n";
    
    // Test 1: Priority + DeliveryTimestamp (should fail)
    try {
        $invalidMsgBuilder = new MessageBuilder();
        $invalidMessage = $invalidMsgBuilder
            ->setTopic($topic)
            ->setBody("Invalid message")
            ->setDeliveryTimestamp(time() * 1000 + 60000) // 1 minute later
            ->setPriority(5)  // Cannot set both!
            ->build();
        
        echo "  ✗ Should have thrown exception for priority + deliveryTimestamp\n";
    } catch (\Apache\Rocketmq\Exception\MessageException $e) {
        echo "  ✓ Correctly rejected: Priority + DeliveryTimestamp\n";
        echo "    Error: " . $e->getMessage() . "\n\n";
    }
    
    // Test 2: Priority + MessageGroup (should fail)
    try {
        $invalidMsgBuilder = new MessageBuilder();
        $invalidMessage = $invalidMsgBuilder
            ->setTopic($topic)
            ->setBody("Invalid message")
            ->setMessageGroup("order-group")
            ->setPriority(5)  // Cannot set both!
            ->build();
        
        echo "  ✗ Should have thrown exception for priority + messageGroup\n";
    } catch (\Apache\Rocketmq\Exception\MessageException $e) {
        echo "  ✓ Correctly rejected: Priority + MessageGroup\n";
        echo "    Error: " . $e->getMessage() . "\n\n";
    }
    
    // Test 3: Negative priority (should fail)
    try {
        $invalidMsgBuilder = new MessageBuilder();
        $invalidMessage = $invalidMsgBuilder
            ->setTopic($topic)
            ->setBody("Invalid message")
            ->setPriority(-1)  // Negative priority not allowed
            ->build();
        
        echo "  ✗ Should have thrown exception for negative priority\n";
    } catch (\Apache\Rocketmq\Exception\MessageException $e) {
        echo "  ✓ Correctly rejected: Negative priority\n";
        echo "    Error: " . $e->getMessage() . "\n\n";
    }
    
    echo "\n========================================\n";
    echo "  All examples completed successfully!\n";
    echo "========================================\n\n";
    
    echo "Next steps:\n";
    echo "1. Create a consumer to receive these priority messages\n";
    echo "2. Verify that higher priority messages are consumed first\n";
    echo "3. Monitor message ordering in your application logs\n\n";
    
} catch (\Exception $e) {
    echo "\n❌ Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    // Shutdown producer
    $producer->shutdown();
    echo "Producer shutdown complete\n";
}
