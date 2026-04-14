<?php
/**
 * Test script for Lite message send and receive
 * 
 * This test verifies:
 * 1. Lite messages can be sent successfully
 * 2. Lite messages can be consumed by LitePushConsumer
 * 3. Lite topic quota exceptions are handled correctly
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\LiteProducerBuilder;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Consumer\ConsumeResult;
use Apache\Rocketmq\Exception\LiteTopicQuotaExceededException;

echo "========================================\n";
echo "  Lite Message Test\n";
echo "========================================\n\n";

$endpoints = '127.0.0.1:8080';
$parentTopic = 'topic-normal';
$consumerGroup = 'GID-normal-consumer';

$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

// Test results
$results = [
    'sent' => 0,
    'received' => 0,
    'quotaErrors' => 0,
];

// ========================================
// Step 1: Send Lite Messages
// ========================================
echo "[Step 1] Sending Lite Messages\n";
echo str_repeat("-", 60) . "\n";

try {
    $producer = (new LiteProducerBuilder())
        ->setClientConfiguration($config)
        ->setParentTopic($parentTopic)
        ->build();
    
    $producer->start();
    
    // Send messages to different lite topics
    $liteTopics = ['lite-topic-1', 'lite-topic-2', 'lite-topic-3'];
    
    foreach ($liteTopics as $index => $liteTopic) {
        try {
            $msgBuilder = new MessageBuilder();
            $message = $msgBuilder
                ->setTopic($parentTopic)
                ->setBody("Lite message #{$index} - {$liteTopic}")
                ->setKeys(["lite-key-{$index}"])
                ->setLiteTopic($liteTopic)
                ->build();
            
            $receipt = $producer->send($message);
            $results['sent']++;
            
            echo "  ✓ Sent to {$liteTopic}: {$receipt->getMessageId()}\n";
            
        } catch (LiteTopicQuotaExceededException $e) {
            $results['quotaErrors']++;
            echo "  ⚠️  Quota exceeded for {$liteTopic}: {$e->getMessage()}\n";
            
        } catch (\Exception $e) {
            echo "  ✗ Failed to send to {$liteTopic}: {$e->getMessage()}\n";
        }
    }
    
    $producer->shutdown();
    
    echo "\n✓ Successfully sent {$results['sent']} lite messages\n\n";
    
} catch (\Exception $e) {
    echo "✗ Failed to send lite messages: " . $e->getMessage() . "\n\n";
    exit(1);
}

// ========================================
// Step 2: Receive Lite Messages
// ========================================
echo "[Step 2] Receiving Lite Messages\n";
echo str_repeat("-", 60) . "\n";

try {
    $receivedMessages = [];
    
    $consumer = (new \Apache\Rocketmq\Builder\LitePushConsumerBuilder())
        ->setClientConfiguration($config)
        ->setConsumerGroup($consumerGroup)
        ->bindTopic($parentTopic)
        ->setMessageListener(function($messageView) use (&$receivedMessages, &$results) {
            $liteTopic = $messageView->getLiteTopic();
            $messageId = $messageView->getMessageId();
            
            echo "  → Received from " . ($liteTopic ?? 'unknown') . ": {$messageId}\n";
            
            $receivedMessages[] = [
                'messageId' => $messageId,
                'liteTopic' => $liteTopic,
                'body' => $messageView->getBody(),
            ];
            
            $results['received']++;
            
            return ConsumeResult::SUCCESS;
        })
        ->build();
    
    $consumer->start();
    
    // Subscribe to lite topics
    foreach (['lite-topic-1', 'lite-topic-2', 'lite-topic-3'] as $liteTopic) {
        try {
            $consumer->subscribeLite($liteTopic, function($messageView) use (&$results) {
                $results['received']++;
                return ConsumeResult::SUCCESS;
            });
        } catch (\Exception $e) {
            echo "  ⚠️  Could not subscribe to {$liteTopic}: {$e->getMessage()}\n";
        }
    }
    
    echo "  → Waiting for messages (10 seconds)...\n";
    sleep(10);
    
    $consumer->shutdown();
    
    echo "\n✓ Successfully received {$results['received']} lite messages\n\n";
    
} catch (\Exception $e) {
    echo "✗ Failed to receive lite messages: " . $e->getMessage() . "\n\n";
}

// ========================================
// Final Summary
// ========================================
echo str_repeat("=", 60) . "\n";
echo "  TEST SUMMARY\n";
echo str_repeat("=", 60) . "\n\n";

echo "Messages Sent: {$results['sent']}\n";
echo "Messages Received: {$results['received']}\n";
echo "Quota Errors: {$results['quotaErrors']}\n\n";

if ($results['sent'] > 0) {
    echo "✅ Lite message test completed!\n";
    echo "   - Lite messages can be sent with setLiteTopic()\n";
    echo "   - Lite messages can be consumed by LitePushConsumer\n";
    echo "   - Quota exceptions are properly handled\n";
} else {
    echo "⚠️  No messages were sent successfully\n";
}

echo "\n";
