<?php
/**
 * SimpleConsumer Message Send/Receive Test
 *
 * Tests:
 * 1. Start SimpleConsumer to subscribe topic-normal
 * 2. Send messages using Producer
 * 3. Pull and consume messages via SimpleConsumer.receive()
 * 4. ACK messages
 *
 * Configuration:
 * - Topic: topic-normal
 * - Consumer Group: GID-normal-consumer
 */

if (!extension_loaded('swoole')) {
    echo "Swoole extension is required for this test\n";
    exit(1);
}

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../PushConsumer.php';
require_once __DIR__ . '/../ProducerSingleton.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'topic-normal';
$consumerGroup = 'GID-normal-consumer';

echo "========================================\n";
echo "  SimpleConsumer Message Test\n";
echo "========================================\n\n";
echo "Configuration:\n";
echo "  - Topic: {$topic}\n";
echo "  - Consumer Group: {$consumerGroup}\n";
echo "  - Endpoints: {$endpoints}\n\n";

go(function () use ($topic, $consumerGroup, $endpoints) {

    try {
        // ========================================
        // Step 1: Start SimpleConsumer
        // ========================================
        echo "[Step 1] Starting SimpleConsumer...\n";
        echo str_repeat("-", 60) . "\n";

        $config = new ClientConfiguration($endpoints);
        $config->withSslEnabled(false);

        $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
        $consumer->start();
        echo "  SimpleConsumer started\n";
        
        // Subscribe to topic (required after start())
        $filterExpression = new FilterExpression('*', FilterExpressionType::TAG);
        $consumer->subscribe($topic, $filterExpression);
        echo "  Subscribed to topic: {$topic}\n\n";

        // ========================================
        // Step 2: Send test messages
        // ========================================
        echo "[Step 2] Sending test messages...\n";
        echo str_repeat("-", 60) . "\n";

        $producer = ProducerSingleton::getInstance($topic);

        $testMessages = [
            ['body' => 'SimpleConsumer test #1', 'tag' => 'sc-tag-1', 'keys' => ['sc-key-001']],
            ['body' => 'SimpleConsumer test #2', 'tag' => 'sc-tag-2', 'keys' => ['sc-key-002']],
            ['body' => 'SimpleConsumer test #3', 'tag' => 'sc-tag-3', 'keys' => ['sc-key-003']],
        ];

        $sentCount = 0;
        foreach ($testMessages as $index => $msgData) {
            try {
                $msgBuilder = new MessageBuilder();
                $message = $msgBuilder
                    ->setTopic($topic)
                    ->setBody($msgData['body'])
                    ->setTag($msgData['tag'])
                    ->setKeys(implode(',', $msgData['keys']))
                    ->build();

                $receipt = $producer->send($message);
                $sentCount++;
                echo "  Sent #{$sentCount}: {$msgData['body']} (ID: {$receipt->getMessageId()})\n";
                usleep(200000); // 200ms
            } catch (\Exception $e) {
                echo "  Failed to send #" . ($index + 1) . ": " . $e->getMessage() . "\n";
            }
        }
        echo "\n  Total sent: {$sentCount} messages\n\n";

        // Wait for messages to be available on broker
        \Swoole\Coroutine::sleep(2);

        // ========================================
        // Step 3: Receive messages
        // ========================================
        echo "[Step 3] Receiving messages via SimpleConsumer.receive()...\n";
        echo str_repeat("-", 60) . "\n";

        $receivedCount = 0;
        $ackedCount = 0;
        $maxAttempts = 5;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            echo "  Attempt #{$attempt}: pulling messages...\n";

            try {
                $messages = $consumer->receive(32, 15);

                if (empty($messages)) {
                    echo "    No messages received, waiting...\n";
                    \Swoole\Coroutine::sleep(2);
                    continue;
                }

                echo "    Got " . count($messages) . " messages\n";

                foreach ($messages as $message) {
                    $receivedCount++;
                    $sysProps = $message->getSystemProperties();
                    $msgId = $sysProps->getMessageId();
                    $body = $message->getBody();
                    if (!is_string($body)) {
                        $body = method_exists($body, 'toString') ? $body->toString() : (string)$body;
                    }

                    echo "    [{$receivedCount}] ID: {$msgId}\n";
                    echo "         Body: {$body}\n";

                    // ACK message
                    try {
                        $consumer->ack($message);
                        $ackedCount++;
                        echo "         ACK: OK\n";
                    } catch (\Exception $e) {
                        echo "         ACK: FAILED - " . $e->getMessage() . "\n";
                    }
                }

                // If we got enough messages, stop early
                if ($receivedCount >= $sentCount) {
                    break;
                }
            } catch (\Exception $e) {
                echo "    Error: " . $e->getMessage() . "\n";
                \Swoole\Coroutine::sleep(1);
            }
        }

        echo "\n";

        // ========================================
        // Step 4: Results
        // ========================================
        echo "[Step 4] Results\n";
        echo str_repeat("-", 60) . "\n";
        echo "  Sent:     {$sentCount}\n";
        echo "  Received: {$receivedCount}\n";
        echo "  ACKed:    {$ackedCount}\n\n";

        if ($receivedCount >= $sentCount) {
            echo "TEST PASSED - SimpleConsumer received messages successfully!\n";
        } elseif ($receivedCount > 0) {
            echo "TEST PARTIAL - Received some messages ({$receivedCount}/{$sentCount})\n";
        } else {
            echo "TEST FAILED - No messages received\n";
        }

        echo "\n";

        // Cleanup
        echo "[Cleanup] Shutting down...\n";
        $consumer->close();
        echo "  SimpleConsumer shutdown\n";

    } catch (\Exception $e) {
        echo "\nERROR: " . $e->getMessage() . "\n";
        echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
        exit(1);
    }

    echo "\n========================================\n";
    echo "  TEST COMPLETE\n";
    echo "========================================\n";
});
