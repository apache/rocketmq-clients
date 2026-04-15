<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Credentials;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Logger;

/**
 * Producer Recall Message Example
 * 
 * This example demonstrates how to:
 * 1. Send delay messages
 * 2. Recall (cancel) delay messages before delivery
 * 3. Monitor recall metrics
 * 
 * Use cases:
 * - Cancel scheduled notifications when user takes action
 * - Withdraw payment reminders after successful payment
 * - Cancel order confirmations when order is cancelled
 */

// Configure endpoints and credentials
// IMPORTANT: Never hardcode credentials in source code!
// Use environment variables or a secure vault service.
$endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8081';
$accessKey = getenv('ROCKETMQ_ACCESS_KEY') ?: '';
$secretKey = getenv('ROCKETMQ_SECRET_KEY') ?: '';
$topic = 'DelayMessageTopic';

// Create client configuration
$config = new ClientConfiguration($endpoints);
if (!empty($accessKey) && !empty($secretKey)) {
    $credentials = new Credentials($accessKey, $secretKey);
    $config->withCredentials($credentials);
}

// Create and start producer (topic parameter is recommended for better performance)
$producer = Producer::getInstance($config, $topic);

try {
    // Start the producer
    $producer->start();
    Logger::info("Producer started successfully, clientId={}", [$producer->getClientId()]);
    
    // =========================================================================
    // Example 1: Send delay message and recall it
    // =========================================================================
    Logger::info("=== Example 1: Basic recall ===");
    
    // Send a delay message (will be delivered after 60 seconds)
    $messageBody = "This is a scheduled notification that will be recalled";
    $delaySeconds = 60; // 60 seconds
    
    Logger::info("Sending delay message, delay={}s", [$delaySeconds]);
    
    $receipt = $producer->sendDelayMessage(
        $messageBody,
        $delaySeconds,
        "NOTIFICATION",
        "MSG_001"
    );
    
    Logger::info("Delay message sent successfully, messageId={}, recallHandle={}", [
        $receipt->getMessageId(),
        $receipt->getRecallHandle()
    ]);
    
    // Save recall handle for later use
    $recallHandle = $receipt->getRecallHandle();
    
    // Simulate user action that requires cancelling the notification
    Logger::info("Simulating user action... waiting 5 seconds");
    sleep(5);
    
    // Recall the message before it's delivered
    Logger::info("Attempting to recall message, recallHandle={}", [$recallHandle]);
    
    try {
        $recallReceipt = $producer->recallMessage('DelayMessageTopic', $recallHandle);
        
        Logger::info("Message recalled successfully, recalledMessageId={}", [
            $recallReceipt->getMessageId()
        ]);
        
    } catch (\Exception $e) {
        Logger::error("Failed to recall message, error={}", [$e->getMessage()]);
    }
    
    // =========================================================================
    // Example 2: Order cancellation scenario
    // =========================================================================
    Logger::info("\n=== Example 2: Order cancellation scenario ===");
    
    class OrderNotificationService {
        private $producer;
        private $scheduledNotifications = [];
        
        public function __construct($producer) {
            $this->producer = $producer;
        }
        
        /**
         * Schedule order confirmation notification
         */
        public function scheduleOrderConfirmation($orderId, $delaySeconds = 3600) {
            $messageBody = "Order #{$orderId} confirmation - Thank you for your purchase!";
            
            Logger::info("Scheduling order confirmation, orderId={}, delay={}s", [
                $orderId,
                $delaySeconds
            ]);
            
            $receipt = $this->producer->sendDelayMessage(
                $messageBody,
                $delaySeconds,
                "ORDER_CONFIRMATION",
                "ORDER_" . $orderId
            );
            
            // Store recall handle
            $this->scheduledNotifications[$orderId] = [
                'recallHandle' => $receipt->getRecallHandle(),
                'messageId' => $receipt->getMessageId(),
                'scheduledTime' => time(),
                'deliveryTime' => time() + $delaySeconds
            ];
            
            Logger::info("Order confirmation scheduled, orderId={}, messageId={}, recallHandle={}", [
                $orderId,
                $receipt->getMessageId(),
                $receipt->getRecallHandle()
            ]);
            
            return $receipt->getMessageId();
        }
        
        /**
         * Cancel order confirmation (when order is cancelled)
         */
        public function cancelOrderConfirmation($orderId) {
            if (!isset($this->scheduledNotifications[$orderId])) {
                Logger::warn("No scheduled notification found for order, orderId={}", [$orderId]);
                return false;
            }
            
            $notification = $this->scheduledNotifications[$orderId];
            $recallHandle = $notification['recallHandle'];
            
            Logger::info("Cancelling order confirmation, orderId={}, recallHandle={}", [
                $orderId,
                $recallHandle
            ]);
            
            try {
                $recallReceipt = $this->producer->recallMessage(
                    'DelayMessageTopic',
                    $recallHandle
                );
                
                // Remove from scheduled notifications
                unset($this->scheduledNotifications[$orderId]);
                
                Logger::info("Order confirmation cancelled successfully, orderId={}, recalledMessageId={}", [
                    $orderId,
                    $recallReceipt->getMessageId()
                ]);
                
                return true;
                
            } catch (\Exception $e) {
                Logger::error("Failed to cancel order confirmation, orderId={}, error={}", [
                    $orderId,
                    $e->getMessage()
                ]);
                return false;
            }
        }
        
        /**
         * Get scheduled notifications count
         */
        public function getScheduledCount() {
            return count($this->scheduledNotifications);
        }
    }
    
    // Create order notification service
    $orderService = new OrderNotificationService($producer);
    
    // Schedule multiple order confirmations
    $orderService->scheduleOrderConfirmation('ORD_001', 3600); // 1 hour
    $orderService->scheduleOrderConfirmation('ORD_002', 7200); // 2 hours
    $orderService->scheduleOrderConfirmation('ORD_003', 1800); // 30 minutes
    
    Logger::info("Scheduled notifications count: {}", [$orderService->getScheduledCount()]);
    
    // Simulate order cancellation
    Logger::info("Simulating order cancellation for ORD_002...");
    $orderService->cancelOrderConfirmation('ORD_002');
    
    Logger::info("Remaining scheduled notifications: {}", [$orderService->getScheduledCount()]);
    
    // =========================================================================
    // Example 3: Batch recall
    // =========================================================================
    Logger::info("\n=== Example 3: Batch recall ===");
    
    class BatchRecallManager {
        private $producer;
        
        public function __construct($producer) {
            $this->producer = $producer;
        }
        
        /**
         * Batch recall messages
         * 
         * @param string $topic Topic name
         * @param array $recallHandles Array of recall handles
         * @return array Result summary
         */
        public function batchRecall($topic, $recallHandles) {
            $successCount = 0;
            $failureCount = 0;
            $failures = [];
            $totalStartTime = microtime(true);
            
            Logger::info("Starting batch recall, total={}, topic={}", [
                count($recallHandles),
                $topic
            ]);
            
            foreach ($recallHandles as $index => $handle) {
                $startTime = microtime(true);
                
                try {
                    $receipt = $this->producer->recallMessage($topic, $handle);
                    $costTime = (microtime(true) - $startTime) * 1000;
                    
                    $successCount++;
                    
                    Logger::debug("Recalled message {}/{}, messageId={}, costTime={}ms", [
                        $index + 1,
                        count($recallHandles),
                        $receipt->getMessageId(),
                        round($costTime, 2)
                    ]);
                    
                } catch (\Exception $e) {
                    $costTime = (microtime(true) - $startTime) * 1000;
                    $failureCount++;
                    
                    $failures[] = [
                        'index' => $index,
                        'recallHandle' => $handle,
                        'error' => $e->getMessage(),
                        'costTime' => round($costTime, 2)
                    ];
                    
                    Logger::warn("Failed to recall message {}/{}, handle={}, costTime={}ms, error={}", [
                        $index + 1,
                        count($recallHandles),
                        $handle,
                        round($costTime, 2),
                        $e->getMessage()
                    ]);
                }
            }
            
            $totalCostTime = (microtime(true) - $totalStartTime) * 1000;
            
            Logger::info("Batch recall completed, total={}, success={}, failure={}, totalCostTime={}ms", [
                count($recallHandles),
                $successCount,
                $failureCount,
                round($totalCostTime, 2)
            ]);
            
            return [
                'total' => count($recallHandles),
                'success' => $successCount,
                'failure' => $failureCount,
                'failures' => $failures,
                'totalCostTime' => round($totalCostTime, 2)
            ];
        }
    }
    
    // Create batch recall manager
    $batchManager = new BatchRecallManager($producer);
    
    // Prepare recall handles for batch operation
    $recallHandles = [];
    for ($i = 1; $i <= 5; $i++) {
        $receipt = $producer->sendDelayMessage(
            "Batch test message {$i}",
            300, // 5 minutes
            "BATCH_TEST",
            "BATCH_MSG_" . $i
        );
        $recallHandles[] = $receipt->getRecallHandle();
        
        Logger::info("Sent delay message {}/5, messageId={}, recallHandle={}", [
            $i,
            $receipt->getMessageId(),
            $receipt->getRecallHandle()
        ]);
    }
    
    // Perform batch recall
    $result = $batchManager->batchRecall('DelayMessageTopic', $recallHandles);
    
    Logger::info("Batch recall result: success={}, failure={}, avgCostTime={}ms", [
        $result['success'],
        $result['failure'],
        $result['total'] > 0 ? round($result['totalCostTime'] / $result['total'], 2) : 0
    ]);
    
    // =========================================================================
    // Example 4: Monitor recall metrics
    // =========================================================================
    Logger::info("\n=== Example 4: Monitor recall metrics ===");
    
    // Get recall metrics
    $recallQps = $producer->calculateRecallQps('DelayMessageTopic');
    $recallSuccessRate = $producer->getRecallSuccessRate('DelayMessageTopic');
    $avgRecallCostTime = $producer->getAvgRecallCostTime('DelayMessageTopic');
    
    Logger::info("Recall metrics for DelayMessageTopic:");
    Logger::info("  - Recall QPS: {} recalls/sec", [$recallQps !== null ? round($recallQps, 2) : 'N/A']);
    Logger::info("  - Success Rate: {}%", [
        $recallSuccessRate !== null ? round($recallSuccessRate * 100, 2) : 'N/A'
    ]);
    Logger::info("  - Avg Cost Time: {} ms", [
        $avgRecallCostTime !== null ? round($avgRecallCostTime, 2) : 'N/A'
    ]);
    
    // Export all metrics to Prometheus format
    Logger::info("\nPrometheus metrics:");
    echo $producer->exportMetricsToPrometheus();
    
    // Export metrics to JSON format
    Logger::info("\nJSON metrics:");
    echo json_encode(json_decode($producer->exportMetricsToJson()), JSON_PRETTY_PRINT) . "\n";
    
    // =========================================================================
    // Example 5: Error handling scenarios
    // =========================================================================
    Logger::info("\n=== Example 5: Error handling scenarios ===");
    
    // Scenario 1: Invalid recall handle
    Logger::info("Scenario 1: Invalid recall handle");
    try {
        $producer->recallMessage('DelayMessageTopic', 'INVALID_HANDLE');
    } catch (\Exception $e) {
        Logger::error("Expected error: {}", [$e->getMessage()]);
    }
    
    // Scenario 2: Empty recall handle
    Logger::info("\nScenario 2: Empty recall handle");
    try {
        $producer->recallMessage('DelayMessageTopic', '');
    } catch (\InvalidArgumentException $e) {
        Logger::error("Expected validation error: {}", [$e->getMessage()]);
    }
    
    // Scenario 3: Recall already delivered message (simulate)
    Logger::info("\nScenario 3: Attempting to recall after delivery time");
    $receipt = $producer->sendDelayMessage(
        "Short delay message",
        2, // 2 seconds
        "SHORT_DELAY",
        "SHORT_MSG"
    );
    
    Logger::info("Sent short delay message, waiting for delivery...");
    sleep(3); // Wait for message to be delivered
    
    try {
        $producer->recallMessage('DelayMessageTopic', $receipt->getRecallHandle());
        Logger::info("Recall succeeded (message might still be in queue)");
    } catch (\Exception $e) {
        Logger::error("Recall failed (expected): {}", [$e->getMessage()]);
    }
    
    Logger::info("\n=== All examples completed ===");
    
} catch (\Exception $e) {
    Logger::error("Fatal error: {}", [$e->getMessage()]);
    echo $e->getTraceAsString() . "\n";
} finally {
    // Shutdown the producer gracefully
    if (isset($producer)) {
        Logger::info("Shutting down producer...");
        $producer->shutdown();
        Logger::info("Producer shutdown completed");
    }
}
