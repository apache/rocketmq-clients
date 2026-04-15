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

/**
 * Producer Transaction Message Example
 * 
 * This example demonstrates how to send transactional messages using RocketMQ PHP SDK.
 * Transactional messages ensure consistency between local transactions and message sending.
 */

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../Producer.php';
require_once __DIR__ . '/../ProducerSingleton.php';
require_once __DIR__ . '/../Transaction.php';

use Apache\Rocketmq\Transaction;
use Apache\Rocketmq\Producer\TransactionChecker;
use Apache\Rocketmq\Producer\TransactionResolution;
use Apache\Rocketmq\Message\MessageView;

// Configuration
$topic = 'topic-php-transcation';

echo "=== Producer Transaction Message Example ===\n\n";

try {
    // Create transaction checker (optional, for checking orphaned transactions)
    $transactionChecker = new class implements TransactionChecker {
        public function check(MessageView $messageView): TransactionResolution {
            echo "  [Transaction Checker] Checking transaction status for message: " . $messageView->getMessageId() . "\n";
            // Return COMMIT, ROLLBACK, or UNKNOWN based on your business logic
            return TransactionResolution::COMMIT;
        }
    };
    
    // Get transactional producer instance using singleton pattern (recommended)
    $producer = ProducerSingleton::getTransactionalInstance($transactionChecker, $topic);
    echo "✓ Transactional Producer initialized (singleton)\n";
    echo "  - Topic: {$topic}\n\n";
    
    // Send multiple transaction messages
    for ($i = 1; $i <= 3; $i++) {
        echo "Sending transaction message #{$i}...\n";
        
        $body = "Transaction message #{$i} - Order created";
        $tag = 'transaction-tag';
        $keys = 'txn-key-' . $i;
        
        // Begin transaction (like Java: producer.beginTransaction())
        $transaction = $producer->beginTransaction();
        
        try {
            // Build message
            $msgBuilder = new \Apache\Rocketmq\Builder\MessageBuilder();
            $message = $msgBuilder
                ->setTopic($topic)
                ->setBody($body)
                ->setTag($tag)
                ->setKeys([$keys])
                ->build();
            
            // Send half message with transaction (like Java: producer.send(message, transaction))
            echo "  - Sending half message...\n";
            $receipts = $producer->sendTransactionMessage($body, $transaction, $tag, $keys);
            $receipt = $receipts[0];  // Get first receipt
            echo "  - Half message sent: {$receipt->getMessageId()}\n";
            
            // Simulate local transaction execution
            echo "  - Executing local transaction...\n";
            usleep(100000); // Simulate processing time
            
            // Commit transaction after local transaction succeeds
            echo "  - Committing transaction...\n";
            $transaction->commit();
            echo "  ✓ Transaction committed successfully\n\n";
            
        } catch (\Exception $e) {
            echo "  ✗ Error: " . $e->getMessage() . "\n";
            echo "  - Rolling back transaction...\n";
            try {
                $transaction->rollback();
                echo "  ✓ Transaction rolled back\n\n";
            } catch (\Exception $rollbackError) {
                echo "  ✗ Failed to rollback: " . $rollbackError->getMessage() . "\n\n";
            }
        }
        
        // Small delay between messages
        usleep(500000); // 0.5 seconds
    }
    
    // Note: Transactional producer will be reused across multiple calls (singleton pattern)
    
} catch (\Exception $e) {
    echo "✗ Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
    exit(1);
}

echo "\n=== Example Complete ===\n";
echo "\nNote: Transactional messages will be consumed only after being committed.\n";
echo "Use TransactionConsumerExample.php to consume these messages.\n";
