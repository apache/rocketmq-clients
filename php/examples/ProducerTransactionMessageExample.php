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
require_once __DIR__ . '/../ProducerOptimized.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\ProducerOptimized;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

$endpoints = '127.0.0.1:8081';
$topic = 'yourTransactionTopic';

$producer = new ProducerOptimized($endpoints, [
    'topics' => [$topic],
    'maxAttempts' => 3,
    'requestTimeout' => 3000,
]);

$producer->start();

$topicResource = new Resource();
$topicResource->setName($topic);

$sysProps = new SystemProperties();
$sysProps->setTag('yourMessageTagA');
$sysProps->setKeys(['yourMessageKey-565ef26f5727']);

$message = new Message();
$message->setTopic($topicResource);
$message->setBody('This is a transaction message for Apache RocketMQ');
$message->setSystemProperties($sysProps);

$transaction = $producer->beginTransaction();

try {
    $result = $producer->sendWithTransaction($message, $transaction);
    echo "Send transaction message successfully, messageId=" . $result['messageId'] . "\n";
} catch (\Throwable $e) {
    echo "Failed to send message: " . $e->getMessage() . "\n";
    $producer->shutdown();
    exit(1);
}

// Commit the transaction.
$transaction->commit();
// Or rollback the transaction.
// $transaction->rollback();

$producer->shutdown();
