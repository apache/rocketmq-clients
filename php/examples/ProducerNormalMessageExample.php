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
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\Producer;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;

$endpoints = '127.0.0.1:8081';
$topic = 'yourNormalTopic';

$producer = new Producer($endpoints, [
    'topics' => [$topic],
    'maxAttempts' => 3,
    'requestTimeout' => 3000,
]);

$producer->start();

$topicResource = new Resource();
$topicResource->setName($topic);

$sysProps = new SystemProperties();
$sysProps->setTag('yourMessageTagA');
$sysProps->setKeys(['yourMessageKey-1c151062f96e']);

$message = new Message();
$message->setTopic($topicResource);
$message->setBody('This is a normal message for Apache RocketMQ');
$message->setSystemProperties($sysProps);

try {
    $result = $producer->send($message);
    echo "Send message successfully, messageId=" . $result['messageId'] . "\n";
} catch (\Throwable $e) {
    echo "Failed to send message: " . $e->getMessage() . "\n";
}

$producer->shutdown();
