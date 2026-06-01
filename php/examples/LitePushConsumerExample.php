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
require_once __DIR__ . '/../LitePushConsumer.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\LitePushConsumer;
use Apache\Rocketmq\ConsumeResult;
use Apache\Rocketmq\SessionCredentials;

$config = ExampleConfig::getInstance();
$endpoints = $config->getEndpoints();
$consumerGroup = $config->getConsumerGroup();
$parentTopic = $config->getLiteParentTopic();
$credentials = $config->getCredentials();

$consumer = new LitePushConsumer($endpoints, $consumerGroup, $parentTopic, [
    'credentials' => $credentials,
    'enableFifoConsumeAccelerator' => true,
]);

// Subscribe to lite topics
try {
    // subscribeLite() may fail due to network errors or quota issues
    $consumer->subscribeLite('lite-topic-1', function($messageView) {
        $body = $messageView->getBody() ?? '';
        echo "Consume lite-topic-1 message: " . $body . "\n";
        return ConsumeResult::SUCCESS;
    });
    $consumer->subscribeLite('lite-topic-2', function($messageView) {
        $body = $messageView->getBody() ?? '';
        echo "Consume lite-topic-2 message: " . $body . "\n";
        return ConsumeResult::SUCCESS;
    });
    $consumer->subscribeLite('lite-topic-3', function($messageView) {
        $body = $messageView->getBody() ?? '';
        echo "Consume lite-topic-3 message: " . $body . "\n";
        return ConsumeResult::SUCCESS;
    });
} catch (\Exception $e) {
    echo "Failed to subscribe lite topic: " . $e->getMessage() . "\n";
    exit(1);
}

$consumer->start();

// Block the main thread (not needed in production).
while (true) {
    pcntl_signal_dispatch();
    sleep(1);
}

// Close the push consumer when you don't need it anymore.
// $consumer->shutdown();
