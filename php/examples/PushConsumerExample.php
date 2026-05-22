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
require_once __DIR__ . '/../PushConsumer.php';
require_once __DIR__ . '/../ConsumeResult.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\ConsumeResult;

// Load configuration
$config = ExampleConfig::getInstance();
$endpoints = $config->getEndpoints();
$consumerGroup = $config->getConsumerGroup();
$topic = $config->getTopic('normal');
$credentials = $config->getCredentials();

// Display configuration
$config->display();

$consumer = new PushConsumer($endpoints, $consumerGroup, [
    'subscriptionExpressions' => [$topic => '*'],
    'credentials' => $credentials,
    'messageListener' => function($messageView) {
        $body = $messageView->getBody() ?? '';
        echo "Consume message: " . $body . "\n";
        return ConsumeResult::SUCCESS;
    },
    'scanIntervalSeconds' => 5,
]);

$consumer->start();

// Block the main thread (not needed in production).
while (true) {
    pcntl_signal_dispatch();
    sleep(1);
}

// Close the push consumer when you don't need it anymore.
// $consumer->shutdown();
