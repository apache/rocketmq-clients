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
require_once __DIR__ . '/../SimpleConsumer.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\SimpleConsumer;

// Load configuration
$config = ExampleConfig::getInstance();
$endpoints = $config->getEndpoints();
$consumerGroup = $config->getConsumerGroup();
$topic = $config->getTopic('normal');
$credentials = $config->getCredentials();

// Display configuration
$config->display();

$consumer = new SimpleConsumer($endpoints, $consumerGroup, [
    'credentials' => $credentials,
    'awaitDuration' => 30,
]);

$consumer->start();
$consumer->subscribe($topic);

echo "Simple consumer started. Press Ctrl+C to exit.\n";

// Receive messages in a loop
$maxMessageNum = 16;
$invisibleDuration = 15;

$running = true;
if (function_exists('pcntl_signal')) {
    pcntl_signal(SIGTERM, function () use (&$running) {
        echo "Received SIGTERM, shutting down...\n";
        $running = false;
    });
    pcntl_signal(SIGINT, function () use (&$running) {
        echo "Received SIGINT, shutting down...\n";
        $running = false;
    });
}

while ($running) {
    if (function_exists('pcntl_signal_dispatch')) {
        pcntl_signal_dispatch();
    }

    try {
        $messages = $consumer->receive($maxMessageNum, $invisibleDuration);
        if (empty($messages)) {
            sleep(1);
            continue;
        }

        echo "Received " . count($messages) . " message(s)\n";

        foreach ($messages as $msg) {
            $body = $msg->getBody() ?? '';
            echo "  Received: " . $body . "\n";

            try {
                $consumer->ack($msg);
                echo "  Acknowledged successfully\n";
            } catch (\Throwable $e) {
                echo "  Failed to acknowledge: " . $e->getMessage() . "\n";
            }
        }
    } catch (\Throwable $e) {
        echo "Failed to receive message: " . $e->getMessage() . "\n";
        sleep(1);
    }
}

$consumer->shutdown();
echo "Simple consumer shut down gracefully.\n";
