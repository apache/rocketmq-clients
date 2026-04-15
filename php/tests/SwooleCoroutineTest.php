#!/usr/bin/env php
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
 * Swoole Coroutine Example
 * 
 * Demonstrates high-performance message processing using Swoole coroutines.
 * This example shows how to leverage Swoole's async capabilities for:
 * - Concurrent message sending
 * - Async metrics export
 * - Non-blocking operations
 * 
 * Requirements:
 * - Swoole extension >= 4.5
 * - Enable: swoole.use_shortname = 'On'
 * 
 * Usage:
 * ```bash
 * php examples/SwooleCoroutineExample.php
 * ```
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Message\MessageBuilder;
use Apache\Rocketmq\Producer\Producer;
use Apache\Rocketmq\SwooleAsyncSupport;

// Check Swoole availability
$requirements = SwooleAsyncSupport::checkRequirements();
if (!$requirements['available']) {
    echo "Error: Swoole requirements not met\n";
    foreach ($requirements['issues'] as $issue) {
        echo "  - {$issue}\n";
    }
    exit(1);
}

echo "=== Swoole Coroutine Example ===\n";
echo "Swoole version: " . $requirements['version'] . "\n\n";

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'test-topic';
$clientId = 'swoole-example-' . getmypid();

$config = new ClientConfiguration($endpoints);
$producer = Producer::getInstance($config, $topic);

// Enable metrics
$producer->enableMetrics();

echo "Starting producer...\n";
$producer->start();
echo "✓ Producer started\n\n";

// ============================================
// Example 1: High-Concurrency Message Sending
// ============================================
echo "Example 1: High-concurrency message sending\n";
echo "Sending 100 messages with concurrency=10...\n\n";

$totalMessages = 100;
$concurrency = 10;

$messages = [];
for ($i = 0; $i < $totalMessages; $i++) {
    $messages[] = MessageBuilder::newMessage()
        ->setTopic($topic)
        ->setBody("Message #{$i}")
        ->setTag('tag-' . ($i % 5))
        ->setKeys(['key-' . $i])
        ->build();
}

$start = microtime(true);
$results = SwooleAsyncSupport::sendBatchConcurrent($producer, $messages, $concurrency);
$duration = round((microtime(true) - $start) * 1000, 2);

$successCount = 0;
$failCount = 0;
foreach ($results as $result) {
    if ($result['error'] === null) {
        $successCount++;
    } else {
        $failCount++;
    }
}

$throughput = round($successCount / ($duration / 1000), 2);

echo "Results:\n";
echo "  Total messages: {$totalMessages}\n";
echo "  Success: {$successCount}\n";
echo "  Failed: {$failCount}\n";
echo "  Duration: {$duration}ms\n";
echo "  Throughput: {$throughput} msg/s\n";
echo "  Concurrency: {$concurrency}\n\n";

// ============================================
// Example 2: Async Metrics Export
// ============================================
echo "Example 2: Asynchronous metrics export\n";

$meterManager = $producer->getMeterManager();

// Simulate some activity first
for ($i = 0; $i < 10; $i++) {
    $msg = MessageBuilder::newMessage()
        ->setTopic($topic)
        ->setBody("Metrics test #{$i}")
        ->build();
    $producer->send($msg);
}

echo "Exporting metrics asynchronously...\n";
$start = microtime(true);

SwooleAsyncSupport::exportMetricsAsync($meterManager);

// Wait for async export to complete
\Swoole\Event::wait();

$duration = round((microtime(true) - $start) * 1000, 2);
echo "✓ Metrics exported in {$duration}ms (non-blocking)\n\n";

// ============================================
// Example 3: Periodic Background Tasks
// ============================================
echo "Example 3: Periodic background tasks\n";

echo "Starting periodic metrics export (every 3 seconds)...\n";
$cancelExport = SwooleAsyncSupport::startPeriodicMetricsExport($meterManager, 3);

// Also start a custom periodic task
$customTaskRunning = true;
go(function() use (&$customTaskRunning) {
    $count = 0;
    while ($customTaskRunning) {
        \Swoole\Coroutine::sleep(2);
        if (!$customTaskRunning) break;
        
        $count++;
        echo "   [Custom Task] Iteration #{$count} at " . date('H:i:s') . "\n";
    }
});

echo "Running for 10 seconds...\n";
usleep(10000000); // 10 seconds

// Stop background tasks
$cancelExport();
$customTaskRunning = false;

echo "✓ Background tasks stopped\n\n";

// ============================================
// Example 4: Custom Concurrent Tasks
// ============================================
echo "Example 4: Custom concurrent tasks\n";

$tasks = [
    function() use ($producer, $topic) {
        // Task 1: Send messages
        $msg = MessageBuilder::newMessage()
            ->setTopic($topic)
            ->setBody('Task 1 message')
            ->build();
        return $producer->send($msg);
    },
    function() use ($meterManager) {
        // Task 2: Export metrics
        return $meterManager->exportMetrics();
    },
    function() {
        // Task 3: Simulate I/O
        \Swoole\Coroutine::sleep(1);
        return ['task' => 3, 'status' => 'completed'];
    },
];

echo "Running 3 tasks concurrently...\n";
$start = microtime(true);
$results = SwooleAsyncSupport::runConcurrent($tasks, 10);
$duration = round((microtime(true) - $start) * 1000, 2);

echo "✓ All tasks completed in {$duration}ms\n";
foreach ($results as $index => $result) {
    if ($result['success']) {
        echo "  Task #" . ($index + 1) . ": SUCCESS\n";
    } else {
        echo "  Task #" . ($index + 1) . ": FAILED - " . $result['error']->getMessage() . "\n";
    }
}
echo "\n";

// ============================================
// Example 5: Async Message Processing
// ============================================
echo "Example 5: Async message processing simulation\n";

// Simulate a message listener with timeout
$asyncListener = SwooleAsyncSupport::createAsyncMessageListener(
    function($message) {
        // Simulate processing
        \Swoole\Coroutine::sleep(0.5);
        return "Processed: " . $message->getBody();
    },
    5 // 5 second timeout
);

echo "Processing message with async listener...\n";
$start = microtime(true);

try {
    $testMessage = MessageBuilder::newMessage()
        ->setTopic($topic)
        ->setBody('Test message')
        ->build();
    
    $result = $asyncListener($testMessage);
    $duration = round((microtime(true) - $start) * 1000, 2);
    
    echo "✓ {$result} ({$duration}ms)\n";
} catch (\Exception $e) {
    echo "✗ Processing failed: " . $e->getMessage() . "\n";
}
echo "\n";

// ============================================
// Example 6: Performance Benchmark
// ============================================
echo "Example 6: Performance benchmark\n";

$benchSizes = [10, 50, 100];
$concurrencyLevels = [1, 5, 10];

echo "Testing different batch sizes and concurrency levels:\n";
printf("%-12s %-15s %-15s %-15s\n", "Batch Size", "Concurrency", "Duration (ms)", "Throughput");
echo str_repeat("-", 60) . "\n";

foreach ($benchSizes as $size) {
    foreach ($concurrencyLevels as $concurrency) {
        $messages = [];
        for ($i = 0; $i < $size; $i++) {
            $messages[] = MessageBuilder::newMessage()
                ->setTopic($topic)
                ->setBody("Benchmark #{$i}")
                ->build();
        }
        
        $start = microtime(true);
        $results = SwooleAsyncSupport::sendBatchConcurrent($producer, $messages, $concurrency);
        $duration = round((microtime(true) - $start) * 1000, 2);
        
        $successCount = count(array_filter($results, function($r) {
            return $r['error'] === null;
        }));
        
        $throughput = round($successCount / ($duration / 1000), 0);
        
        printf("%-12d %-15d %-15.2f %-15d\n", $size, $concurrency, $duration, $throughput);
    }
}
echo "\n";

// ============================================
// Cleanup
// ============================================
echo "Shutting down...\n";
$producer->shutdown();
echo "✓ Producer shutdown complete\n\n";

echo "=== Swoole Coroutine Example Complete ===\n";
echo "\nKey Takeaways:\n";
echo "1. Use coroutines for high-concurrency scenarios\n";
echo "2. Leverage sendBatchConcurrent for batch operations\n";
echo "3. Use async metrics export to avoid blocking\n";
echo "4. Start periodic tasks for background operations\n";
echo "5. Monitor performance and adjust concurrency levels\n";
