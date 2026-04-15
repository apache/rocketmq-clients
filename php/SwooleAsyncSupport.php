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

namespace Apache\Rocketmq;

use Apache\Rocketmq\Message\MessageInterface;
use Apache\Rocketmq\Producer\SendReceipt;

/**
 * Swoole async support for RocketMQ PHP SDK
 * 
 * Provides coroutine-based asynchronous operations when running in Swoole environment.
 * This significantly improves performance by utilizing Swoole's non-blocking I/O.
 * 
 * Features:
 * - Coroutine-based message sending
 * - Async metrics export
 * - Non-blocking telemetry communication
 * - Connection pool management
 * 
 * Requirements:
 * - Swoole extension >= 4.5
 * - Enable coroutines: swoole.use_shortname = 'On'
 * 
 * @example
 * ```php
 * // Check if Swoole is available
 * if (SwooleAsyncSupport::isAvailable()) {
 *     // Use coroutine-based async send
 *     go(function() use ($producer, $message) {
 *         try {
 *             $receipt = SwooleAsyncSupport::sendAsync($producer, $message);
 *             echo "Message sent: " . $receipt->getMessageId();
 *         } catch (\Exception $e) {
 *             echo "Send failed: " . $e->getMessage();
 *         }
 *     });
 * }
 * ```
 */
class SwooleAsyncSupport {
    /**
     * @var bool Whether Swoole extension is loaded
     */
    private static $swooleLoaded = null;
    
    /**
     * @var bool Whether coroutines are enabled
     */
    private static $coroutinesEnabled = null;
    
    /**
     * Check if Swoole extension is available
     * 
     * @return bool True if Swoole is loaded
     */
    public static function isAvailable(): bool {
        if (self::$swooleLoaded === null) {
            self::$swooleLoaded = extension_loaded('swoole');
        }
        return self::$swooleLoaded;
    }
    
    /**
     * Check if coroutines are enabled
     * 
     * @return bool True if coroutines can be used
     */
    public static function isCoroutineEnabled(): bool {
        if (!self::isAvailable()) {
            return false;
        }
        
        if (self::$coroutinesEnabled === null) {
            self::$coroutinesEnabled = class_exists('\\Swoole\\Coroutine');
        }
        
        return self::$coroutinesEnabled;
    }
    
    /**
     * Send message asynchronously using coroutine
     * 
     * This method wraps the synchronous send operation in a coroutine,
     * allowing it to run concurrently with other operations.
     * 
     * @param object $producer Producer instance
     * @param MessageInterface $message Message to send
     * @return SendReceipt Send receipt
     * @throws \Exception If send fails
     */
    public static function sendAsync($producer, MessageInterface $message): SendReceipt {
        if (!self::isCoroutineEnabled()) {
            throw new \RuntimeException("Swoole coroutines are not available");
        }
        
        // Create channel for result
        $channel = new \Swoole\Coroutine\Channel(1);
        
        // Run send in coroutine
        go(function() use ($producer, $message, $channel) {
            try {
                $receipt = $producer->send($message);
                $channel->push(['success' => true, 'receipt' => $receipt]);
            } catch (\Exception $e) {
                $channel->push(['success' => false, 'error' => $e]);
            }
        });
        
        // Wait for result with timeout
        $result = $channel->pop(30); // 30 seconds timeout
        
        if ($result === false) {
            throw new \Exception("Send operation timed out");
        }
        
        if (!$result['success']) {
            throw $result['error'];
        }
        
        return $result['receipt'];
    }
    
    /**
     * Send multiple messages concurrently using coroutines
     * 
     * Sends all messages in parallel using separate coroutines,
     * significantly improving throughput.
     * 
     * @param object $producer Producer instance
     * @param MessageInterface[] $messages Messages to send
     * @param int $concurrency Maximum concurrent sends (default: 10)
     * @return array Array of ['receipt' => SendReceipt, 'error' => Exception|null]
     */
    public static function sendBatchConcurrent($producer, array $messages, int $concurrency = 10): array {
        if (!self::isCoroutineEnabled()) {
            throw new \RuntimeException("Swoole coroutines are not available");
        }
        
        $results = [];
        $semaphore = new \Swoole\Coroutine\Semaphore($concurrency);
        $channel = new \Swoole\Coroutine\Channel(count($messages));
        
        foreach ($messages as $index => $message) {
            go(function() use ($producer, $message, $index, $semaphore, $channel) {
                $semaphore->pop(); // Acquire semaphore
                
                try {
                    $receipt = $producer->send($message);
                    $channel->push([
                        'index' => $index,
                        'success' => true,
                        'receipt' => $receipt,
                        'error' => null
                    ]);
                } catch (\Exception $e) {
                    $channel->push([
                        'index' => $index,
                        'success' => false,
                        'receipt' => null,
                        'error' => $e
                    ]);
                } finally {
                    $semaphore->push(); // Release semaphore
                }
            });
        }
        
        // Collect results
        for ($i = 0; $i < count($messages); $i++) {
            $result = $channel->pop(30);
            if ($result !== false) {
                $results[$result['index']] = [
                    'receipt' => $result['receipt'],
                    'error' => $result['error']
                ];
            }
        }
        
        // Sort by index
        ksort($results);
        
        return $results;
    }
    
    /**
     * Export metrics asynchronously
     * 
     * Runs metrics export in background coroutine without blocking main flow.
     * 
     * @param ClientMeterManager $meterManager Meter manager instance
     * @return void
     */
    public static function exportMetricsAsync(ClientMeterManager $meterManager): void {
        if (!self::isCoroutineEnabled()) {
            return;
        }
        
        go(function() use ($meterManager) {
            try {
                $metrics = $meterManager->exportMetrics();
                Logger::debug("Async metrics export completed, count={}", [count($metrics)]);
            } catch (\Exception $e) {
                Logger::error("Async metrics export failed, error={}", [$e->getMessage()]);
            }
        });
    }
    
    /**
     * Start periodic metrics exporter
     * 
     * Creates a background coroutine that exports metrics at regular intervals.
     * 
     * @param ClientMeterManager $meterManager Meter manager instance
     * @param int $intervalSeconds Export interval in seconds
     * @return callable Cancel function to stop the periodic export
     */
    public static function startPeriodicMetricsExport(
        ClientMeterManager $meterManager,
        int $intervalSeconds = 60
    ): callable {
        if (!self::isCoroutineEnabled()) {
            return function() {};
        }
        
        $running = true;
        
        go(function() use ($meterManager, $intervalSeconds, &$running) {
            Logger::info("Started periodic metrics export, interval={}s", [$intervalSeconds]);
            
            while ($running) {
                try {
                    // Sleep for interval
                    \Swoole\Coroutine::sleep($intervalSeconds);
                    
                    if (!$running) {
                        break;
                    }
                    
                    // Update gauges and export
                    $meterManager->updateGauges();
                    $metrics = $meterManager->exportMetrics();
                    
                    Logger::debug("Periodic metrics exported, count={}", [count($metrics)]);
                    
                } catch (\Exception $e) {
                    Logger::error("Error in periodic metrics export, error={}", [$e->getMessage()]);
                }
            }
            
            Logger::info("Periodic metrics export stopped");
        });
        
        // Return cancel function
        return function() use (&$running) {
            $running = false;
        };
    }
    
    /**
     * Run multiple tasks concurrently
     * 
     * Generic utility for running any callable functions concurrently.
     * 
     * @param callable[] $tasks Array of tasks to run
     * @param int $timeout Timeout in seconds
     * @return array Results array
     */
    public static function runConcurrent(array $tasks, int $timeout = 30): array {
        if (!self::isCoroutineEnabled()) {
            throw new \RuntimeException("Swoole coroutines are not available");
        }
        
        $channel = new \Swoole\Coroutine\Channel(count($tasks));
        
        foreach ($tasks as $index => $task) {
            go(function() use ($task, $index, $channel) {
                try {
                    $result = call_user_func($task);
                    $channel->push([
                        'index' => $index,
                        'success' => true,
                        'result' => $result,
                        'error' => null
                    ]);
                } catch (\Exception $e) {
                    $channel->push([
                        'index' => $index,
                        'success' => false,
                        'result' => null,
                        'error' => $e
                    ]);
                }
            });
        }
        
        $results = [];
        for ($i = 0; $i < count($tasks); $i++) {
            $result = $channel->pop($timeout);
            if ($result !== false) {
                $results[$result['index']] = $result;
            }
        }
        
        ksort($results);
        return $results;
    }
    
    /**
     * Create async consumer message handler
     * 
     * Wraps message listener in coroutine for async processing.
     * 
     * @param callable $listener Original message listener
     * @param int $timeout Processing timeout in seconds
     * @return callable Wrapped listener
     */
    public static function createAsyncMessageListener(callable $listener, int $timeout = 30): callable {
        if (!self::isCoroutineEnabled()) {
            return $listener;
        }
        
        return function($message) use ($listener, $timeout) {
            $channel = new \Swoole\Coroutine\Channel(1);
            
            go(function() use ($listener, $message, $channel) {
                try {
                    $result = call_user_func($listener, $message);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Exception $e) {
                    $channel->push(['success' => false, 'error' => $e]);
                }
            });
            
            $result = $channel->pop($timeout);
            
            if ($result === false) {
                throw new \Exception("Message processing timed out after {$timeout}s");
            }
            
            if (!$result['success']) {
                throw $result['error'];
            }
            
            return $result['result'];
        };
    }
    
    /**
     * Get Swoole version
     * 
     * @return string|null Swoole version or null if not loaded
     */
    public static function getSwooleVersion(): ?string {
        if (!self::isAvailable()) {
            return null;
        }
        
        return phpversion('swoole');
    }
    
    /**
     * Check Swoole requirements
     * 
     * @return array ['available' => bool, 'version' => string|null, 'issues' => string[]]
     */
    public static function checkRequirements(): array {
        $issues = [];
        
        if (!self::isAvailable()) {
            $issues[] = "Swoole extension is not loaded";
            return [
                'available' => false,
                'version' => null,
                'issues' => $issues
            ];
        }
        
        $version = self::getSwooleVersion();
        
        if (version_compare($version, '4.5.0', '<')) {
            $issues[] = "Swoole version {$version} is too old, require >= 4.5.0";
        }
        
        if (!self::isCoroutineEnabled()) {
            $issues[] = "Swoole coroutines are not available";
        }
        
        return [
            'available' => empty($issues),
            'version' => $version,
            'issues' => $issues
        ];
    }
}
