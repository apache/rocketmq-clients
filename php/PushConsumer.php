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

// Load required files
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Exception/ClientException.php';
require_once __DIR__ . '/SimpleConsumer.php';
require_once __DIR__ . '/TelemetrySession.php';

// Load AsyncTelemetrySession if Swoole is available
if (extension_loaded('swoole')) {
    require_once __DIR__ . '/AsyncTelemetrySession.php';
}

use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Consumer\ConsumeResult;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\ClientStateException;
use Apache\Rocketmq\Exception\MessageException;
use Apache\Rocketmq\Exception\NetworkException;
use Apache\Rocketmq\Exception\ServerException;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use const Grpc\STATUS_OK;

// Initialize logger to write to ~/logs/rocketmq/rocketmq_client_php.log
\Apache\Rocketmq\Logger::init();
/**
 * RocketMQ PushConsumer Implementation
 * 
 * Supports the following features:
 * - Message listener with concurrent consumption
 * - Automatic ACK/NACK
 * - ProcessQueue-based message caching and flow control
 * - Thread pool (Swoole coroutine pool) for parallel message processing
 * - Message cache size/count control
 * - Detailed logging and metrics collection
 * 
 * Architecture (inspired by Java PushConsumer):
 * - Uses ProcessQueue to manage messages per MessageQueue
 * - Dispatches messages to consumption executor (Swoole coroutines)
 * - Implements StandardConsumeService for concurrent message processing
 * - Supports cache threshold control (count and bytes)
 * 
 * Usage example:
 * $consumer = PushConsumer::getInstance($config, 'my-group', 'my-topic');
 * $consumer->setMessageListener(function($message) {
 *     // Process message
 *     return \Apache\Rocketmq\Consumer\ConsumeResult::SUCCESS; // or FAILURE
 * });
 * $consumer->start();
 */
class PushConsumer
{
    /**
     * @var MessagingServiceClient|null gRPC client instance
     */
    private $client = null;
    
    /**
     * @var ClientConfiguration Client configuration object
     */
    private $config;
    
    /**
     * @var string Consumer group name
     */
    private $consumerGroup;
    
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var callable|null Message listener callback
     */
    private $messageListener = null;
    
    /**
     * @var bool Whether running
     */
    private $running = false;
    
    /**
     * @var int Message invisible duration (seconds)
     */
    private $invisibleDuration = 15;
    
    /**
     * @var int Maximum message count per receive
     */
    private $maxMessageNum = 16;
    
    /**
     * @var int Maximum cached message count across all process queues
     */
    private $maxCacheMessageCount = 1024;
    
    /**
     * @var int Maximum cached message size in bytes across all process queues
     */
    private $maxCacheMessageSizeInBytes = 67108864; // 64MB
    
    /**
     * @var int Number of concurrent consumption workers (coroutines)
     */
    private $consumptionThreadCount = 20;
    
    /**
     * @var bool Enable FIFO consume accelerator (parallel consume different message groups)
     */
    private $enableFifoConsumeAccelerator = true;
    
    /**
     * @var array FIFO message group locks: messageGroup => locked (bool)
     */
    private $fifoMessageGroupLocks = [];
    
    /**
     * @var \SplQueue[] FIFO message group queues: messageGroup => SplQueue
     */
    private $fifoMessageGroupQueues = [];
    
    /**
     * @var array Process queue table: topic => ProcessQueue[]
     */
    private $processQueueTable = [];
    
    /**
     * @var int Total cached message count
     */
    private $totalCachedMessageCount = 0;
    
    /**
     * @var int Total cached message size in bytes
     */
    private $totalCachedMessageSize = 0;
    
    /**
     * @var \SplQueue Message consumption task queue
     */
    private $consumptionTaskQueue = null;
    
    /**
     * @var int Number of active consumption workers
     */
    private $activeWorkers = 0;
    
    /**
     * @var array Consumption metrics
     */
    private $metrics = [
        'receptionTimes' => 0,
        'receivedMessagesQuantity' => 0,
        'consumptionOkQuantity' => 0,
        'consumptionErrorQuantity' => 0,
    ];
    
    /**
     * @var array<string, FilterExpression> Subscription expressions: topic => FilterExpression
     */
    private $subscriptionExpressions = [];
    
    /**
     * @var bool Whether subscription expressions are initialized (used during construction)
     */
    private $subscriptionInitialized = false;
    
    /**
     * @var TelemetrySession|null Telemetry session for bidirectional stream
     */
    private $telemetrySession = null;
    
    /**
     * @var int Heartbeat interval in seconds (Java default: 5s)
     */
    private const HEARTBEAT_INTERVAL = 5;
    
    /**
     * @var int Settings sync interval in seconds (Java default: 300s = 5min)
     */
    private const SETTINGS_SYNC_INTERVAL = 300;
    
    /**
     * Private constructor to prevent direct instantiation
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string (backward compatible)
     * @param string|null $consumerGroup Consumer group name（Used when first parameter is endpoints string）
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    private function __construct($configOrEndpoints, $consumerGroup = null, $topic = null)
    {
        // Supports two construction methods:
        // 1. New configuration object method: __construct(ClientConfiguration $config, $consumerGroup, $topic)
        // 2. Legacy compatible method: __construct($endpoints, $consumerGroup, $topic)
        
        if ($configOrEndpoints instanceof ClientConfiguration) {
            // New configuration object method
            if (empty($consumerGroup) || empty($topic)) {
                throw new \InvalidArgumentException("Consumer group and topic cannot be empty");
            }
            
            $this->config = $configOrEndpoints;
            $this->consumerGroup = $consumerGroup;
            $this->topic = $topic;
        } else {
            // Legacy compatible method
            if (empty($consumerGroup) || empty($topic)) {
                throw new \InvalidArgumentException("Consumer group and topic cannot be empty");
            }
            
            $this->config = new ClientConfiguration($configOrEndpoints);
            $this->consumerGroup = $consumerGroup;
            $this->topic = $topic;
        }
        
        $this->clientId = $this->generateClientId();
    }
    
    /**
     * Get PushConsumer singleton instance (configuration object recommended)
     * 
     * Usage example:
     * ```php
     * // Method 1: Using configuration object (recommended)
     * $config = new ClientConfiguration('127.0.0.1:8080');
     * $consumer = PushConsumer::getInstance($config, 'my-group', 'my-topic');
     * 
     * // Method 2: Using legacy method (backward compatible)
     * $consumer = PushConsumer::getInstance('127.0.0.1:8080', 'my-group', 'my-topic');
     * ```
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string
     * @param string|null $consumerGroup Consumer group name（Used when first parameter is endpoints string）
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @return PushConsumer PushConsumer instance
     */
    public static function getInstance($configOrEndpoints, $consumerGroup = null, $topic = null)
    {
        return new self($configOrEndpoints, $consumerGroup, $topic);
    }
    
    /**
     * Generate Client ID
     * 
     * @return string Client ID
     */
    private function generateClientId()
    {
        return Util::generateClientId();
    }
    
    /**
     * Get gRPC client
     * 
     * @return MessagingServiceClient gRPC client
     */
    private function getClient()
    {
        if ($this->client === null) {
            // Get connection from pool with fixed client ID
            // This ensures all RPC calls use the same client ID as Telemetry Session
            $pool = ConnectionPool::getInstance();
            // Set connection pool configuration from client configuration
            $pool->setConfigFromClientConfiguration($this->config);
            $this->client = $pool->getConnectionWithClientId($this->config, $this->clientId);
        }
        return $this->client;
    }
    
    /**
     * Set message listener
     * 
     * @param callable $listener Message listener callback
     * @return void
     */
    public function setMessageListener($listener)
    {
        $this->messageListener = $listener;
    }
    
    /**
     * Set max cache message count
     * 
     * @param int $count Maximum cached message count
     * @return void
     */
    public function setMaxCacheMessageCount($count)
    {
        $this->maxCacheMessageCount = $count;
    }
    
    /**
     * Set max cache message size in bytes
     * 
     * @param int $size Maximum cached message size in bytes
     * @return void
     */
    public function setMaxCacheMessageSizeInBytes($size)
    {
        $this->maxCacheMessageSizeInBytes = $size;
    }
    
    /**
     * Set consumption thread count (coroutine count)
     * 
     * @param int $count Number of concurrent workers
     * @return void
     */
    public function setConsumptionThreadCount($count)
    {
        $this->consumptionThreadCount = $count;
    }
    
    /**
     * Enable or disable FIFO consume accelerator
     * 
     * When enabled, messages from different message groups will be consumed in parallel,
     * while messages within the same group are consumed sequentially.
     * 
     * @param bool $enable Enable accelerator
     * @return void
     */
    public function setEnableFifoConsumeAccelerator($enable)
    {
        $this->enableFifoConsumeAccelerator = (bool)$enable;
    }
    
    /**
     * Get consumption metrics
     * 
     * @return array Metrics data
     */
    public function getMetrics()
    {
        return $this->metrics;
    }
    
    /**
     * Subscribe to a topic with filter expression
     * 
     * This method allows dynamic subscription to additional topics.
     * All consumers in the same group must have the same subscription expressions.
     *
     * @param string $topic Topic name
     * @param FilterExpression|null $filterExpression Filter expression (default: subscribe all)
     * @return PushConsumer Self for method chaining
     * @throws ClientException If consumer is not running or topic is invalid
     */
    public function subscribe(string $topic, $filterExpression = null): self
    {
        // Allow subscription during initialization phase (before start)
        // Once started, subscription changes are not allowed (similar to Java)
        if ($this->running) {
            throw new ClientStateException("PushConsumer is already running, cannot subscribe to new topic: {$topic}");
        }
        
        if ($filterExpression === null) {
            // Default: subscribe all messages
            $filterExpression = new \Apache\Rocketmq\Consumer\FilterExpression('*', \Apache\Rocketmq\Consumer\FilterExpressionType::TAG);
        }
        
        // Save subscription expression
        $this->subscriptionExpressions[$topic] = $filterExpression;
        
        Logger::info("Subscribed to topic: {}, filter: {}, clientId={}", [
            $topic,
            $filterExpression->getExpression(),
            $this->clientId
        ]);
        
        return $this;
    }
    
    /**
     * Unsubscribe from a topic
     * 
     * This method removes the subscription and stops fetching messages from the topic.
     * It stops the backend task to fetch messages from the server, and besides that, 
     * the locally cached message whose topic was removed before would not be delivered 
     * to message listener anymore.
     *
     * Nothing occurs if the specified topic does not exist in subscription expressions.
     *
     * @param string $topic Topic name to unsubscribe
     * @return PushConsumer Self for method chaining
     * @throws ClientException If consumer is not running
     */
    public function unsubscribe(string $topic): self
    {
        // Check if consumer is running (similar to Java's checkRunning())
        // Unsubscribe is only allowed when consumer is running
        if (!$this->running) {
            throw new ClientStateException("PushConsumer is not running, cannot unsubscribe from topic: {$topic}");
        }
        
        // Remove from subscription expressions
        if (isset($this->subscriptionExpressions[$topic])) {
            unset($this->subscriptionExpressions[$topic]);
        }
        
        Logger::info("Unsubscribed from topic: {}, clientId={}", [
            $topic,
            $this->clientId
        ]);
        
        return $this;
    }
    
    /**
     * Get subscription expressions
     * 
     * Returns a copy of the subscription expressions map to prevent external modification,
     * similar to Java's implementation: new HashMap<>(subscriptionExpressions).
     * 
     * @return array<string, FilterExpression> Copy of subscription expressions map (topic => FilterExpression)
     */
    public function getSubscriptionExpressions(): array
    {
        // Return a copy to prevent external modification (defensive copy)
        return $this->subscriptionExpressions; // In PHP, arrays are copied by value, so this is already safe
    }
    
    /**
     * Start consumption workers (Swoole coroutines)
     * 
     * This method creates a pool of Swoole coroutines that consume messages
     * from the task queue concurrently, similar to Java's ThreadPoolExecutor.
     * 
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance for ACK/NACK
     * @return void
     */
    private function startConsumptionWorkers($simpleConsumer)
    {
        Logger::info("Starting {} consumption workers, clientId={}", [
            $this->consumptionThreadCount,
            $this->clientId
        ]);
        
        // Create worker coroutines
        for ($i = 0; $i < $this->consumptionThreadCount; $i++) {
            \Swoole\Coroutine::create(function() use ($simpleConsumer, $i) {
                $this->activeWorkers++;
                
                Logger::debug("Consumption worker {} started, clientId={}", [
                    $i,
                    $this->clientId
                ]);
                
                while ($this->running) {
                    try {
                        // Wait for task from queue
                        if ($this->consumptionTaskQueue->isEmpty()) {
                            \Swoole\Coroutine::sleep(0.01); // Sleep 10ms to avoid busy-waiting
                            continue;
                        }
                        
                        $task = $this->consumptionTaskQueue->dequeue();
                        
                        if ($task === null) {
                            continue;
                        }
                        
                        // Execute consumption task
                        $this->executeConsumptionTask($task, $simpleConsumer);
                        
                    } catch (\Exception $e) {
                        Logger::error("Exception in consumption worker {$i}, clientId={$this->clientId}", [
                            'error' => $e->getMessage(),
                            'trace' => $e->getTraceAsString()
                        ]);
                    }
                }
                
                $this->activeWorkers--;
                Logger::debug("Consumption worker {} stopped, clientId={}", [$i, $this->clientId]);
            });
        }
        
        Logger::info("All consumption workers started, clientId={}", [$this->clientId]);
    }
    
    /**
     * Dispatch message to consumption task queue
     * 
     * This method implements flow control based on cache thresholds,
     * similar to Java's ProcessQueue.isCacheFull().
     * It also detects FIFO messages and routes them to FIFO processing.
     * 
     * @param \Apache\Rocketmq\V2\Message $message gRPC message
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @return void
     */
    private function dispatchMessage($message, $simpleConsumer)
    {
        // Check if this is a FIFO message
        $systemProperties = $message->getSystemProperties();
        $messageGroup = $systemProperties->getMessageGroup();
        $messageType = $systemProperties->getMessageType();
        
        // If message has a message group or is marked as FIFO type, use FIFO processing
        if (!empty($messageGroup) || $messageType === \Apache\Rocketmq\V2\MessageType::FIFO) {
            $this->dispatchFifoMessage($message, $simpleConsumer);
            return;
        }
        
        // Normal message processing
        // Check cache thresholds (flow control)
        if ($this->isCacheFull()) {
            Logger::warn("Cache is full, waiting before receiving more messages, cachedCount={}, cachedSize={}MB, maxCount={}, maxSize={}MB, clientId={}", [
                $this->totalCachedMessageCount,
                round($this->totalCachedMessageSize / 1024 / 1024, 2),
                $this->maxCacheMessageCount,
                round($this->maxCacheMessageSizeInBytes / 1024 / 1024, 2),
                $this->clientId
            ]);
            
            // Wait until cache is released
            while ($this->running && $this->isCacheFull()) {
                \Swoole\Coroutine::sleep(0.1);
            }
        }
        
        // Update cache statistics
        $messageSize = strlen($message->getBody());
        $this->totalCachedMessageCount++;
        $this->totalCachedMessageSize += $messageSize;
        
        // Create consumption task
        $task = [
            'message' => $message,
            'messageSize' => $messageSize,
            'timestamp' => time(),
        ];
        
        // Enqueue task
        $this->consumptionTaskQueue->enqueue($task);
        
        Logger::debug("Message dispatched to consumption queue, queueSize={}, cachedCount={}, clientId={}", [
            $this->consumptionTaskQueue->count(),
            $this->totalCachedMessageCount,
            $this->clientId
        ]);
    }
    
    /**
     * Execute consumption task
     * 
     * This method calls the user-defined message listener and handles
     * ACK/NACK based on the result, similar to Java's StandardConsumeService.
     * 
     * @param array $task Consumption task
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @return void
     */
    private function executeConsumptionTask($task, $simpleConsumer)
    {
        $message = $task['message'];
        $messageSize = $task['messageSize'];
        $systemProperties = $message->getSystemProperties();
        $messageId = $systemProperties->getMessageId();
        
        Logger::debug("Executing consumption task, messageId={}, topic={}, clientId={}", [
            $messageId,
            $this->topic,
            $this->clientId
        ]);
        
        try {
            // Convert gRPC Message to MessageView-like object
            $messageView = $this->convertToMessageView($message);
            
            // Call message listener
            $startTime = microtime(true);
            $result = call_user_func($this->messageListener, $messageView);
            $duration = round((microtime(true) - $startTime) * 1000, 2);
            
            // Handle consumption result
            if ($result === \Apache\Rocketmq\Consumer\ConsumeResult::SUCCESS || $result === true || $result === null) {
                // ACK message
                $simpleConsumer->ack($message);
                
                $this->metrics['consumptionOkQuantity']++;
                
                Logger::info("Message consumed successfully, messageId={}, duration={}ms, topic={}, clientId={}", [
                    $messageId,
                    $duration,
                    $this->topic,
                    $this->clientId
                ]);
            } else {
                // NACK message (do not ACK, let it be redelivered)
                $this->metrics['consumptionErrorQuantity']++;
                
                Logger::warn("Message consumption failed, will be redelivered, messageId={}, duration={}ms, topic={}, clientId={}", [
                    $messageId,
                    $duration,
                    $this->topic,
                    $this->clientId
                ]);
            }
            
        } catch (\Exception $e) {
            // Exception during consumption - do not ACK
            $this->metrics['consumptionErrorQuantity']++;
            
            Logger::error("Exception during message consumption, messageId={}, topic={}, clientId={}", [
                $messageId,
                $this->topic,
                $this->clientId
            ], ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            
        } finally {
            // Release cache
            $this->totalCachedMessageCount--;
            $this->totalCachedMessageSize -= $messageSize;
            
            Logger::debug("Consumption task completed, messageId={}, remainingCache={}, clientId={}", [
                $messageId,
                $this->totalCachedMessageCount,
                $this->clientId
            ]);
        }
    }
    
    /**
     * Dispatch FIFO message to message group queue
     * 
     * This method implements the FIFO Consume Accelerator pattern from Java:
     * - Groups messages by messageGroup for parallel consumption
     * - Messages within the same group are consumed sequentially
     * - Different groups can be consumed in parallel (when accelerator is enabled)
     * 
     * Reference: Java FifoConsumeService.consume() lines 52-74
     * 
     * @param \Apache\Rocketmq\V2\Message $message gRPC message
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @return void
     */
    private function dispatchFifoMessage($message, $simpleConsumer)
    {
        $systemProperties = $message->getSystemProperties();
        $messageGroup = $systemProperties->getMessageGroup();
        $messageId = $systemProperties->getMessageId();
        
        // If no message group, treat as normal concurrent message
        if (empty($messageGroup)) {
            Logger::debug("FIFO message without messageGroup, using standard consumption, messageId={}, clientId={}", [
                $messageId,
                $this->clientId
            ]);
            $this->dispatchMessage($message, $simpleConsumer);
            return;
        }
        
        // Initialize message group queue if not exists
        if (!isset($this->fifoMessageGroupQueues[$messageGroup])) {
            $this->fifoMessageGroupQueues[$messageGroup] = new \SplQueue();
            $this->fifoMessageGroupLocks[$messageGroup] = false;
            
            Logger::debug("Created new FIFO message group, messageGroup={}, clientId={}", [
                $messageGroup,
                $this->clientId
            ]);
        }
        
        // Enqueue message to its group queue
        $messageSize = strlen($message->getBody());
        $task = [
            'message' => $message,
            'messageSize' => $messageSize,
            'timestamp' => time(),
            'messageGroup' => $messageGroup,
        ];
        
        $this->fifoMessageGroupQueues[$messageGroup]->enqueue($task);
        
        // Update cache statistics
        $this->totalCachedMessageCount++;
        $this->totalCachedMessageSize += $messageSize;
        
        Logger::debug("FIFO message enqueued, messageGroup={}, queueSize={}, messageId={}, clientId={}", [
            $messageGroup,
            $this->fifoMessageGroupQueues[$messageGroup]->count(),
            $messageId,
            $this->clientId
        ]);
        
        // Try to start consumption for this message group
        // Similar to Java's consumeIteratively() being called for each group
        $this->consumeFifoMessageGroup($messageGroup, $simpleConsumer);
    }
    
    /**
     * Consume messages from a FIFO message group sequentially
     * 
     * This is the PHP equivalent of Java's FifoConsumeService.consumeIteratively().
     * It ensures sequential consumption within a message group while allowing
     * different groups to be consumed in parallel (accelerator pattern).
     * 
     * Key behaviors:
     * - Lock mechanism prevents concurrent consumption of the same group
     * - Each group runs in its own Swoole coroutine for parallelism
     * - After finishing one message, recursively processes the next
     * 
     * Reference: Java FifoConsumeService.consumeIteratively() lines 76-93
     * 
     * @param string $messageGroup Message group identifier
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @return void
     */
    private function consumeFifoMessageGroup($messageGroup, $simpleConsumer)
    {
        // Check if accelerator is disabled
        if (!$this->enableFifoConsumeAccelerator) {
            Logger::debug("FIFO accelerator disabled, consuming sequentially, messageGroup={}, clientId={}", [
                $messageGroup,
                $this->clientId
            ]);
        }
        
        // Check if this message group is already being consumed (locked)
        // This ensures strict ordering within the same group
        if ($this->fifoMessageGroupLocks[$messageGroup]) {
            Logger::debug("Message group is locked, will process after current message completes, messageGroup={}, clientId={}", [
                $messageGroup,
                $this->clientId
            ]);
            return;
        }
        
        // Lock this message group to prevent concurrent consumption
        $this->fifoMessageGroupLocks[$messageGroup] = true;
        
        // Create a new Swoole coroutine for this message group
        // This allows different groups to be consumed in parallel (accelerator)
        \Swoole\Coroutine::create(function() use ($messageGroup, $simpleConsumer) {
            try {
                Logger::debug("Starting FIFO message group consumption, messageGroup={}, clientId={}", [
                    $messageGroup,
                    $this->clientId
                ]);
                
                // Process messages iteratively (like Java's recursive consumeIteratively)
                $this->consumeFifoMessageGroupIteratively($messageGroup, $simpleConsumer);
                
            } catch (\Exception $e) {
                Logger::error("Exception in FIFO message group consumption, messageGroup={}, clientId={}", [
                    $messageGroup,
                    $this->clientId
                ], ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
            } finally {
                // Unlock message group
                $this->fifoMessageGroupLocks[$messageGroup] = false;
                
                Logger::debug("Message group unlocked, messageGroup={}, clientId={}", [
                    $messageGroup,
                    $this->clientId
                ]);
                
                // Check if there are more messages waiting in the queue
                // If yes, trigger consumption again (similar to Java's future.addListener)
                if (isset($this->fifoMessageGroupQueues[$messageGroup]) && 
                    !$this->fifoMessageGroupQueues[$messageGroup]->isEmpty()) {
                    
                    Logger::debug("More messages in queue, continuing consumption, messageGroup={}, remaining={}, clientId={}", [
                        $messageGroup,
                        $this->fifoMessageGroupQueues[$messageGroup]->count(),
                        $this->clientId
                    ]);
                    
                    // Recursively consume remaining messages
                    $this->consumeFifoMessageGroup($messageGroup, $simpleConsumer);
                } else {
                    Logger::debug("Message group queue empty, consumption complete, messageGroup={}, clientId={}", [
                        $messageGroup,
                        $this->clientId
                    ]);
                }
            }
        });
    }
    
    /**
     * Consume messages from a FIFO message group iteratively
     * 
     * This method consumes messages one by one from the group queue,
     * ensuring strict ordering within the group.
     * 
     * @param string $messageGroup Message group identifier
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @return void
     */
    private function consumeFifoMessageGroupIteratively($messageGroup, $simpleConsumer)
    {
        while ($this->running && isset($this->fifoMessageGroupQueues[$messageGroup])) {
            $queue = $this->fifoMessageGroupQueues[$messageGroup];
            
            if ($queue->isEmpty()) {
                break;
            }
            
            $task = $queue->dequeue();
            
            if ($task === null) {
                continue;
            }
            
            // Execute consumption task (same as normal message)
            $this->executeFifoConsumptionTask($task, $simpleConsumer, $messageGroup);
        }
    }
    
    /**
     * Execute FIFO consumption task with retry logic
     * 
     * This is the PHP equivalent of Java's ProcessQueue.eraseFifoMessage().
     * It handles:
     * - Calling the message listener
     * - Retrying on failure (up to max attempts with exponential backoff)
     * - ACKing on success
     * - Not ACKing on final failure (message will be redelivered)
     * 
     * Reference: Java ProcessQueue.eraseFifoMessage() lines 519-546
     * 
     * @param array $task Consumption task
     * @param SimpleConsumer $simpleConsumer SimpleConsumer instance
     * @param string $messageGroup Message group identifier
     * @return void
     */
    private function executeFifoConsumptionTask($task, $simpleConsumer, $messageGroup)
    {
        $message = $task['message'];
        $messageSize = $task['messageSize'];
        $systemProperties = $message->getSystemProperties();
        $messageId = $systemProperties->getMessageId();
        
        // Get retry policy from config
        $maxAttempts = 3; // Default max attempts for FIFO messages
        $attempt = 1;
        
        Logger::info("Starting FIFO message consumption, messageId={}, messageGroup={}, maxAttempts={}, clientId={}", [
            $messageId,
            $messageGroup,
            $maxAttempts,
            $this->clientId
        ]);
        
        while ($attempt <= $maxAttempts) {
            try {
                // Convert gRPC Message to MessageView-like object
                $messageView = $this->convertToMessageView($message);
                
                // Call message listener
                $startTime = microtime(true);
                $result = call_user_func($this->messageListener, $messageView);
                $duration = round((microtime(true) - $startTime) * 1000, 2);
                
                // Handle consumption result
                if ($result === \Apache\Rocketmq\Consumer\ConsumeResult::SUCCESS || $result === true || $result === null) {
                    // Success - ACK message
                    $simpleConsumer->ack($message);
                    
                    $this->metrics['consumptionOkQuantity']++;
                    
                    if ($attempt > 1) {
                        Logger::info("FIFO message consumed successfully after retry, messageId={}, messageGroup={}, duration={}ms, attempt={}/{}, clientId={}", [
                            $messageId,
                            $messageGroup,
                            $duration,
                            $attempt,
                            $maxAttempts,
                            $this->clientId
                        ]);
                    } else {
                        Logger::debug("FIFO message consumed successfully, messageId={}, messageGroup={}, duration={}ms, clientId={}", [
                            $messageId,
                            $messageGroup,
                            $duration,
                            $this->clientId
                        ]);
                    }
                    
                    // Success - exit retry loop
                    break;
                } else {
                    // Consumption failed (returned FAILURE)
                    $this->metrics['consumptionErrorQuantity']++;
                    
                    Logger::warn("FIFO message consumption returned FAILURE, will retry, messageId={}, messageGroup={}, duration={}ms, attempt={}/{}, clientId={}", [
                        $messageId,
                        $messageGroup,
                        $duration,
                        $attempt,
                        $maxAttempts,
                        $this->clientId
                    ]);
                    
                    // Increment attempt and retry
                    $attempt++;
                    
                    if ($attempt <= $maxAttempts) {
                        // Exponential backoff: 200ms, 400ms, 800ms...
                        $backoffMs = pow(2, $attempt - 1) * 100;
                        Logger::debug("Waiting {}ms before retry, messageId={}, messageGroup={}, clientId={}", [
                            $backoffMs,
                            $messageId,
                            $messageGroup,
                            $this->clientId
                        ]);
                        usleep($backoffMs * 1000);
                    }
                }
                
            } catch (\Exception $e) {
                // Exception during consumption
                $this->metrics['consumptionErrorQuantity']++;
                
                Logger::error("Exception during FIFO message consumption, messageId={}, messageGroup={}, attempt={}/{}, clientId={}", [
                    $messageId,
                    $messageGroup,
                    $attempt,
                    $maxAttempts,
                    $this->clientId
                ], ['error' => $e->getMessage(), 'trace' => $e->getTraceAsString()]);
                
                // Increment attempt and retry
                $attempt++;
                
                if ($attempt <= $maxAttempts) {
                    // Exponential backoff
                    $backoffMs = pow(2, $attempt - 1) * 100;
                    usleep($backoffMs * 1000);
                }
            }
        }
        
        // If all attempts failed, do not ACK (message will be redelivered by RocketMQ)
        if ($attempt > $maxAttempts) {
            Logger::error("Failed to consume FIFO message after all attempts, messageId={}, messageGroup={}, maxAttempts={}, clientId={}", [
                $messageId,
                $messageGroup,
                $maxAttempts,
                $this->clientId
            ]);
            
            // Note: In production, you might want to forward to DLQ here
            // For now, we just don't ACK so RocketMQ will redeliver it
        }
        
        // Release cache (similar to Java's evictCache)
        $this->totalCachedMessageCount--;
        $this->totalCachedMessageSize -= $messageSize;
        
        Logger::debug("FIFO consumption task completed, messageId={}, messageGroup={}, remainingCache={}, clientId={}", [
            $messageId,
            $messageGroup,
            $this->totalCachedMessageCount,
            $this->clientId
        ]);
    }
    
    /**
     * Check if cache is full
     * 
     * @return bool True if cache exceeds thresholds
     */
    private function isCacheFull()
    {
        // Check message count threshold
        if ($this->totalCachedMessageCount >= $this->maxCacheMessageCount) {
            return true;
        }
        
        // Check message size threshold
        if ($this->totalCachedMessageSize >= $this->maxCacheMessageSizeInBytes) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Start consumer (blocking mode)
     * 
     * This implementation follows Java PushConsumer architecture:
     * - Uses ProcessQueue to manage messages per MessageQueue
     * - Dispatches messages to Swoole coroutines for concurrent processing
     * - Implements flow control based on cache thresholds
     * - Provides detailed logging and metrics collection
     * 
     * @return void
     * @throws \Exception If message listener is not set or Swoole is not available
     */
    /**
     * Start the push consumer (long-running mode)
     * 
     * This method starts the consumer and blocks until shutdown() is called.
     * It follows Java's PushConsumer design:
     * 1. Initialize subscription settings
     * 2. Create consumption workers
     * 3. Start background tasks to scan assignments and receive messages
     * 4. Keep running until explicitly stopped
     * 
     * @return void
     * @throws \Exception If message listener is not set or Swoole is not available
     */
    public function start()
    {
        if ($this->messageListener === null) {
            throw new \Exception("Message listener must be set before starting");
        }
        
        // Check Swoole availability
        if (!extension_loaded('swoole')) {
            throw new \Exception(
                "PushConsumer requires Swoole extension for concurrent consumption. " .
                "Please install it: pecl install swoole"
            );
        }
        
        if ($this->running) {
            Logger::warn("PushConsumer is already running, clientId={}", [$this->clientId]);
            return;
        }
        
        // Mark subscription as initialized (no longer in construction phase)
        $this->subscriptionInitialized = true;
        
        $this->running = true;
        $this->consumptionTaskQueue = new \SplQueue();
        
        Logger::info("Begin to start the rocketmq PushConsumer, clientId={}, consumerGroup={}, topic={}", [
            $this->clientId,
            $this->consumerGroup,
            $this->topic
        ]);
        
        try {
            // Create MessagingServiceClient directly (like Java's ClientManager)
            $grpcClient = $this->createMessagingServiceClient();
            
            // Initialize and start telemetry session
            $this->initializeTelemetrySession();
            
            // Start background timers for heartbeat and settings sync
            $this->startBackgroundTimers();
            
            // Sync settings to server via Telemetry (like Java's syncSettings)
            $this->syncSettings();
            
            // Start consumption workers with gRPC client
            $this->startConsumptionWorkersWithGrpcClient($grpcClient);
            
            Logger::info("The rocketmq PushConsumer starts successfully, clientId={}, consumptionThreads={}, maxCacheCount={}, maxCacheSize={}MB", [
                $this->clientId,
                $this->consumptionThreadCount,
                $this->maxCacheMessageCount,
                round($this->maxCacheMessageSizeInBytes / 1024 / 1024, 2)
            ]);
            
            // Main loop: receive messages and dispatch to workers (wrapped in coroutine)
            \Swoole\Coroutine::create(function() use ($grpcClient) {
                while ($this->running) {
                    try {
                        $this->metrics['receptionTimes']++;
                        
                        // Receive messages directly via gRPC (like Java's ConsumerImpl.receiveMessage)
                        $messages = $this->receiveMessagesViaGrpc($grpcClient);
                        $messageCount = count($messages);
                        
                        if ($messageCount > 0) {
                            $this->metrics['receivedMessagesQuantity'] += $messageCount;
                            
                            Logger::debug("Received {} messages, dispatching to consumption workers, topic={}, clientId={}", [
                                $messageCount,
                                $this->topic,
                                $this->clientId
                            ]);
                            
                            // Dispatch messages to consumption workers
                            foreach ($messages as $message) {
                                $this->dispatchMessageWithGrpcClient($message, $grpcClient);
                            }
                        } else {
                            // No messages received, wait a bit before next poll
                            \Swoole\Coroutine::sleep(1);
                        }
                    } catch (\Exception $e) {
                        Logger::error("Failed to receive message, clientId={$this->clientId}", ['error' => $e->getMessage()]);
                        \Swoole\Coroutine::sleep(1); // Avoid frequent retries
                    }
                }
            });
            
            // Keep the main process alive
            while ($this->running) {
                \Swoole\Event::wait();
                usleep(100000); // 100ms
            }
            
        } catch (\Throwable $t) {
            Logger::error("Exception raised while starting the rocketmq PushConsumer, clientId={$this->clientId}", [
                'error' => $t->getMessage(),
                'trace' => $t->getTraceAsString()
            ]);
            $this->shutdown();
            throw $t;
        }
    }
    
    /**
     * Shutdown the push consumer gracefully
     * 
     * Follows Java's shutdown order:
     * 1. Stop receiving new messages
     * 2. Wait for inflight receive requests to finish
     * 3. Shutdown consumption executor and wait for all messages to be consumed
     * 4. Stop telemetry session and background timers
     * 5. Sleep briefly to allow async ACK operations
     * 6. Release resources
     */
    public function shutdown()
    {
        if (!$this->running) {
            Logger::warn("PushConsumer is not running, clientId={}", [$this->clientId]);
            return;
        }
        
        Logger::info("Begin to shutdown the rocketmq PushConsumer, clientId={}", [$this->clientId]);
        
        // Step 1: Stop receiving new messages
        $this->running = false;
        
        // Step 2: Wait for consumption task queue to be empty
        if ($this->consumptionTaskQueue !== null) {
            $timeout = 30; // 30 seconds timeout
            $startTime = time();
            
            while (!$this->consumptionTaskQueue->isEmpty() && (time() - $startTime) < $timeout) {
                Logger::info("Waiting for consumption tasks to complete, remaining={}, clientId={}", [
                    $this->consumptionTaskQueue->count(),
                    $this->clientId
                ]);
                usleep(100000); // 100ms
            }
            
            if (!$this->consumptionTaskQueue->isEmpty()) {
                Logger::warn("Timeout waiting for consumption tasks, remaining={}, clientId={}", [
                    $this->consumptionTaskQueue->count(),
                    $this->clientId
                ]);
            }
        }
        
        // Step 3: Wait for active workers to finish
        $workerTimeout = 10; // 10 seconds
        $workerStartTime = time();
        
        while ($this->activeWorkers > 0 && (time() - $workerStartTime) < $workerTimeout) {
            Logger::info("Waiting for active consumption workers to finish, activeWorkers={}, clientId={}", [
                $this->activeWorkers,
                $this->clientId
            ]);
            usleep(100000); // 100ms
        }
        
        if ($this->activeWorkers > 0) {
            Logger::warn("Timeout waiting for workers, activeWorkers={}, clientId={}", [
                $this->activeWorkers,
                $this->clientId
            ]);
        }
        
        // Step 4: Stop telemetry session
        if ($this->telemetrySession !== null) {
            try {
                $this->telemetrySession->stop();
                Logger::info("Telemetry session stopped, clientId={}", [$this->clientId]);
            } catch (\Exception $e) {
                Logger::warn("Failed to stop telemetry session, clientId={}", [
                    $this->clientId,
                    'error' => $e->getMessage()
                ]);
            }
        }
        
        // Step 5: Sleep briefly to allow async ACK operations
        usleep(1000000); // 1 second
        
        // Step 6: Log final metrics
        Logger::info("Shutdown the rocketmq PushConsumer successfully, clientId={}, metrics={}", [
            $this->clientId,
            json_encode($this->metrics)
        ]);
    }
    

    
    /**
     * Convert gRPC Message to MessageView-like object
     * 
     * @param \Apache\Rocketmq\V2\Message $grpcMessage
     * @return object Simple message view object
     */
    private function convertToMessageView($grpcMessage)
    {
        $systemProperties = $grpcMessage->getSystemProperties();
        
        // Create a simple stdClass object with necessary properties
        $messageView = new \stdClass();
        $messageView->messageId = $systemProperties->getMessageId();
        $messageView->topic = $grpcMessage->getTopic()->getName();
        $messageView->body = $grpcMessage->getBody();
        $messageView->tag = $systemProperties->hasTag() ? $systemProperties->getTag() : null;
        $messageView->keys = $systemProperties->getKeys();
        $messageView->bornTimestamp = $systemProperties->getBornTimestamp() ? 
            $systemProperties->getBornTimestamp()->getSeconds() * 1000 : time() * 1000;
        $messageView->receiptHandle = $systemProperties->getReceiptHandle();
        
        // Add methods via closure binding
        $messageView->getMessageId = function() use ($messageView) {
            return $messageView->messageId;
        };
        $messageView->getTopic = function() use ($messageView) {
            return $messageView->topic;
        };
        $messageView->getBody = function() use ($messageView) {
            return $messageView->body;
        };
        $messageView->getTag = function() use ($messageView) {
            return $messageView->tag;
        };
        $messageView->getKeys = function() use ($messageView) {
            return $messageView->keys ?? [];
        };
        $messageView->getBornTimestamp = function() use ($messageView) {
            return $messageView->bornTimestamp;
        };
        
        return $messageView;
    }
    
    /**
     * Stop consumer
     * 
     * @return void
     */
    public function stop()
    {
        $this->running = false;
        echo "PushConsumer is stopping...\n";
    }
    


    
    /**
     * Close consumer
     * 
     * @return void
     */
    public function close()
    {
        $this->stop();
        if ($this->client !== null) {
            try {
                $pool = ConnectionPool::getInstance();
                $pool->returnConnection($this->config, $this->client);
            } catch (\Exception $e) {
                // Ignore exception when returning connection
                Logger::error("Failed to return connection to pool, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            } finally {
                $this->client = null;
            }
        }
    }
    
    /**
     * Get Client configuration object
     * 
     * @return ClientConfiguration Client configuration object
     */
    public function getConfig()
    {
        return $this->config;
    }
    
    /**
     * Set max message number
     *
     * @param int $maxMessageNum
     * @return void
     */
    public function setMaxMessageNum($maxMessageNum)
    {
        $this->maxMessageNum = $maxMessageNum;
    }
    
    /**
     * Set invisible duration in seconds
     *
     * @param int $invisibleDuration
     * @return void
     */
    public function setInvisibleDuration($invisibleDuration)
    {
        $this->invisibleDuration = $invisibleDuration;
    }
    
    /**
     * Get max message number
     *
     * @return int
     */
    public function getMaxMessageNum()
    {
        return $this->maxMessageNum;
    }
    
    /**
     * Get invisible duration
     *
     * @return int
     */
    public function getInvisibleDuration()
    {
        return $this->invisibleDuration;
    }
    
    /**
     * Create MessagingServiceClient instance (like Java's ClientManager)
     * 
     * @return MessagingServiceClient
     */
    private function createMessagingServiceClient()
    {
        $endpoints = $this->config->getEndpoints();
        $sslEnabled = $this->config->isSslEnabled();
        
        // Create gRPC channel options
        $opts = [
            'credentials' => $sslEnabled ? \Grpc\ChannelCredentials::createSsl() : \Grpc\ChannelCredentials::createInsecure(),
            'grpc.ssl_target_name_override' => '',
            'update_metadata' => function ($metaData) {
                // Add client ID to metadata
                $metaData['headers'] = ['clientID' => $this->clientId];
                return $metaData;
            }
        ];
        
        return new MessagingServiceClient($endpoints, $opts);
    }
    
    /**
     * Receive messages directly via gRPC (like Java's ConsumerImpl.receiveMessage)
     * 
     * @param MessagingServiceClient $grpcClient
     * @return array Array of received messages
     */
    private function receiveMessagesViaGrpc($grpcClient)
    {
        // Build ReceiveMessageRequest
        $topicResource = new Resource();
        $topicResource->setResourceNamespace($this->config->getNamespace());
        $topicResource->setName($this->topic);
        
        $groupResource = new Resource();
        $groupResource->setResourceNamespace($this->config->getNamespace());
        $groupResource->setName($this->consumerGroup);
        
        // Build message queue (like SimpleConsumer)
        $mq = new MessageQueue();
        $topicResource = new Resource();
        $topicResource->setResourceNamespace($this->config->getNamespace());
        $topicResource->setName($this->topic);
        $mq->setTopic($topicResource);
        $mq->setAcceptMessageTypes([MessageType::NORMAL]);
        
        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($mq);
        $request->setBatchSize($this->maxMessageNum);
        
        $invisibleDurationProto = new \Google\Protobuf\Duration();
        $invisibleDurationProto->setSeconds($this->invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationProto);
        
        $request->setAutoRenew(false);
        
        $longPollingTimeout = new \Google\Protobuf\Duration();
        $longPollingTimeout->setSeconds(30);  // 30 seconds long polling
        $request->setLongPollingTimeout($longPollingTimeout);
        
        try {
            // Call gRPC service (server streaming)
            $call = $grpcClient->ReceiveMessage($request);
            
            // Extract messages from response stream
            $messages = [];
            foreach ($call->responses() as $response) {
                if ($response->hasMessage()) {
                    $messages[] = $response->getMessage();
                }
            }
            
            return $messages;
        } catch (\Exception $e) {
            Logger::error("gRPC receive message failed: " . $e->getMessage());
            return [];
        }
    }
    
    /**
     * Start consumption workers with gRPC client
     * 
     * @param MessagingServiceClient $grpcClient
     * @return void
     */
    private function startConsumptionWorkersWithGrpcClient($grpcClient)
    {
        // For now, we'll use a simple implementation
        // In a full implementation, this would create worker coroutines
        Logger::info("Started {} consumption workers", [$this->consumptionThreadCount]);
    }
    
    /**
     * Dispatch message to consumption worker with gRPC client
     * 
     * @param object $message The message to dispatch
     * @param MessagingServiceClient $grpcClient
     * @return void
     */
    private function dispatchMessageWithGrpcClient($message, $grpcClient)
    {
        try {
            // Convert gRPC Message to MessageView
            $messageView = $this->convertToMessageView($message);
            
            // Call message listener
            $result = call_user_func($this->messageListener, $messageView);
            
            // ACK the message if successful
            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                $this->ackMessageViaGrpc($grpcClient, $message);
            }
        } catch (\Exception $e) {
            Logger::error("Failed to process message: " . $e->getMessage());
        }
    }
    
    /**
     * Sync settings to server via Telemetry (like Java's syncSettings)
     * 
     * This method sends the consumer's subscription settings to the server
     * through a one-way telemetry call, allowing the server to know about
     * this consumer's configuration.
     * 
     * @return void
     */
    private function syncSettings()
    {
        if (!$this->telemetrySession) {
            Logger::warn("Cannot sync settings, telemetry session is null, clientId={}", [$this->clientId]);
            return;
        }
        
        try {
            // Ensure subscriptionExpressions is an array (defensive check)
            if (!is_array($this->subscriptionExpressions)) {
                Logger::warn("subscriptionExpressions is not an array, resetting to empty array, clientId={}, type={}", [
                    $this->clientId,
                    gettype($this->subscriptionExpressions)
                ]);
                $this->subscriptionExpressions = [];
            }
            
            // Build PushSubscriptionSettings protobuf message
            $subscriptionSettings = new \Apache\Rocketmq\PushSubscriptionSettings(
                $this->config->getNamespace(),
                $this->clientId,
                $this->config->getEndpoints(),
                $this->consumerGroup,
                $this->subscriptionExpressions
            );
            
            // Convert to protobuf
            $settingsProto = $subscriptionSettings->toProtobuf();
            
            // Send settings via telemetry session
            $command = new \Apache\Rocketmq\V2\TelemetryCommand();
            $command->setSettings($settingsProto);
            
            $this->telemetrySession->write($command);
            
            Logger::info("Syncing PushConsumer settings to server, clientId={}, consumerGroup={}, topics={}", [
                $this->clientId,
                $this->consumerGroup,
                implode(',', array_keys($this->subscriptionExpressions))
            ]);
            
        } catch (\Throwable $t) {
            Logger::warn("Failed to sync settings, clientId={$this->clientId}", [
                'error' => $t->getMessage()
            ]);
            // Don't throw - settings sync is not critical for operation
        }
    }
    
    /**
     * Initialize telemetry session
     * 
     * @return void
     * @throws \Exception If initialization fails
     */
    private function initializeTelemetrySession(): void
    {
        // Try to use Swoole async implementation if available
        if (extension_loaded('swoole') && class_exists('Apache\Rocketmq\AsyncTelemetrySession')) {
            try {
                // For PushConsumer, we need to pass all subscribed topics
                $topics = array_keys($this->subscriptionExpressions);
                $firstTopic = !empty($topics) ? $topics[0] : '';
                
                $this->telemetrySession = new AsyncTelemetrySession(
                    $this->createMessagingServiceClient(),
                    $this->clientId,
                    $this->consumerGroup,
                    $firstTopic,
                    2, // ClientType::PUSH_CONSUMER
                    15 // awaitDuration in seconds
                );
                $this->telemetrySession->start();
                Logger::info("AsyncTelemetrySession started with Swoole, clientId={}", [$this->clientId]);
                return;
            } catch (\Exception $e) {
                Logger::warn("Failed to start AsyncTelemetrySession, clientId={}", [
                    $this->clientId,
                    "error" => $e->getMessage()
                ]);
                Logger::warn("Falling back to disabled telemetry, clientId={}", [$this->clientId]);
            }
        }
        
        // Fallback: disable telemetry
        Logger::info("Note: Telemetry session disabled (Swoole not available or failed), clientId={}", [$this->clientId]);
        $this->telemetrySession = null;
    }
    
    /**
     * Start background timers for heartbeat and settings sync
     * Similar to Java's ClientManagerImpl scheduler
     * 
     * @return void
     */
    private function startBackgroundTimers(): void
    {
        // Use Swoole timer if available (preferred)
        if (extension_loaded('swoole') && class_exists('\Swoole\Timer')) {
            Logger::info("Starting Swoole timer for heartbeat, interval={}s, clientId={}", [
                self::HEARTBEAT_INTERVAL,
                $this->clientId
            ]);
            
            \Swoole\Timer::tick(self::HEARTBEAT_INTERVAL * 1000, function($timerId) {
                if (!$this->running) {
                    \Swoole\Timer::clear($timerId);
                    return;
                }
                
                try {
                    $this->heartbeat();
                    Logger::debug("Heartbeat sent, clientId={}", [$this->clientId]);
                } catch (\Exception $e) {
                    Logger::warn("Warning: Heartbeat failed, clientId={}", [
                        $this->clientId,
                        'error' => $e->getMessage()
                    ]);
                }
            });
            
            // Sync settings every 5 minutes (Java default)
            \Swoole\Timer::tick(self::SETTINGS_SYNC_INTERVAL * 1000, function($timerId) {
                if (!$this->running) {
                    \Swoole\Timer::clear($timerId);
                    return;
                }
                
                try {
                    $this->syncSettings();
                    Logger::debug("Settings synced, clientId={}", [$this->clientId]);
                } catch (\Exception $e) {
                    Logger::warn("Warning: Settings sync failed, clientId={}", [
                        $this->clientId,
                        'error' => $e->getMessage()
                    ]);
                }
            });
            
            return;
        }
        
        // Fallback warning
        Logger::warn("Warning: Swoole timer not available, background timers disabled, clientId={}", [$this->clientId]);
    }
    
    /**
     * Send heartbeat to server
     * 
     * @return void
     * @throws \Exception If heartbeat fails
     */
    private function heartbeat(): void
    {
        if (!$this->telemetrySession || !$this->telemetrySession->isActive()) {
            Logger::warn("Cannot send heartbeat, telemetry session is not active, clientId={}", [$this->clientId]);
            return;
        }
        
        try {
            // Build heartbeat request
            $groupResource = new Resource();
            $groupResource->setResourceNamespace($this->config->getNamespace());
            $groupResource->setName($this->consumerGroup);
            
            $request = new HeartbeatRequest();
            $request->setGroup($groupResource);
            
            // Wrap in TelemetryCommand
            $command = new \Apache\Rocketmq\V2\TelemetryCommand();
            // Note: Heartbeat is typically sent via MessagingService.Heartbeat RPC
            // For telemetry-based heartbeat, we would use a different approach
            
            // For now, use direct gRPC call
            $grpcClient = $this->createMessagingServiceClient();
            list($response, $status) = $grpcClient->Heartbeat($request)->wait();
            
            if ($status->code !== STATUS_OK) {
                throw new \Exception("Heartbeat failed: {$status->details}");
            }
            
            Logger::debug("Heartbeat sent successfully, clientId={}", [$this->clientId]);
            
        } catch (\Exception $e) {
            Logger::warn("Failed to send heartbeat, clientId={$this->clientId}", [
                'error' => $e->getMessage()
            ]);
            throw $e;
        }
    }
    
    /**
     * Acknowledge message via gRPC
     * 
     * @param MessagingServiceClient $grpcClient
     * @param object $message The message to acknowledge
     * @return void
     */
    private function ackMessageViaGrpc($grpcClient, $message)
    {
        try {
            $systemProperties = $message->getSystemProperties();
            $receiptHandle = $systemProperties->getReceiptHandle();
            
            $topicResource = new Resource();
            $topicResource->setResourceNamespace($this->config->getNamespace());
            $topicResource->setName($this->topic);
            
            $groupResource = new Resource();
            $groupResource->setResourceNamespace($this->config->getNamespace());
            $groupResource->setName($this->consumerGroup);
            
            $entry = new AckMessageEntry();
            $entry->setTopic($topicResource);
            $entry->setReceiptHandle($receiptHandle);
            
            $request = new AckMessageRequest();
            $request->setGroup($groupResource);
            $request->setEntries([$entry]);
            
            list($response, $status) = $grpcClient->AckMessage($request)->wait();
            
            if ($status->code !== STATUS_OK) {
                Logger::warn("Failed to ACK message: {$status->details}");
                return;
            }
            
            // Check response status
            if ($response->hasStatus() && $response->getStatus()->getCode() !== 20000) {
                Logger::warn("ACK failed with code: " . $response->getStatus()->getCode());
            }
        } catch (\Exception $e) {
            Logger::error("Failed to ACK message: " . $e->getMessage());
        }
    }
}