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

require 'vendor/autoload.php';
require_once __DIR__ . '/Logger.php';

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
use Apache\Rocketmq\TelemetrySession;
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
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;
use const Grpc\STATUS_OK;

// Initialize logger to write to ~/logs/rocketmq/rocketmq_client_php.log
\Apache\Rocketmq\Logger::init();

/**
 * RocketMQ SimpleConsumer Implementation
 * 
 * Supports the following features:
 * - Pull messages
 * - Acknowledge messages (ACK)
 * - Change message visibility duration
 * - Long polling
 * 
 * Usage example:
 * $consumer = SimpleConsumer::getInstance($endpoints, $consumerGroup, $topic);
 * $messages = $consumer->receive();
 * foreach ($messages as $message) {
 *     // Process message
 *     $consumer->ack($message);
 * }
 */
class SimpleConsumer
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
     * @var TelemetrySession|null Telemetry session for bidirectional stream
     */
    private $telemetrySession = null;
    
    /**
     * @var MetricsCollector|null Metrics collector
     */
    private $metricsCollector = null;
    
    /**
     * @var ClientMeterManager|null Client meter manager
     */
    private $meterManager = null;
    
    /**
     * @var int Maximum message number per receive
     */
    private $maxMessageNum = 32;
    
    /**
     * @var int Message invisible duration (seconds)
     */
    private $invisibleDuration = 30;
    
    /**
     * @var int Long polling wait duration (seconds)
     */
    private $awaitDuration = 30;
    
    /**
     * @var RetryPolicy Retry policy
     */
    private $retryPolicy;
    
    /**
     * @var string Client state: CREATED, STARTING, RUNNING, STOPPING, TERMINATED
     */
    private $state = 'CREATED';
    
    /**
     * @var int|null Background heartbeat timer PID
     */
    private $heartbeatTimerPid = null;
    
    /**
     * @var int Heartbeat interval in seconds (Java default: 10s)
     */
    const HEARTBEAT_INTERVAL = 10;
    
    /**
     * @var int Settings sync interval in seconds (Java default: 5min = 300s)
     */
    const SETTINGS_SYNC_INTERVAL = 300;
    
    /**
     * Private constructor to prevent direct instantiation
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string (backward compatible)
     * @param string|null $consumerGroup Consumer group name（Used when first parameter is endpoints string）
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @param RetryPolicy|null $retryPolicy Retry policy (optional, Used when first parameter is endpoints string)
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    private function __construct($configOrEndpoints, $consumerGroup = null, $topic = null, $retryPolicy = null)
    {
        // Supports two construction methods:
        // 1. New configuration object method: __construct(ClientConfiguration $config, $consumerGroup, $topic)
        // 2. Legacy compatible method: __construct($endpoints, $consumerGroup, $topic, $retryPolicy)
        
        if ($configOrEndpoints instanceof ClientConfiguration) {
            // New configuration object method
            if (empty($consumerGroup) || empty($topic)) {
                throw new \InvalidArgumentException("Consumer group and topic cannot be empty");
            }
            
            $this->config = $configOrEndpoints;
            $this->consumerGroup = $consumerGroup;
            $this->topic = $topic;
            $this->retryPolicy = $this->config->getOrCreateRetryPolicy();
            
            // Set route cache configuration from client configuration
            $routeCache = \Apache\Rocketmq\RouteCache::getInstance();
            $routeCache->setConfigFromClientConfiguration($this->config);
        } else {
            // Legacy compatible method
            if (empty($consumerGroup) || empty($topic)) {
                throw new \InvalidArgumentException("Consumer group and topic cannot be empty");
            }
            
            $this->config = new ClientConfiguration($configOrEndpoints);
            if ($retryPolicy !== null) {
                $this->config->withRetryPolicy($retryPolicy);
            }
            $this->consumerGroup = $consumerGroup;
            $this->topic = $topic;
            $this->retryPolicy = $this->config->getOrCreateRetryPolicy();
            
            // Set route cache configuration from client configuration
            $routeCache = \Apache\Rocketmq\RouteCache::getInstance();
            $routeCache->setConfigFromClientConfiguration($this->config);
        }
        
        $this->clientId = $this->generateClientId();
        
        // Auto-initialize metrics support
        $this->metricsCollector = new MetricsCollector($this->clientId);
        $this->meterManager = new ClientMeterManager($this->clientId, $this->metricsCollector);
    }
    
    /**
     * Get SimpleConsumer singleton instance (configuration object recommended)
     * 
     * Usage example:
     * ```php
     * // Method 1: Using configuration object (recommended)
     * $config = new ClientConfiguration('127.0.0.1:8080');
     * $consumer = SimpleConsumer::getInstance($config, 'my-group', 'my-topic');
     * 
     * // Method 2: Using legacy method (backward compatible)
     * $consumer = SimpleConsumer::getInstance('127.0.0.1:8080', 'my-group', 'my-topic');
     * ```
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string
     * @param string|null $consumerGroup Consumer group name（Used when first parameter is endpoints string）
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @param RetryPolicy|null $retryPolicy Retry policy (optional, only effective when using endpoint string)
     * @return SimpleConsumer SimpleConsumer instance
     */
    public static function getInstance($configOrEndpoints, $consumerGroup = null, $topic = null, $retryPolicy = null)
    {
        return new self($configOrEndpoints, $consumerGroup, $topic, $retryPolicy);
    }
    
    /**
     * Start consumer
     * 
     * @return void
     * @throws \Exception If startup fails
     */
    public function start()
    {
        if ($this->state !== 'CREATED') {
            throw new \Exception("Consumer is already {$this->state}");
        }
        
        $this->state = 'STARTING';
        
        try {
            // Send heartbeat to verify connection
            $this->heartbeat();
            
            // Initialize and start telemetry session
            $this->initializeTelemetrySession();
            
            // Start background timers for heartbeat and settings sync
            $this->startBackgroundTimers();
            
            $this->state = 'RUNNING';
        } catch (\Exception $e) {
            $this->state = 'FAILED';
            throw new \Exception("Failed to start consumer: " . $e->getMessage(), 0, $e);
        }
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
     * Send heartbeat
     * 
     * @return void
     */
    public function heartbeat()
    {
        $request = new HeartbeatRequest();
        
        // Set consumer group
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        // Set client type
        $request->setClientType(\Apache\Rocketmq\V2\ClientType::SIMPLE_CONSUMER);
        
        $call = $this->getClient()->Heartbeat($request);
        list($response, $status) = $call->wait();
        
        return $response;
    }
    
    /**
     * Enable metrics collection and export
     * 
     * This method enables automatic metrics collection for message consumption.
     * It will automatically collect:
     * - Process latency (histogram)
     * - Message size distribution
     * - Success/failure counts
     * - Cached messages count (gauge)
     * 
     * @param string|null $exportEndpoint OTLP export endpoint (optional)
     *                                    Example: "http://localhost:4318/v1/metrics"
     * @param int $exportInterval Export interval in seconds (default: 60)
     * @return self Return current instance for chainable calls
     * @example
     * ```php
     * // Simple usage - just enable metrics
     * $consumer->enableMetrics();
     * 
     * // With custom export endpoint
     * $consumer->enableMetrics('http://localhost:4318/v1/metrics', 30);
     * ```
     */
    public function enableMetrics(?string $exportEndpoint = null, int $exportInterval = 60): self {
        // Enable the meter manager
        $this->meterManager->enable($exportEndpoint, $exportInterval);
        
        Logger::info("Metrics enabled for Consumer, clientId={}, exportEndpoint={}", [
            $this->clientId,
            $exportEndpoint ?? 'local-only'
        ]);
        
        return $this;
    }
    
    /**
     * Disable metrics collection
     * 
     * @return self Return current instance for chainable calls
     */
    public function disableMetrics(): self {
        if ($this->meterManager !== null) {
            $this->meterManager->disable();
            Logger::info("Metrics disabled for Consumer, clientId={}", [$this->clientId]);
        }
        
        return $this;
    }
    
    /**
     * Get client meter manager
     * 
     * @return ClientMeterManager|null Meter manager or null if not enabled
     */
    public function getMeterManager(): ?ClientMeterManager {
        return $this->meterManager;
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
                $this->telemetrySession = new AsyncTelemetrySession(
                    $this->getClient(),
                    $this->clientId,
                    $this->consumerGroup,
                    $this->topic,
                    \Apache\Rocketmq\V2\ClientType::SIMPLE_CONSUMER,
                    $this->awaitDuration
                );
                $this->telemetrySession->start();
                Logger::info("AsyncTelemetrySession started with Swoole, clientId={}", [$this->clientId]);
                return;
            } catch (\Exception $e) {
                Logger::warn("Failed to start AsyncTelemetrySession, clientId={}", [$this->clientId, "error" => $e->getMessage()]);
                Logger::warn("Falling back to disabled telemetry, clientId={}", [$this->clientId]);
            }
        }
        
        // Fallback: disable telemetry (RocketMQ Proxy has compatibility issues)
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
        // Use Swoole timer if available (preferred over pcntl_fork)
        if (extension_loaded('swoole') && class_exists('\Swoole\Timer')) {
            Logger::info("Starting Swoole timer for heartbeat, interval={}s, clientId={}", [self::HEARTBEAT_INTERVAL, $this->clientId]);
            
            \Swoole\Timer::tick(self::HEARTBEAT_INTERVAL * 1000, function($timerId) {
                if ($this->state !== 'RUNNING') {
                    \Swoole\Timer::clear($timerId);
                    return;
                }
                
                try {
                    $this->heartbeat();
                    Logger::debug("Heartbeat sent, clientId={}", [$this->clientId]);
                } catch (\Exception $e) {
                    Logger::warn("Warning: Heartbeat failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                }
            });
            
            // Sync settings every 5 minutes (Java default)
            \Swoole\Timer::tick(self::SETTINGS_SYNC_INTERVAL * 1000, function($timerId) {
                if ($this->state !== 'RUNNING') {
                    \Swoole\Timer::clear($timerId);
                    return;
                }
                
                try {
                    $this->syncSettings();
                    Logger::debug("Settings synced, clientId={}", [$this->clientId]);
                } catch (\Exception $e) {
                    Logger::warn("Warning: Settings sync failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                }
            });
            
            return;
        }
        
        // Fallback to pcntl_fork if Swoole timer not available
        if (!function_exists('pcntl_fork')) {
            Logger::warn("Warning: Neither Swoole timer nor pcntl_fork available, background timers disabled, clientId={}", [$this->clientId]);
            return;
        }
        
        // Start heartbeat timer using pcntl_fork
        $this->heartbeatTimerPid = pcntl_fork();
        
        if ($this->heartbeatTimerPid == -1) {
            Logger::error("Error: Failed to fork heartbeat timer process, clientId={}", [$this->clientId]);
            return;
        } elseif ($this->heartbeatTimerPid > 0) {
            Logger::info("Started background heartbeat timer (PID: {}), interval={}s, clientId={}", [
                $this->heartbeatTimerPid,
                self::HEARTBEAT_INTERVAL,
                $this->clientId
            ]);
            return;
        } else {
            // Child process: run heartbeat loop
            $this->runHeartbeatLoop();
            exit(0);
        }
    }
    
    /**
     * Run heartbeat loop in background process
     * Sends heartbeat and syncs settings periodically
     * 
     * @return void
     */
    private function runHeartbeatLoop(): void
    {
        Logger::info("Heartbeat timer started, clientId={}", [$this->clientId]);
        
        $lastSettingsSync = time();
        $cycleCount = 0;
        
        while ($this->state === 'RUNNING') {
            sleep(self::HEARTBEAT_INTERVAL);
            
            if ($this->state !== 'RUNNING') {
                break;
            }
            
            $cycleCount++;
            
            // Send heartbeat
            try {
                $this->heartbeat();
                Logger::debug("Heartbeat sent (cycle #{}), clientId={}", [$cycleCount, $this->clientId]);
            } catch (\Exception $e) {
                Logger::warn("Warning: Heartbeat failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
            }
            
            // Sync settings every SETTINGS_SYNC_INTERVAL seconds (Java default: 5min)
            $now = time();
            if (($now - $lastSettingsSync) >= self::SETTINGS_SYNC_INTERVAL) {
                try {
                    $this->syncSettings();
                    $lastSettingsSync = $now;
                    Logger::debug("Settings synced, clientId={}", [$this->clientId]);
                } catch (\Exception $e) {
                    Logger::warn("Warning: Settings sync failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                }
            }
        }
        
        Logger::info("Heartbeat timer stopped, clientId={}", [$this->clientId]);
    }
    
    /**
     * Sync settings to server (similar to Java's client.syncSettings())
     * 
     * @return void
     * @throws \Exception If sync fails
     */
    private function syncSettings(): void
    {
        if (!$this->telemetrySession) {
            Logger::warn("Warning: Cannot sync settings, telemetry session is null, clientId={}", [$this->clientId]);
            return;
        }
        
        // For Swoole async session, it handles sync automatically
        // For sync session, we would need to send Settings via Telemetry stream
        // This is a placeholder for future implementation
        Logger::info("Settings sync requested (not yet implemented for sync mode), clientId={}", [$this->clientId]);
    }
    
    /**
     * Receive messages (with retry)
     * 
     * @param int|null $maxMessageNum Maximum message count
     * @param int|null $invisibleDuration Message invisible duration (seconds)
     * @return array Message list
     * @throws \Exception If receive fails
     */
    public function receive($maxMessageNum = null, $invisibleDuration = null)
    {
        return $this->receiveWithRetry($maxMessageNum, $invisibleDuration);
    }
    
    /**
     * Receive messages (internal implementation, with retry)
     * 
     * @param int|null $maxMessageNum Maximum message count
     * @param int|null $invisibleDuration Message invisible duration (seconds)
     * @param int $attempt Current attempt count
     * @return array Message list
     * @throws \Exception If receive fails
     */
    private function receiveWithRetry($maxMessageNum = null, $invisibleDuration = null, $attempt = 1)
    {
        // Check state
        if ($this->state !== 'RUNNING' && $this->state !== 'CREATED') {
            throw new \Exception("Consumer is not running (current state: {$this->state})");
        }
        
        try {
            return $this->receiveInternal($maxMessageNum, $invisibleDuration);
        } catch (\Exception $e) {
            // Determine whether to retry
            if (!$this->retryPolicy->shouldRetry($attempt, $e)) {
                throw $e;
            }
            
            // Calculate backoff time
            $delayMs = $this->retryPolicy->getNextAttemptDelay($attempt);
            
            // Execute backoff wait
            if ($delayMs > 0) {
                usleep($delayMs * 1000);
            }
            
            // Recursive retry
            return $this->receiveWithRetry($maxMessageNum, $invisibleDuration, $attempt + 1);
        }
    }
    
    /**
     * Receive messages (internal implementation, no retry)
     * 
     * @param int|null $maxMessageNum Maximum message count
     * @param int|null $invisibleDuration Message invisible duration (seconds)
     * @return array Message list
     * @throws \Exception If receive fails
     */
    private function receiveInternal($maxMessageNum = null, $invisibleDuration = null)
    {
        // Check state - only RUNNING state can receive messages
        if ($this->state !== 'RUNNING') {
            Logger::error("Unable to receive message because consumer is not running, state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \Exception("Simple consumer is not running now");
        }
        
        // Validate parameters
        $maxMessageNum = $maxMessageNum ?: $this->maxMessageNum;
        if ($maxMessageNum <= 0) {
            Logger::error("maxMessageNum must be greater than 0, maxMessageNum={}, clientId={}", [
                $maxMessageNum,
                $this->clientId
            ]);
            throw new \InvalidArgumentException("maxMessageNum must be greater than 0");
        }
        
        $invisibleDuration = $invisibleDuration ?: $this->invisibleDuration;
        
        Logger::debug("Receiving messages, topic={}, maxMessageNum={}, invisibleDuration={}s, awaitDuration={}s, clientId={}", [
            $this->topic,
            $maxMessageNum,
            $invisibleDuration,
            $this->awaitDuration,
            $this->clientId
        ]);
        
        $request = new ReceiveMessageRequest();
        
        // Set consumer group
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        // Set message queue
        $mq = new MessageQueue();
        $resource = new Resource();
        $resource->setName($this->topic);
        $mq->setTopic($resource);
        $mq->setAcceptMessageTypes([MessageType::NORMAL]);
        
        $request->setMessageQueue($mq);
        
        // Set invisible duration (required for simple consumer)
        $invisibleDurationProto = new \Google\Protobuf\Duration();
        $invisibleDurationProto->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationProto);
        
        // Set batch size
        $request->setBatchSize($maxMessageNum);
        
        // Enable auto renew
        $request->setAutoRenew(true);
        
        // Set long polling timeout
        $longPollingTimeout = new \Google\Protobuf\Duration();
        $longPollingTimeout->setSeconds($this->awaitDuration);
        $request->setLongPollingTimeout($longPollingTimeout);
        
        $call = $this->getClient()->ReceiveMessage($request);
        
        $messages = [];
        
        // Read response stream
        foreach ($call->responses() as $response) {
            if ($response->hasMessage()) {
                $messages[] = $response->getMessage();
            } else {
                // Log status for debugging
                if ($response->hasStatus()) {
                    $statusCode = $response->getStatus()->getCode();
                    $statusMessage = $response->getStatus()->getMessage();
                    Logger::debug("ReceiveMessage response - Code: {}, Message: {}, clientId={}", [
                        $statusCode,
                        $statusMessage,
                        $this->clientId
                    ]);
                }
            }
        }
        
        Logger::info("Received {} messages, topic={}, clientId={}", [
            count($messages),
            $this->topic,
            $this->clientId
        ]);
        
        return $messages;
    }
    
    /**
     * Acknowledge message (ACK, with retry)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @return bool Whether successful
     * @throws \Exception If acknowledgement fails
     */
    public function ack($message)
    {
        return $this->ackWithRetry($message);
    }
    
    /**
     * Acknowledge message (internal implementation, with retry)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @param int $attempt Current attempt count
     * @return bool Whether successful
     * @throws \Exception If acknowledgement fails
     */
    private function ackWithRetry($message, $attempt = 1)
    {
        try {
            return $this->ackInternal($message);
        } catch (\Exception $e) {
            if (!$this->retryPolicy->shouldRetry($attempt, $e)) {
                throw $e;
            }
            
            $delayMs = $this->retryPolicy->getNextAttemptDelay($attempt);
            if ($delayMs > 0) {
                usleep($delayMs * 1000);
            }
            
            return $this->ackWithRetry($message, $attempt + 1);
        }
    }
    
    /**
     * Acknowledge message (internal implementation, no retry)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @return bool Whether successful
     * @throws \Exception If acknowledgement fails
     */
    private function ackInternal($message)
    {
        // Check state - only RUNNING state can ack messages
        if ($this->state !== 'RUNNING') {
            Logger::error("Unable to ack message because consumer is not running, state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \Exception("Simple consumer is not running now");
        }
        
        $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
        $messageId = $message->getSystemProperties()->getMessageId();
        
        if (empty($receiptHandle)) {
            Logger::error("Message has no receipt_handle, cannot ACK, messageId={}, clientId={}", [
                $messageId,
                $this->clientId
            ]);
            throw new \Exception("Message has no receipt_handle, cannot ACK");
        }
        
        Logger::debug("Acknowledging message, messageId={}, topic={}, clientId={}", [
            $messageId,
            $this->topic,
            $this->clientId
        ]);
        
        $request = new AckMessageRequest();
        
        // Set consumer group (required)
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        // Set topic (required) - extract from message or use configured topic
        $topicName = $this->topic;
        if ($message->hasTopic()) {
            $topicName = $message->getTopic()->getName();
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topicName);
        $request->setTopic($topicResource);
        
        $entry = new AckMessageEntry();
        $entry->setReceiptHandle($receiptHandle);
        $entry->setMessageId($messageId);
        
        $request->setEntries([$entry]);
        
        $call = $this->getClient()->AckMessage($request);
        list($response, $status) = $call->wait();
        
        // Check gRPC status
        if ($status->code !== STATUS_OK) {
            Logger::error("gRPC call failed when ACK message, messageId={}, error={}, clientId={}", [
                $messageId,
                $status->details,
                $this->clientId
            ]);
            throw new \Exception(
                "gRPC call failed: " . $status->details,
                $status->code
            );
        }
        
        // Check response status (20000 means OK in RocketMQ)
        if ($response->getStatus()->getCode() !== 20000) {
            Logger::error("Failed to ACK message, messageId={}, code={}, message={}, clientId={}", [
                $messageId,
                $response->getStatus()->getCode(),
                $response->getStatus()->getMessage(),
                $this->clientId
            ]);
            throw new \Exception(
                "Failed to ACK message: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
        
        Logger::info("Message acknowledged successfully, messageId={}, topic={}, clientId={}", [
            $messageId,
            $topicName,
            $this->clientId
        ]);
        
        return true;
    }
    
    /**
     * Change message visibility duration
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @param int $durationSeconds New invisible duration (seconds)
     * @return string New receipt handle
     * @throws \Exception If change fails
     */
    public function changeInvisibleDuration($message, $durationSeconds)
    {
        // Check state - only RUNNING state can change invisible duration
        if ($this->state !== 'RUNNING') {
            Logger::error("Unable to change invisible duration because consumer is not running, state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \Exception("Simple consumer is not running now");
        }
        
        $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
        $messageId = $message->getSystemProperties()->getMessageId();
        
        if (empty($receiptHandle)) {
            Logger::error("Message has no receipt_handle, cannot change visibility duration, messageId={}, clientId={}", [
                $messageId,
                $this->clientId
            ]);
            throw new \Exception("Message has no receipt_handle, cannot change visibility duration");
        }
        
        Logger::debug("Changing invisible duration, messageId={}, duration={}s, clientId={}", [
            $messageId,
            $durationSeconds,
            $this->clientId
        ]);
        
        $request = new \Apache\Rocketmq\V2\ChangeInvisibleDurationRequest();
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration(
            (new \Google\Protobuf\Duration())->setSeconds($durationSeconds)
        );
        
        $mq = new MessageQueue();
        $resource = new Resource();
        $resource->setName($this->topic);
        $mq->setTopic($resource);
        $request->setMessageQueue($mq);
        
        $call = $this->getClient()->ChangeInvisibleDuration($request);
        list($response, $status) = $call->wait();
        
        // Check gRPC status
        if ($status->code !== STATUS_OK) {
            Logger::error("gRPC call failed when changing invisible duration, messageId={}, error={}, clientId={}", [
                $messageId,
                $status->details,
                $this->clientId
            ]);
            throw new \Exception(
                "gRPC call failed: " . $status->details,
                $status->code
            );
        }
        
        // Check response status (20000 means OK in RocketMQ)
        if ($response->getStatus()->getCode() !== 20000) {
            Logger::error("Failed to change visibility duration, messageId={}, code={}, message={}, clientId={}", [
                $messageId,
                $response->getStatus()->getCode(),
                $response->getStatus()->getMessage(),
                $this->clientId
            ]);
            throw new \Exception(
                "Failed to change visibility duration: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
        
        $newReceiptHandle = $response->getReceiptHandle();
        
        Logger::info("Changed invisible duration successfully, messageId={}, duration={}s, clientId={}", [
            $messageId,
            $durationSeconds,
            $this->clientId
        ]);
        
        return $newReceiptHandle;
    }
    


    
    /**
     * Close consumer
     * 
     * @return void
     */
    public function close()
    {
        if ($this->state === 'TERMINATED' || $this->state === 'STOPPING') {
            return;
        }
        
        $this->state = 'STOPPING';
        
        // Stop background heartbeat timer
        if ($this->heartbeatTimerPid !== null && $this->heartbeatTimerPid > 0) {
            Logger::info("Stopping background heartbeat timer (PID: {$this->heartbeatTimerPid}), clientId={$this->clientId}");
            posix_kill($this->heartbeatTimerPid, SIGTERM);
            pcntl_waitpid($this->heartbeatTimerPid, $status);
            $this->heartbeatTimerPid = null;
            Logger::info("Background heartbeat timer stopped, clientId={$this->clientId}");
        }
        
        // Shutdown meter manager
        if ($this->meterManager !== null) {
            $this->meterManager->shutdown();
            Logger::info("Meter manager shutdown, clientId={}", [$this->clientId]);
        }
        
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
        
        $this->state = 'TERMINATED';
    }
    
    /**
     * Get current state
     * 
     * @return string State name
     */
    public function getState()
    {
        return $this->state;
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
     * Get Retry policy
     * 
     * @return RetryPolicy Retry policy
     */
    public function getRetryPolicy()
    {
        return $this->retryPolicy;
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
     * Set await duration in seconds
     *
     * @param int $awaitDuration
     * @return void
     */
    public function setAwaitDuration($awaitDuration)
    {
        $this->awaitDuration = $awaitDuration;
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
     * Get await duration
     *
     * @return int
     */
    public function getAwaitDuration()
    {
        return $this->awaitDuration;
    }
    
    /**
     * Get Telemetry Session (for debugging and monitoring)
     * 
     * @return TelemetrySession|null
     */
    public function getTelemetrySession()
    {
        return $this->telemetrySession;
    }
}

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
     * Send heartbeat to register consumer with server
     * 
     * @return void
     * @throws \Exception If heartbeat fails
     */
    public function heartbeat()
    {
        $request = new HeartbeatRequest();
        
        // Set client type
        $request->setClientType(\Apache\Rocketmq\V2\ClientType::PUSH_CONSUMER);
        
        // Set consumer group
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        $call = $this->getClient()->Heartbeat($request);
        list($response, $status) = $call->wait();
        
        // Check gRPC status
        if ($status->code !== STATUS_OK) {
            throw new \Exception(
                "gRPC call failed: " . $status->details,
                $status->code
            );
        }
        
        // Check response status (20000 means OK in RocketMQ)
        if ($response->getStatus()->getCode() !== 20000) {
            throw new \Exception(
                "Failed to send heartbeat: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
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
    public function start()
    {
        if ($this->messageListener === null) {
            throw new \Exception("Message listener must be set");
        }
        
        // Check Swoole availability
        if (!extension_loaded('swoole')) {
            throw new \Exception(
                "PushConsumer requires Swoole extension for concurrent consumption. " .
                "Please install it: pecl install swoole"
            );
        }
        
        $this->running = true;
        $this->consumptionTaskQueue = new \SplQueue();
        
        Logger::info("PushConsumer started with concurrent consumption, clientId={}, consumptionThreads={}, maxCacheCount={}, maxCacheSize={}MB", [
            $this->clientId,
            $this->consumptionThreadCount,
            $this->maxCacheMessageCount,
            round($this->maxCacheMessageSizeInBytes / 1024 / 1024, 2)
        ]);
        
        // Create SimpleConsumer to pull messages
        $simpleConsumer = SimpleConsumer::getInstance($this->config, $this->consumerGroup, $this->topic);
        
        // Start consumption workers
        $this->startConsumptionWorkers($simpleConsumer);
        
        // Main loop: receive messages and dispatch to workers (wrapped in coroutine)
        \Swoole\Coroutine::create(function() use ($simpleConsumer) {
            while ($this->running) {
                try {
                    $this->metrics['receptionTimes']++;
                    
                    $messages = $simpleConsumer->receive($this->maxMessageNum, $this->invisibleDuration);
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
                            $this->dispatchMessage($message, $simpleConsumer);
                        }
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
    }
    
    /**
     * Start consumer in background (non-blocking mode)
     * 
     * @return void
     * @throws \Exception If message listener is not set
     */
    public function startAsync()
    {
        if ($this->messageListener === null) {
            throw new \Exception("Message listener must be set");
        }
        
        $this->running = true;
        
        echo "PushConsumer starting in background...\n";
        
        // Use pcntl_fork to run consumer in background (if available)
        if (function_exists('pcntl_fork')) {
            $pid = pcntl_fork();
            
            if ($pid == -1) {
                throw new \Exception("Failed to fork process");
            } elseif ($pid > 0) {
                // Parent process returns immediately
                echo "PushConsumer started in background (PID: {$pid})\n";
                return;
            }
            // Child process continues to run consumer
        }
        
        // Run consumer loop
        $simpleConsumer = SimpleConsumer::getInstance($this->config, $this->consumerGroup, $this->topic);
        
        while ($this->running) {
            try {
                $messages = $simpleConsumer->receive($this->maxMessageNum, $this->invisibleDuration);
                
                foreach ($messages as $message) {
                    try {
                        // Call message listener
                        $result = call_user_func($this->messageListener, $message);
                        
                        // Decide whether to ACK based on return value
                        if ($result === true || $result === null) {
                            $simpleConsumer->ack($message);
                        }
                    } catch (\Exception $e) {
                        Logger::error("Failed to process message, clientId={$this->clientId}", ['error' => $e->getMessage()]);
                    }
                }
            } catch (\Exception $e) {
                Logger::error("Failed to receive message, clientId={$this->clientId}", ['error' => $e->getMessage()]);
                sleep(1);
            }
        }
    }
    
    /**
     * Poll messages once (non-blocking)
     * 
     * @return int Number of messages consumed
     * @throws \Exception If message listener is not set or receive fails
     */
    public function pollOnce()
    {
        if ($this->messageListener === null) {
            throw new \Exception("Message listener must be set");
        }
        
        if (!$this->running) {
            $this->running = true;
            
            // Send heartbeat to register consumer with server
            try {
                $this->heartbeat();
            } catch (\Exception $e) {
                Logger::warn("Failed to send heartbeat, clientId={$this->clientId}", ['error' => $e->getMessage()]);
                // Continue anyway, heartbeat is not critical for polling
            }
        }
        
        // Create SimpleConsumer to pull messages
        $simpleConsumer = SimpleConsumer::getInstance($this->config, $this->consumerGroup, $this->topic);
        
        try {
            $messages = $simpleConsumer->receive($this->maxMessageNum, $this->invisibleDuration);
            $count = 0;
            
            foreach ($messages as $grpcMessage) {
                try {
                    // Convert gRPC Message to a simple object that listener can use
                    $messageView = $this->convertToMessageView($grpcMessage);
                    
                    // Call message listener with the converted message
                    $result = call_user_func($this->messageListener, $messageView);
                    
                    // Decide whether to ACK based on return value
                    if ($result === \Apache\Rocketmq\Consumer\ConsumeResult::SUCCESS || $result === true || $result === null) {
                        $simpleConsumer->ack($grpcMessage);
                    }
                    
                    $count++;
                } catch (\Exception $e) {
                    Logger::error("Failed to process message, clientId={$this->clientId}", ['error' => $e->getMessage()]);
                    // Do not ACK on error, let message be redelivered
                }
            }
            
            return $count;
        } catch (\Exception $e) {
            Logger::error("Failed to receive message, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            return 0;
        }
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
}