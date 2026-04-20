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

// Load AsyncTelemetrySession if Swoole is available
if (extension_loaded('swoole')) {
    require_once __DIR__ . '/AsyncTelemetrySession.php';
}

use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression as V2FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;
use Apache\Rocketmq\Consumer\SubscriptionLoadBalancer;
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
     * @var array<string, FilterExpression> Subscription expressions map (topic -> FilterExpression)
     * Aligned with Java SimpleConsumerImpl.subscriptionExpressions
     */
    private $subscriptionExpressions = [];
    
    /**
     * @var array<string, \Apache\Rocketmq\Consumer\SubscriptionLoadBalancer> Subscription load balancer cache (topic => SubscriptionLoadBalancer)
     * Aligned with Java SimpleConsumerImpl.subscriptionRouteDataCache
     * Each topic has its own load balancer with independent round-robin index
     */
    private $subscriptionRouteDataCache = [];
    
    /**
     * @var int Round-robin index for topic selection (multi-topic support)
     * Initialized with random value, aligned with Java RandomUtils.nextInt(0, Integer.MAX_VALUE)
     */
    private $topicIndex = 0;
    
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
        
        // Initialize topic index with random value (aligned with Java RandomUtils.nextInt(0, Integer.MAX_VALUE))
        // This ensures different consumers start from different topics in multi-topic scenarios
        $this->topicIndex = mt_rand(0, PHP_INT_MAX);
        
        // NOTE: No automatic subscription in constructor anymore
        // Users must explicitly call subscribe() to add subscriptions
        // This aligns with Java SimpleConsumerImpl design
        
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
     * Check if consumer is in RUNNING state.
     * Aligned with Java SimpleConsumerImpl.checkRunning().
     * 
     * @throws \RuntimeException If consumer is not running
     */
    private function checkRunning(): void
    {
        if ($this->state !== 'RUNNING') {
            Logger::error("Simple consumer is not running, state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \RuntimeException("Simple consumer is not running now");
        }
    }
    
    /**
     * Subscribe to a topic with a filter expression.
     * Aligned with Java SimpleConsumerImpl.subscribe(topic, filterExpression).
     *
     * @param string $topic Topic to subscribe
     * @param FilterExpression $filterExpression Filter expression for the topic
     * @return $this
     */
    public function subscribe(string $topic, FilterExpression $filterExpression): self
    {
        // Allow subscription in CREATED or RUNNING state (aligned with Java builder pattern)
        // Java sets subscriptions during build(), PHP allows it before or after start()
        if ($this->state !== 'CREATED' && $this->state !== 'RUNNING') {
            Logger::error("Cannot subscribe when consumer is in state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \RuntimeException("Cannot subscribe when consumer is not in CREATED or RUNNING state");
        }
        
        // Aligned with Java: query route data first before adding subscription
        // If route query fails, exception will be thrown (strict alignment with Java behavior)
        $this->getRouteData($topic);
        
        $this->subscriptionExpressions[$topic] = $filterExpression;
        
        // Clear load balancer for this specific topic when subscription changes
        // This ensures fresh route data on next receive()
        if (isset($this->subscriptionRouteDataCache[$topic])) {
            unset($this->subscriptionRouteDataCache[$topic]);
            Logger::debug("Cleared load balancer for resubscribed topic, topic={}, clientId={}", [
                $topic,
                $this->clientId
            ]);
        }
        
        Logger::info("Subscribed topic with filter expression, topic={}, expression={}, type={}, clientId={}", [
            $topic, $filterExpression->getExpression(), $filterExpression->getType()->value(), $this->clientId
        ]);
        return $this;
    }
    
    /**
     * Unsubscribe from a topic.
     * Aligned with Java SimpleConsumerImpl.unsubscribe(topic).
     *
     * @param string $topic Topic to unsubscribe
     * @return $this
     */
    public function unsubscribe(string $topic): self
    {
        // Allow unsubscription in CREATED or RUNNING state
        if ($this->state !== 'CREATED' && $this->state !== 'RUNNING') {
            Logger::error("Cannot unsubscribe when consumer is in state={}, clientId={}", [
                $this->state,
                $this->clientId
            ]);
            throw new \RuntimeException("Cannot unsubscribe when consumer is not in CREATED or RUNNING state");
        }
        
        unset($this->subscriptionExpressions[$topic]);
        
        // Clear load balancer for this topic (aligned with Java SimpleConsumerImpl)
        if (isset($this->subscriptionRouteDataCache[$topic])) {
            unset($this->subscriptionRouteDataCache[$topic]);
            Logger::debug("Cleared load balancer for unsubscribed topic, topic={}, clientId={}", [
                $topic,
                $this->clientId
            ]);
        }
        
        Logger::info("Unsubscribed topic, topic={}, clientId={}", [$topic, $this->clientId]);
        return $this;
    }
    
    /**
     * Get current subscription expressions.
     *
     * @return array<string, FilterExpression>
     */
    public function getSubscriptionExpressions(): array
    {
        return $this->subscriptionExpressions;
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
     * Get route data for a topic.
     * Aligned with Java ClientImpl.getRouteData(topic).
     * Queries the route from broker and caches readable queues in subscriptionRouteDataCache.
     *
     * @param string $topic Topic to query route
     * @return void
     * @throws \Exception If route query fails
     */
    private function getRouteData(string $topic): void
    {
        // Check if already cached in subscriptionRouteDataCache
        if (!empty($this->subscriptionRouteDataCache[$topic])) {
            Logger::debug("Load balancer already cached, topic={}, queueCount={}, clientId={}", [
                $topic,
                $this->subscriptionRouteDataCache[$topic]->getQueueCount(),
                $this->clientId
            ]);
            return;
        }
        
        // Query route and create SubscriptionLoadBalancer
        $queues = $this->queryTopicRouteForTopic($topic);
        $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($queues);
        
        Logger::info("Created load balancer for subscribed topic, topic={}, queueCount={}, clientId={}", [
            $topic,
            count($queues),
            $this->clientId
        ]);
    }
    
    /**
     * Query topic route and cache readable master queues.
     * Aligned with Java SimpleConsumerImpl.getSubscriptionLoadBalancer() flow.
     * Uses per-topic isolated cache (subscriptionRouteDataCache).
     *
     * @return array Array of MessageQueue objects
     * @throws \Exception If route query fails
     */
    private function queryTopicRoute(): array
    {
        // Use subscriptionRouteDataCache for the configured topic
        if (!empty($this->subscriptionRouteDataCache[$this->topic])) {
            return $this->subscriptionRouteDataCache[$this->topic]->getMessageQueues();
        }

        $request = new QueryRouteRequest();
        $topicResource = new Resource();
        $topicResource->setName($this->topic);
        $request->setTopic($topicResource);

        [$response, $status] = $this->getClient()->QueryRoute($request)->wait();

        if ($status->code !== \Grpc\STATUS_OK) {
            throw new \Exception("Failed to query route for topic {$this->topic}: {$status->details}");
        }

        $queues = [];
        foreach ($response->getMessageQueues() as $mq) {
            // Filter: only readable master queues (aligned with Java SubscriptionLoadBalancer.isReadableMasterQueue)
            // Java: mq.getPermission().isReadable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId()
            $perm = $mq->getPermission();
            $isReadable = ($perm === Permission::READ || $perm === Permission::READ_WRITE);
            
            // Check if it's master broker (broker ID = 0, aligned with Java Utilities.MASTER_BROKER_ID)
            $brokerId = $mq->getBroker()->getId();
            $isMaster = ($brokerId === 0);
            
            Logger::info("Queue route info, topic={}, brokerId={}, perm={}, isReadable={}, isMaster={}", [
                $this->topic,
                $brokerId,
                \Apache\Rocketmq\V2\Permission::name($perm),
                $isReadable ? 'true' : 'false',
                $isMaster ? 'true' : 'false'
            ]);
            
            if ($isReadable && $isMaster) {
                $queues[] = $mq;
            }
        }

        if (empty($queues)) {
            throw new \Exception("No readable queue found for topic {$this->topic}");
        }

        // Create and cache SubscriptionLoadBalancer (per-topic isolation)
        $this->subscriptionRouteDataCache[$this->topic] = new SubscriptionLoadBalancer($queues);
        
        Logger::info("Created load balancer for topic, topic={}, readableQueues={}, clientId={}", [
            $this->topic, count($queues), $this->clientId
        ]);

        return $queues;
    }
    
    /**
     * Select a message queue using round-robin.
     * Aligned with Java SubscriptionLoadBalancer.takeMessageQueue().
     *
     * @return MessageQueue
     */
    private function takeMessageQueue(): MessageQueue
    {
        $queues = $this->queryTopicRoute();
        $index = $this->queueIndex % count($queues);
        $this->queueIndex++;
        return $queues[$index];
    }
    
    /**
     * Select a message queue for specific topic using round-robin.
     * Supports multi-topic subscription by caching routes per topic.
     * Aligned with Java SimpleConsumerImpl.getSubscriptionLoadBalancer() + takeMessageQueue().
     *
     * @param string $topic Topic name
     * @return MessageQueue Selected message queue
     * @throws \Exception If route query fails or no readable queue found
     */
    private function takeMessageQueueForTopic(string $topic): MessageQueue
    {
        // Check if load balancer is cached for this topic
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            // Query route and create load balancer
            $queues = $this->queryTopicRouteForTopic($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($queues);
            
            Logger::debug("Created load balancer for topic, topic={}, queueCount={}, clientId={}", [
                $topic,
                count($queues),
                $this->clientId
            ]);
        }
        
        // Use SubscriptionLoadBalancer to select queue (each topic has independent index)
        return $this->subscriptionRouteDataCache[$topic]->takeMessageQueue();
    }
    
    /**
     * Query topic route for specific topic and filter readable master queues.
     * Similar to queryTopicRoute() but supports arbitrary topic parameter.
     *
     * @param string $topic Topic name
     * @return array Array of MessageQueue objects
     * @throws \Exception If route query fails
     */
    private function queryTopicRouteForTopic(string $topic): array
    {
        $request = new QueryRouteRequest();
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $request->setTopic($topicResource);

        [$response, $status] = $this->getClient()->QueryRoute($request)->wait();

        if ($status->code !== \Grpc\STATUS_OK) {
            throw new \Exception("Failed to query route for topic {$topic}: {$status->details}");
        }

        $queues = [];
        foreach ($response->getMessageQueues() as $mq) {
            // Filter: only readable master queues (aligned with Java SubscriptionLoadBalancer.isReadableMasterQueue)
            // Java: mq.getPermission().isReadable() && Utilities.MASTER_BROKER_ID == mq.getBroker().getId()
            $perm = $mq->getPermission();
            $isReadable = ($perm === Permission::READ || $perm === Permission::READ_WRITE);
            
            // Check if it's master broker (broker ID = 0, aligned with Java Utilities.MASTER_BROKER_ID)
            $brokerId = $mq->getBroker()->getId();
            $isMaster = ($brokerId === 0);
            
            Logger::info("Queue route info, topic={}, brokerId={}, perm={}, isReadable={}, isMaster={}", [
                $topic,
                $brokerId,
                \Apache\Rocketmq\V2\Permission::name($perm),
                $isReadable ? 'true' : 'false',
                $isMaster ? 'true' : 'false'
            ]);
            
            if ($isReadable && $isMaster) {
                $queues[] = $mq;
            }
        }

        if (empty($queues)) {
            throw new \Exception("No readable queue found for topic {$topic}");
        }

        Logger::info("Queried topic route, topic={}, readableQueues={}, clientId={}", [
            $topic, count($queues), $this->clientId
        ]);

        return $queues;
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
     * Receive messages asynchronously using Swoole coroutine.
     * Aligned with Java SimpleConsumerImpl.receiveAsync().
     * 
     * @param int $maxMessageNum Maximum message count
     * @param int $invisibleDuration Message invisible duration (seconds)
     * @return \Swoole\Coroutine\Channel Channel that will receive the result
     * @throws \Exception If Swoole extension is not available
     */
    public function receiveAsync(int $maxMessageNum, int $invisibleDuration)
    {
        if (!extension_loaded('swoole')) {
            throw new \RuntimeException("Swoole extension is required for async operations");
        }
        
        // Create a channel to receive the result
        $channel = new \Swoole\Coroutine\Channel(1);
        
        // Execute in a new coroutine
        \Swoole\Coroutine::create(function() use ($maxMessageNum, $invisibleDuration, $channel) {
            try {
                $result = $this->receive($maxMessageNum, $invisibleDuration);
                $channel->push(['success' => true, 'data' => $result]);
            } catch (\Exception $e) {
                $channel->push(['success' => false, 'error' => $e]);
            }
        });
        
        return $channel;
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
        // Check state - only RUNNING state can receive messages (aligned with Java)
        $this->checkRunning();
        
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
     * Aligned with Java SimpleConsumerImpl.receive0() - supports multi-topic round-robin
     * 
     * @param int|null $maxMessageNum Maximum message count
     * @param int|null $invisibleDuration Message invisible duration (seconds)
     * @return array Message list
     * @throws \Exception If receive fails
     */
    private function receiveInternal($maxMessageNum = null, $invisibleDuration = null)
    {
        // Check state using unified method (aligned with Java)
        $this->checkRunning();
        
        // Validate parameters
        $maxMessageNum = $maxMessageNum ?? $this->maxMessageNum;  // Use null coalescing operator
        if ($maxMessageNum <= 0) {
            Logger::error("maxMessageNum must be greater than 0, maxMessageNum={}, clientId={}", [
                $maxMessageNum,
                $this->clientId
            ]);
            throw new \InvalidArgumentException("maxMessageNum must be greater than 0");
        }
        
        $invisibleDuration = $invisibleDuration ?? $this->invisibleDuration;
        
        // Validate invisibleDuration (aligned with Java Duration type safety)
        if ($invisibleDuration <= 0) {
            Logger::error("invisibleDuration must be greater than 0, invisibleDuration={}, clientId={}", [
                $invisibleDuration,
                $this->clientId
            ]);
            throw new \InvalidArgumentException("invisibleDuration must be greater than 0");
        }
        
        // Check if has subscriptions (aligned with Java L162-164)
        if (empty($this->subscriptionExpressions)) {
            throw new \InvalidArgumentException("There is no topic to receive message");
        }
        
        // Round-robin select topic from all subscribed topics (aligned with Java L159-166)
        $topics = array_keys($this->subscriptionExpressions);
        $topicIndexValue = $this->topicIndex % count($topics);
        $this->topicIndex++;
        $selectedTopic = $topics[$topicIndexValue];
        
        // Get filter expression for selected topic
        $filterExpression = $this->subscriptionExpressions[$selectedTopic];
        
        Logger::debug("Receiving messages, selectedTopic={}, maxMessageNum={}, invisibleDuration={}s, awaitDuration={}s, clientId={}", [
            $selectedTopic,
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
        
        // Get message queue from route query for selected topic
        // Aligned with Java SimpleConsumerImpl: route query -> load balancer -> takeMessageQueue
        $mq = $this->takeMessageQueueForTopic($selectedTopic);
        $request->setMessageQueue($mq);
        
        // Set filter expression from subscription (required - missing this causes server NPE)
        // Aligned with Java SimpleConsumerImpl: filterExpression = subscriptionExpressions.get(topic)
        $v2Filter = new V2FilterExpression();
        $v2Filter->setExpression($filterExpression->getExpression());
        $v2Filter->setType(
            $filterExpression->getType() === FilterExpressionType::SQL92
                ? FilterType::SQL
                : FilterType::TAG
        );
        $request->setFilterExpression($v2Filter);
        
        // Set invisible duration (required for simple consumer)
        $invisibleDurationProto = new \Google\Protobuf\Duration();
        $invisibleDurationProto->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationProto);
        
        // Set batch size
        $request->setBatchSize($maxMessageNum);
        
        // SimpleConsumer uses auto_renew=false (Java ConsumerImpl line 275)
        // PushConsumer uses auto_renew=true
        $request->setAutoRenew(false);
        
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
            $selectedTopic,
            $this->clientId
        ]);
        
        return $messages;
    }
    
    /**
     * Acknowledge message (ACK, with retry)
     * Aligned with Java SimpleConsumerImpl.ack() - returns void
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @return void
     * @throws \Exception If acknowledgement fails
     */
    public function ack($message): void
    {
        $this->ackWithRetry($message);
    }
    
    /**
     * Acknowledge message asynchronously using Swoole coroutine.
     * Aligned with Java SimpleConsumerImpl.ackAsync().
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @return \Swoole\Coroutine\Channel Channel that will receive the result
     * @throws \Exception If Swoole extension is not available
     */
    public function ackAsync($message)
    {
        if (!extension_loaded('swoole')) {
            throw new \RuntimeException("Swoole extension is required for async operations");
        }
        
        // Create a channel to receive the result
        $channel = new \Swoole\Coroutine\Channel(1);
        
        // Execute in a new coroutine
        \Swoole\Coroutine::create(function() use ($message, $channel) {
            try {
                $this->ack($message);
                $channel->push(['success' => true]);
            } catch (\Exception $e) {
                $channel->push(['success' => false, 'error' => $e]);
            }
        });
        
        return $channel;
    }
    
    /**
     * Acknowledge message (internal implementation, with retry)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @param int $attempt Current attempt count
     * @return void
     * @throws \Exception If acknowledgement fails
     */
    private function ackWithRetry($message, $attempt = 1): void
    {
        try {
            $this->ackInternal($message);
        } catch (\Exception $e) {
            if (!$this->retryPolicy->shouldRetry($attempt, $e)) {
                throw $e;
            }
            
            $delayMs = $this->retryPolicy->getNextAttemptDelay($attempt);
            if ($delayMs > 0) {
                usleep($delayMs * 1000);
            }
            
            $this->ackWithRetry($message, $attempt + 1);
        }
    }
    
    /**
     * Acknowledge message (internal implementation, no retry)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object
     * @return void
     * @throws \Exception If acknowledgement fails
     */
    private function ackInternal($message): void
    {
        // Check state using unified method
        $this->checkRunning();
        
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
    }
    
    /**
     * Change message visibility duration
     * Aligned with Java SimpleConsumerImpl.changeInvisibleDuration() - returns void and updates message
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object (will be updated with new receipt handle)
     * @param int $durationSeconds New invisible duration (seconds)
     * @return void
     * @throws \Exception If change fails
     */
    public function changeInvisibleDuration($message, $durationSeconds): void
    {
        // Check state using unified method
        $this->checkRunning();
        
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
        
        // Update message's receipt handle (aligned with Java)
        $newReceiptHandle = $response->getReceiptHandle();
        $message->getSystemProperties()->setReceiptHandle($newReceiptHandle);
        
        Logger::info("Changed invisible duration successfully, messageId={}, duration={}s, clientId={}", [
            $messageId,
            $durationSeconds,
            $this->clientId
        ]);
    }
    
    /**
     * Change message visibility duration asynchronously using Swoole coroutine.
     * Aligned with Java SimpleConsumerImpl.changeInvisibleDurationAsync().
     * 
     * @param \Apache\Rocketmq\V2\Message $message Message object (will be updated with new receipt handle)
     * @param int $durationSeconds New invisible duration (seconds)
     * @return \Swoole\Coroutine\Channel Channel that will receive the result
     * @throws \Exception If Swoole extension is not available
     */
    public function changeInvisibleDurationAsync($message, int $durationSeconds)
    {
        if (!extension_loaded('swoole')) {
            throw new \RuntimeException("Swoole extension is required for async operations");
        }
        
        // Create a channel to receive the result
        $channel = new \Swoole\Coroutine\Channel(1);
        
        // Execute in a new coroutine
        \Swoole\Coroutine::create(function() use ($message, $durationSeconds, $channel) {
            try {
                $this->changeInvisibleDuration($message, $durationSeconds);
                $channel->push(['success' => true]);
            } catch (\Exception $e) {
                $channel->push(['success' => false, 'error' => $e]);
            }
        });
        
        return $channel;
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
    
    /**
     * Callback for topic route data update from Telemetry.
     * Aligned with Java SimpleConsumerImpl.onTopicRouteDataUpdate0().
     * 
     * This method is called when the broker pushes updated route information via Telemetry session.
     * It updates the SubscriptionLoadBalancer for the specified topic while preserving the round-robin index.
     * 
     * @param string $topic Topic name
     * @param array $messageQueues Updated message queues from route data
     * @return void
     */
    public function onTopicRouteDataUpdate(string $topic, array $messageQueues): void
    {
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            // Create new load balancer if not exists
            try {
                $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($messageQueues);
                Logger::info("Created load balancer from telemetry update, topic={}, queueCount={}, clientId={}", [
                    $topic,
                    count($messageQueues),
                    $this->clientId
                ]);
            } catch (\InvalidArgumentException $e) {
                Logger::warn("Failed to create load balancer from telemetry update, topic={}, clientId={}, error={}", [
                    $topic,
                    $this->clientId,
                    $e->getMessage()
                ]);
            }
            return;
        }
        
        // Update existing load balancer (preserves round-robin index)
        try {
            $oldLoadBalancer = $this->subscriptionRouteDataCache[$topic];
            $this->subscriptionRouteDataCache[$topic] = $oldLoadBalancer->update($messageQueues);
            
            Logger::debug("Updated load balancer from telemetry, topic={}, oldQueueCount={}, newQueueCount={}, preservedIndex={}, clientId={}", [
                $topic,
                $oldLoadBalancer->getQueueCount(),
                $this->subscriptionRouteDataCache[$topic]->getQueueCount(),
                $this->subscriptionRouteDataCache[$topic]->getIndex(),
                $this->clientId
            ]);
        } catch (\InvalidArgumentException $e) {
            Logger::warn("Failed to update load balancer from telemetry, topic={}, clientId={}, error={}", [
                $topic,
                $this->clientId,
                $e->getMessage()
            ]);
        }
    }
}
