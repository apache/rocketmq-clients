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
use Apache\Rocketmq\V2\MessageQueue as V2MessageQueue;
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
            Logger::info("Sending heartbeat..., clientId={}", [$this->clientId]);
            $this->heartbeat();
            Logger::info("Heartbeat sent, clientId={}", [$this->clientId]);
            
            // Skip Telemetry session for now - it's not working with official gRPC extension in Swoole
            Logger::warn("Skipping Telemetry session initialization (known issue with gRPC bidirectional streams in Swoole), clientId={}", [$this->clientId]);
            $this->telemetrySession = null;
            
            // Sync initial settings to server after telemetry session is ready (only if has subscriptions)
            if (!empty($this->subscriptionExpressions)) {
                Logger::warn("Skipping initial settings sync because Telemetry session is disabled, clientId={}", [$this->clientId]);
            }
            
            // Start background timers for heartbeat and settings sync
            Logger::info("Starting background timers..., clientId={}", [$this->clientId]);
            $this->startBackgroundTimers();
            Logger::info("Background timers started, clientId={}", [$this->clientId]);
            
            $this->state = 'RUNNING';
            Logger::info("SimpleConsumer started successfully, state=RUNNING, clientId={}", [$this->clientId]);
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
        }
        
        // Sync settings to server if consumer is running (aligned with Java)
        // This notifies the server about the new subscription
        if ($this->state === 'RUNNING') {
            // Settings sync is critical for subscription changes
            $this->syncSettings();
        }
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
        }
        
        // Sync settings to server if consumer is running (aligned with Java)
        // This notifies the server about the removed subscription
        if ($this->state === 'RUNNING') {
            // Settings sync is critical for unsubscription changes
            $this->syncSettings();
        }
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
            return;
        }
        
        // Query route and create SubscriptionLoadBalancer
        $queues = $this->queryTopicRouteForTopic($topic);
        $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($queues);
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
        $topicResource->setResourceNamespace($this->config->getNamespace());
        $topicResource->setName($this->topic);
        $request->setTopic($topicResource);
        $request->setEndpoints($this->config->getEndpointsAsProtobuf());

        $startTime = microtime(true);
        [$response, $status] = $this->getClient()->QueryRoute($request)->wait();
        $elapsed = round((microtime(true) - $startTime) * 1000, 2);

        if ($status->code !== \Grpc\STATUS_OK) {
            Logger::error("RPC QueryRoute failed, topic={}, status_code={}, details={}, elapsed={}ms, clientId={}", [
                $this->topic,
                $status->code,
                $status->details,
                $elapsed,
                $this->clientId
            ]);
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
            
            if ($isReadable && $isMaster) {
                $queues[] = $mq;
            }
        }

        if (empty($queues)) {
            throw new \Exception("No readable queue found for topic {$this->topic}");
        }

        // Create and cache SubscriptionLoadBalancer (per-topic isolation)
        $this->subscriptionRouteDataCache[$this->topic] = new SubscriptionLoadBalancer($queues);

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
    private function takeMessageQueueForTopic(string $topic): \Apache\Rocketmq\V2\MessageQueue
    {
        // Check if load balancer is cached for this topic
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            // Query route and create load balancer
            $queues = $this->queryTopicRouteForTopic($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($queues);
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
        $topicResource->setResourceNamespace($this->config->getNamespace());
        $topicResource->setName($topic);
        $request->setTopic($topicResource);
        $request->setEndpoints($this->config->getEndpointsAsProtobuf());
        
        // Debug: log request parameters
        error_log("DEBUG QueryRoute Request: topic={$topic}, namespace=" . $this->config->getNamespace());
        $endpointsProto = $this->config->getEndpointsAsProtobuf();
        if ($endpointsProto) {
            $scheme = $endpointsProto->getScheme();
            $addresses = $endpointsProto->getAddresses();
            error_log("DEBUG QueryRoute Request: endpoints scheme={$scheme}, count=" . count($addresses));
            foreach ($addresses as $addr) {
                error_log("DEBUG QueryRoute Request: endpoint host=" . $addr->getHost() . ", port=" . $addr->getPort());
            }
            
            // Log the full protobuf object for debugging
            $serialized = $endpointsProto->serializeToString();
            error_log("DEBUG QueryRoute Request: endpoints serialized length=" . strlen($serialized));
            if (strlen($serialized) > 0) {
                error_log("DEBUG QueryRoute Request: endpoints hex=" . bin2hex($serialized));
            } else {
                error_log("WARNING: Endpoints serialization returned empty string!");
                // Try to inspect the object
                error_log("DEBUG: Endpoints object scheme=" . $endpointsProto->getScheme());
                error_log("DEBUG: Endpoints addresses count=" . count($endpointsProto->getAddresses()));
            }
        } else {
            error_log("DEBUG QueryRoute Request: endpoints is null");
        }
        
        // Build metadata for gRPC call (aligned with Java Signature.sign)
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-protocol' => ['GRPC_V2'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // Always include namespace header (even if empty), aligned with Java
        $namespace = $this->config->getNamespace();
        $metadata['x-mq-namespace'] = [$namespace];
        
        // Add request ID
        $metadata['x-mq-request-id'] = [uniqid('php-', true)];
        
        // Add date-time for signature
        $dateTime = gmdate('Ymd\THis\Z');
        $metadata['x-mq-date-time'] = [$dateTime];
        
        // Add credentials if available
        $provider = $this->config->getCredentialsProvider();
        if ($provider !== null) {
            $credentials = $provider->getSessionCredentials();
            if ($credentials !== null) {
                $accessKey = $credentials->getAccessKey();
                $accessSecret = $credentials->getAccessSecret();
                
                if (!empty($accessKey) && !empty($accessSecret)) {
                    // Generate signature (aligned with Java TLSHelper.sign)
                    $signature = strtoupper(hash_hmac('sha1', $dateTime, $accessSecret));
                    
                    $authorization = 'MQv2-HMAC-SHA1 ' .
                        'Credential=' . $accessKey . ', ' .
                        'SignedHeaders=x-mq-date-time, ' .
                        'Signature=' . $signature;
                    
                    $metadata['authorization'] = [$authorization];
                    
                    // Add session token if available
                    $securityToken = $credentials->tryGetSecurityToken();
                    if ($securityToken !== null) {
                        $metadata['x-mq-session-token'] = [$securityToken];
                    }
                }
            }
        }

        $startTime = microtime(true);
        [$response, $status] = $this->getClient()->QueryRoute($request, $metadata)->wait();
        $elapsed = round((microtime(true) - $startTime) * 1000, 2);

        // Debug output
        error_log("DEBUG: QueryRoute status_code=" . ($status->code ?? 'null') . ", response_type=" . (is_object($response) ? get_class($response) : gettype($response)));

        if ($status->code !== \Grpc\STATUS_OK) {
            Logger::error("RPC QueryRoute failed, topic={}, status_code={}, details={}, elapsed={}ms, clientId={}", [
                $topic,
                $status->code,
                $status->details,
                $elapsed,
                $this->clientId
            ]);
            throw new \Exception("Failed to query route for topic {$topic}: {$status->details}");
        }
        
        // Debug: log message queues count
        $queuesCount = $response ? count($response->getMessageQueues()) : 0;
        Logger::debug("QueryRoute returned {} message queues, clientId={}", [
            $queuesCount,
            $this->clientId
        ]);
        
        // Debug output all queues
        if ($response && $queuesCount > 0) {
            error_log("DEBUG: Total queues: {$queuesCount}");
            foreach ($response->getMessageQueues() as $idx => $mq) {
                $perm = $mq->getPermission();
                $broker = $mq->getBroker();
                $brokerId = $broker ? $broker->getId() : 'null';
                $brokerName = $broker ? $broker->getName() : 'null';
                error_log("DEBUG: Queue #{$idx}: permission={$perm}, brokerId={$brokerId}, brokerName={$brokerName}");
            }
        } else {
            error_log("DEBUG: No queues in response or response is null");
            if ($response) {
                error_log("DEBUG: Response object exists but has " . $queuesCount . " queues");
            }
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
            
            if ($isReadable && $isMaster) {
                // Ensure broker has endpoints set
                $broker = $mq->getBroker();
                
                if ($broker !== null) {
                    $existingEndpoints = $broker->getEndpoints();
                    // Check if endpoints exist and has addresses
                    $hasExistingEndpoints = ($existingEndpoints !== null && 
                                            method_exists($existingEndpoints, 'getAddresses') && 
                                            count($existingEndpoints->getAddresses()) > 0);
                    
                    // If broker doesn't have endpoints, try to set from client config
                    if (!$hasExistingEndpoints) {
                        $proxyEndpoints = $this->config->getEndpointsAsProtobuf();
                        if ($proxyEndpoints !== null) {
                            $broker->setEndpoints($proxyEndpoints);
                        } else {
                            Logger::warn("Client config also has no endpoints, skipping queue, topic={}, brokerId={}, clientId={}", [
                                $topic,
                                $brokerId,
                                $this->clientId
                            ]);
                            continue;
                        }
                    }
                    $mqTopic = $mq->getTopic();
                    if ($mqTopic !== null && empty($mqTopic->getResourceNamespace())) {
                        $mqTopic->setResourceNamespace($this->config->getNamespace());
                    }
                    $queues[] = $mq;
                } else {
                    Logger::warn("Broker is null, skipping queue, topic={}, queueId={}, clientId={}", [
                        $topic,
                        $mq->getId(),
                        $this->clientId
                    ]);
                }
            }
        }

        if (empty($queues)) {
            Logger::error("No readable queue found for topic {}, clientId={}", [$topic, $this->clientId]);
            throw new \Exception("No readable queue found for topic {$topic}");
        }

        Logger::debug("QueryRoute success for topic {}, readableQueues={}, clientId={}", [
            $topic,
            count($queues),
            $this->clientId
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
        
        // Set consumer group with namespace (aligned with Java getProtobufGroup())
        $group = new Resource();
        $group->setResourceNamespace($this->config->getNamespace());
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
        // Telemetry session is required for proper operation
        // Without it, settings cannot be synced to server, which will cause issues with receiveMessage
        
        if (!extension_loaded('swoole')) {
            throw new \Exception(
                "Swoole extension is required for SimpleConsumer. " .
                "Telemetry session (bidirectional gRPC stream) depends on Swoole coroutines. " .
                "Please install Swoole: pecl install swoole"
            );
        }
        
        if (!class_exists('Apache\Rocketmq\AsyncTelemetrySession')) {
            throw new \Exception(
                "AsyncTelemetrySession class not found. " .
                "Please ensure all PHP SDK files are properly loaded."
            );
        }
        
        try {
            // Create and start TelemetrySession (uses official gRPC extension)
            $this->telemetrySession = new TelemetrySession(
                $this->clientId,
                $this->getClient()
            );
            $this->telemetrySession->start();
            
            Logger::info("Telemetry session started successfully, clientId={}", [$this->clientId]);
        } catch (\Exception $e) {
            Logger::error("Failed to initialize telemetry session, clientId={}, error={}", [
                $this->clientId,
                $e->getMessage()
            ]);
            throw new \Exception(
                "Failed to start SimpleConsumer: Telemetry session initialization failed. " .
                "This is required for proper operation. Error: " . $e->getMessage(),
                0,
                $e
            );
        }
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
            \Swoole\Timer::tick(self::HEARTBEAT_INTERVAL * 1000, function($timerId) {
                if ($this->state !== 'RUNNING') {
                    \Swoole\Timer::clear($timerId);
                    return;
                }
                
                try {
                    $this->heartbeat();
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
            } catch (\Exception $e) {
                Logger::warn("Warning: Heartbeat failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
            }
            
            // Sync settings every SETTINGS_SYNC_INTERVAL seconds (Java default: 5min)
            $now = time();
            if (($now - $lastSettingsSync) >= self::SETTINGS_SYNC_INTERVAL) {
                try {
                    $this->syncSettings();
                    $lastSettingsSync = $now;
                } catch (\Exception $e) {
                    Logger::warn("Warning: Settings sync failed, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                }
            }
        }
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
            // Telemetry session is optional - skip settings sync if not available
            Logger::warn("Telemetry session not initialized, skipping settings sync, clientId={}", [$this->clientId]);
            return;
        }
        
        // Build current settings with subscriptions
        $settings = $this->buildCurrentSettings();
        
        // Send settings via telemetry session
        if ($this->telemetrySession instanceof TelemetrySession) {
            $this->telemetrySession->sendSettings($settings);
        } elseif (method_exists($this->telemetrySession, 'sendCustomSettings')) {
            // Fallback for other implementations
            $this->telemetrySession->sendCustomSettings($settings);
        } else {
            throw new \Exception(
                "Telemetry session does not support sendCustomSettings. " .
                "This should not happen with AsyncTelemetrySession. clientId={$this->clientId}"
            );
        }
    }
    
    /**
     * Build current settings with subscriptions (aligned with Java SimpleSubscriptionSettings.toProtobuf())
     * 
     * @return \Apache\Rocketmq\V2\Settings Settings object
     */
    private function buildCurrentSettings(): \Apache\Rocketmq\V2\Settings
    {
        $settings = new \Apache\Rocketmq\V2\Settings();
        
        // Set client type
        $settings->setClientType(\Apache\Rocketmq\V2\ClientType::SIMPLE_CONSUMER);
        
        // Set user agent
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion(Util::getSdkVersion());
        $settings->setUserAgent($ua);
        
        // Set request timeout
        $requestTimeout = new \Google\Protobuf\Duration();
        $requestTimeout->setSeconds($this->config->getRequestTimeout());
        $settings->setRequestTimeout($requestTimeout);
        
        // Build subscription
        $subscription = new \Apache\Rocketmq\V2\Subscription();
        
        // Set group with namespace
        $group = new \Apache\Rocketmq\V2\Resource();
        $group->setResourceNamespace($this->config->getNamespace());
        $group->setName($this->consumerGroup);
        $subscription->setGroup($group);
        
        // Set long polling timeout
        $longPollingTimeout = new \Google\Protobuf\Duration();
        $longPollingTimeout->setSeconds($this->awaitDuration);
        $subscription->setLongPollingTimeout($longPollingTimeout);
        
        // Build subscription entries from subscription expressions
        $subscriptionEntries = [];
        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            // Create topic resource with namespace
            $topicResource = new \Apache\Rocketmq\V2\Resource();
            $topicResource->setResourceNamespace($this->config->getNamespace());
            $topicResource->setName($topic);
            
            // Create filter expression
            $v2FilterExpression = new \Apache\Rocketmq\V2\FilterExpression();
            $v2FilterExpression->setExpression($filterExpression->getExpression());
            
            // Set filter type
            $filterType = $filterExpression->getType();
            if ($filterType === FilterExpressionType::SQL92) {
                $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::SQL);
            } else {
                $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);
            }
            
            // Create subscription entry
            $entry = new \Apache\Rocketmq\V2\SubscriptionEntry();
            $entry->setTopic($topicResource);
            $entry->setExpression($v2FilterExpression);
            
            $subscriptionEntries[] = $entry;
        }
        
        $subscription->setSubscriptions($subscriptionEntries);
        $settings->setSubscription($subscription);
        
        return $settings;
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
        
        Logger::debug("SimpleConsumer.receive called, maxMessageNum={}, invisibleDuration={}, clientId={}", [
            $maxMessageNum ?? $this->maxMessageNum,
            $invisibleDuration ?? $this->invisibleDuration,
            $this->clientId
        ]);
        
        error_log("DEBUG: receiveInternal called, maxMessageNum=" . ($maxMessageNum ?? $this->maxMessageNum) . ", invisibleDuration=" . ($invisibleDuration ?? $this->invisibleDuration));
        
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
            Logger::error("No subscriptions configured, cannot receive messages, clientId={}", [$this->clientId]);
            throw new \InvalidArgumentException("There is no topic to receive message");
        }
        
        Logger::debug("Subscriptions found, count={}, topics={}, clientId={}", [
            count($this->subscriptionExpressions),
            implode(',', array_keys($this->subscriptionExpressions)),
            $this->clientId
        ]);
        
        // Round-robin select topic from all subscribed topics (aligned with Java L159-166)
        $topics = array_keys($this->subscriptionExpressions);
        $topicIndexValue = $this->topicIndex % count($topics);
        $this->topicIndex++;
        $selectedTopic = $topics[$topicIndexValue];
        
        // Get filter expression for selected topic
        $filterExpression = $this->subscriptionExpressions[$selectedTopic];
        
        $request = new ReceiveMessageRequest();
        
        // Set consumer group with namespace (aligned with Java getProtobufGroup())
        $group = new Resource();
        $group->setResourceNamespace($this->config->getNamespace());
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        // Get message queue from route query for selected topic
        // Aligned with Java SimpleConsumerImpl: route query -> load balancer -> takeMessageQueue
        $mq = $this->takeMessageQueueForTopic($selectedTopic);

        $mqTopic = $mq->getTopic();
        if ($mqTopic !== null && empty($mqTopic->getResourceNamespace())) {
            $newTopic = new \Apache\Rocketmq\V2\Resource();
            $newTopic->setName($mqTopic->getName());
            $newTopic->setResourceNamespace($this->config->getNamespace());

            $newMq = new \Apache\Rocketmq\V2\MessageQueue();
            $newMq->setTopic($newTopic);
            $newMq->setId($mq->getId());
            $newMq->setPermission($mq->getPermission());

            $broker = $mq->getBroker();
            if ($broker !== null) {
                $newBroker = clone $broker;
                $newMq->setBroker($newBroker);
            }
            $mq = $newMq;
        }

        // Ensure broker has endpoints set before adding to request
        $broker = $mq->getBroker();
        if ($broker !== null) {
            $brokerEndpoints = $broker->getEndpoints();
            $hasEndpoints = ($brokerEndpoints !== null && 
                           method_exists($brokerEndpoints, 'getAddresses') && 
                           count($brokerEndpoints->getAddresses()) > 0);
            
            if (!$hasEndpoints) {
                $proxyEndpoints = $this->config->getEndpointsAsProtobuf();
                if ($proxyEndpoints !== null) {
                    $newBroker = clone $broker;
                    $broker->setEndpoints($proxyEndpoints);
                    $mq->setBroker($newBroker);
                } else {
                    Logger::warn("Client config also has no endpoints, MessageQueue may fail, topic={}, brokerId={}, queueId={}, clientId={}", [
                        $mqTopicName ?? ($mq->getTopic()->getName() ?? 'unknown'),
                        $mqBrokerId ?? ($mq->getBroker()->getId() ?? 'unknown'),
                        $mqQueueId ?? ($mq->getId() ?? 'unknown'),
                        $this->clientId
                    ]);
                }
            }
        }
        
        $request->setMessageQueue($mq);
        
        // Set filter expression from subscription (required - missing this causes server NPE)
        // Aligned with Java SimpleConsumerImpl: filterExpression = subscriptionExpressions.get(topic)
        $expression = $filterExpression->getExpression();
        if (empty($expression)) {
            Logger::warn("FilterExpression is empty, using default '*', topic={}, clientId={}", [
                $selectedTopic,
                $this->clientId
            ]);
            $expression = '*';
        }
        
        $v2Filter = new V2FilterExpression();
        $v2Filter->setExpression($expression);
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

        // Build full metadata for gRPC call (aligned with Java Signature.sign)
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-protocol' => ['GRPC_V2'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // Always include namespace header (even if empty), aligned with Java
        $namespace = $this->config->getNamespace();
        $metadata['x-mq-namespace'] = [$namespace];
        
        // Add request ID
        $metadata['x-mq-request-id'] = [uniqid('php-', true)];
        
        // Add date-time for signature
        $dateTime = gmdate('Ymd\THis\Z');
        $metadata['x-mq-date-time'] = [$dateTime];
        
        // Add credentials if available
        $provider = $this->config->getCredentialsProvider();
        if ($provider !== null) {
            $credentials = $provider->getSessionCredentials();
            if ($credentials !== null) {
                $accessKey = $credentials->getAccessKey();
                $accessSecret = $credentials->getAccessSecret();
                
                if (!empty($accessKey) && !empty($accessSecret)) {
                    // Generate signature (aligned with Java TLSHelper.sign)
                    $signature = strtoupper(hash_hmac('sha1', $dateTime, $accessSecret));
                    
                    $authorization = 'MQv2-HMAC-SHA1 ' .
                        'Credential=' . $accessKey . ', ' .
                        'SignedHeaders=x-mq-date-time, ' .
                        'Signature=' . $signature;
                    
                    $metadata['authorization'] = [$authorization];
                    
                    // Add session token if available
                    $securityToken = $credentials->tryGetSecurityToken();
                    if ($securityToken !== null) {
                        $metadata['x-mq-session-token'] = [$securityToken];
                    }
                }
            }
        }

        Logger::debug("Sending ReceiveMessage request, topic={}, broker={}, queueId={}, batchSize={}, clientId={}", [
            $selectedTopic,
            $mq->getBroker()->getName(),
            $mq->getId(),
            $maxMessageNum,
            $this->clientId
        ]);
        
        error_log("DEBUG: Calling ReceiveMessage RPC now...");

        // Use official gRPC extension with metadata (blocking but reliable)
        error_log("DEBUG: About to call ReceiveMessage RPC...");
        $call = $this->getClient()->ReceiveMessage($request, $metadata);
        error_log("DEBUG: ReceiveMessage RPC call returned, iterating responses...");
        
        $messages = [];
        
        // Read response stream (this will block until long polling timeout or messages available)
        $responseCount = 0;
        foreach ($call->responses() as $response) {
            $responseCount++;
            error_log("DEBUG: Got response #{$responseCount}");
            
            if ($response->hasStatus()) {
                $status = $response->getStatus();
                error_log("DEBUG: Response has status, code=" . $status->getCode() . ", message=" . $status->getMessage());
            }
            
            if ($response->hasMessage()) {
                $messages[] = $response->getMessage();
                error_log("DEBUG: Received message, messageId=" . $response->getMessage()->getSystemProperties()->getMessageId());
                Logger::debug("Received message from stream, messageId={}", [
                    $response->getMessage()->getSystemProperties()->getMessageId()
                ]);
            } else {
                error_log("DEBUG: Response has no message");
                Logger::debug("Response has no message, status code={}", [
                    $response->hasStatus() ? $response->getStatus()->getCode() : 'N/A'
                ]);
            }
        }
        
        error_log("DEBUG: Response iteration complete, responseCount={$responseCount}, messageCount=" . count($messages));
        
        Logger::debug("ReceiveMessage response received, responseCount={}, messageCount={}, clientId={}", [
            $responseCount,
            count($messages),
            $this->clientId
        ]);
        
        return $messages;
    }
    
    /**
     * Receive messages using Swoole HTTP/2 Client with upgrade()/push()/recv()
     * 
     * @param \Apache\Rocketmq\V2\ReceiveMessageRequest $request
     * @param array $metadata
     * @param string $topic
     * @return array
     */
    private function receiveWithSwooleHttp2($request, $metadata, $topic): array
    {
        Logger::debug("Using Swoole HTTP/2 upgrade/push/recv, topic={}, clientId={}", [$topic, $this->clientId]);
        
        // Parse endpoints
        list($host, $port) = explode(':', $this->config->getEndpoints());
        $port = intval($port);
        
        // Create Swoole HTTP/2 client
        $httpClient = new \Swoole\Coroutine\Http\Client($host, $port);
        
        if (!$httpClient) {
            throw new \Exception("Failed to create Swoole HTTP/2 client");
        }
        
        // Configure HTTP/2
        $httpClient->set([
            'open_http2_protocol' => true,
            'timeout' => 10,
            'ssl_verify_peer' => false,
        ]);
        
        // Build headers from metadata
        $headers = [
            'content-type' => 'application/grpc',
            'te' => 'trailers',
            'user-agent' => 'rocketmq-php-client/' . Util::getSdkVersion(),
        ];
        
        foreach ($metadata as $key => $values) {
            $headers[$key] = $values[0];
        }
        
        // Set headers before upgrade
        $httpClient->setHeaders($headers);
        
        // Upgrade to HTTP/2 (only path parameter)
        $path = '/apache.rocketmq.v2.MessagingService/ReceiveMessage';
        $upgradeResult = $httpClient->upgrade($path);
        
        if (!$upgradeResult) {
            $httpClient->close();
            throw new \Exception("Upgrade failed: errCode=" . $httpClient->errCode . ", errMsg=" . $httpClient->errMsg);
        }
        
        Logger::debug("HTTP/2 upgrade OK, clientId={}", [$this->clientId]);
        
        // Serialize and frame request
        $requestData = $request->serializeToString();
        $framedData = pack('CN', 0, strlen($requestData)) . $requestData;
        
        // Push request
        $pushResult = $httpClient->push($framedData);
        
        if (!$pushResult) {
            $httpClient->close();
            throw new \Exception("Push failed: errCode=" . $httpClient->errCode);
        }
        
        Logger::debug("Request pushed, waiting..., clientId={}", [$this->clientId]);
        
        // Receive responses
        $messages = [];
        $timeout = $this->awaitDuration + 2;
        $startTime = microtime(true);
        
        while ((microtime(true) - $startTime) < $timeout) {
            $response = $httpClient->recv(0.5);
            
            if ($response === false) {
                $errCode = $httpClient->errCode;
                if ($errCode !== 0) {
                    Logger::warn("Stream error {$errCode}, clientId={$this->clientId}");
                }
                break;
            }
            
            if ($response === null || strlen($response) === 0) {
                continue;
            }
            
            Logger::debug("Got response {} bytes, clientId={}", [strlen($response), $this->clientId]);
            
            if (strlen($response) > 5) {
                $data = substr($response, 5);
                $receiveResponse = new \Apache\Rocketmq\V2\ReceiveMessageResponse();
                $receiveResponse->mergeFromString($data);
                
                if ($receiveResponse->hasStatus()) {
                    $code = $receiveResponse->getStatus()->getCode();
                    if ($code !== \Apache\Rocketmq\V2\Code::OK) {
                        Logger::warn("Status not OK: code={}, clientId={}", [$code, $this->clientId]);
                        break;
                    }
                }
                
                if ($receiveResponse->hasMessage()) {
                    $message = $receiveResponse->getMessage();
                    $messages[] = $message;
                    Logger::debug("Got message, total={}, clientId={}", [count($messages), $this->clientId]);
                }
            }
        }
        
        $httpClient->close();
        
        Logger::info("Swoole receive done, count={}, topic={}, clientId={}", [count($messages), $topic, $this->clientId]);
        
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
        
        $request = new AckMessageRequest();
        
        // Set consumer group with namespace (aligned with Java getProtobufGroup())
        $group = new Resource();
        $group->setResourceNamespace($this->config->getNamespace());
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        // Set topic with namespace (required)
        $topicName = $this->topic;
        if ($message->hasTopic()) {
            $topicName = $message->getTopic()->getName();
        }
        
        $topicResource = new Resource();
        $topicResource->setResourceNamespace($this->config->getNamespace());
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
        
        $request = new \Apache\Rocketmq\V2\ChangeInvisibleDurationRequest();
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration(
            (new \Google\Protobuf\Duration())->setSeconds($durationSeconds)
        );
        
        // Create V2\MessageQueue for request (Protobuf object)
        $mq = new V2MessageQueue();
        $resource = new Resource();
        $resource->setResourceNamespace($this->config->getNamespace());
        $resource->setName($this->topic);
        $mq->setTopic($resource);
        $request->setMessageQueue($mq);
        
        $metadata = $this->buildMetadata();
        $call = $this->getClient()->ChangeInvisibleDuration($request, $metadata);
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
            posix_kill($this->heartbeatTimerPid, SIGTERM);
            pcntl_waitpid($this->heartbeatTimerPid, $status);
            $this->heartbeatTimerPid = null;
        }
        
        // Shutdown meter manager
        if ($this->meterManager !== null) {
            $this->meterManager->shutdown();
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
        } catch (\InvalidArgumentException $e) {
            Logger::warn("Failed to update load balancer from telemetry, topic={}, clientId={}, error={}", [
                $topic,
                $this->clientId,
                $e->getMessage()
            ]);
        }
    }
}
