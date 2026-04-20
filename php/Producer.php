<?php
declare(strict_types=1);
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

require_once __DIR__ . '/Util.php';
require_once __DIR__ . '/RouteCache.php';
require_once __DIR__ . '/ClientState.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/HealthChecker.php';
require_once __DIR__ . '/Producer/Producer.php';
require_once __DIR__ . '/Producer/PublishingLoadBalancer.php';

// Load composer autoload if available
if (file_exists(__DIR__ . '/vendor/autoload.php')) {
    require_once __DIR__ . '/vendor/autoload.php';
}

// Autoload gRPC generated classes
spl_autoload_register(function ($class) {
    $class = str_replace('\\', '/', $class);
    $file = __DIR__ . '/grpc/' . $class . '.php';
    if (file_exists($file)) {
        require_once $file;
    }
});

use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Builder\ProducerBuilder;
use Apache\Rocketmq\Builder\PushConsumerBuilder;
use Apache\Rocketmq\Builder\SimpleConsumerBuilder;
use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientConfigurationException;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\ClientStateException;
use Apache\Rocketmq\Exception\MessageException;
use Apache\Rocketmq\Exception\NetworkException;
use Apache\Rocketmq\Exception\ServerException;
use Apache\Rocketmq\Exception\TransactionException;
use Apache\Rocketmq\Message\Message as MessageInterface;
use Apache\Rocketmq\Producer\Producer as ProducerInterface;
use Apache\Rocketmq\Producer\RecallReceipt;
use Apache\Rocketmq\Producer\SendReceipt;
use Apache\Rocketmq\Producer\Transaction;
use Apache\Rocketmq\Producer\TransactionChecker;
use Apache\Rocketmq\Producer\TransactionResolution;
use Apache\Rocketmq\Producer\TransactionImpl;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\Producer\PublishingLoadBalancer;
use Grpc\ChannelCredentials;
use const Grpc\STATUS_OK;

// Initialize logger to write to ~/logs/rocketmq/rocketmq_client_php.log
\Apache\Rocketmq\Logger::init();

/**
 * RocketMQ Producer Implementation
 * 
 * Supports the following message types:
 * - Normal messages
 * - FIFO messages (ordered messages)
 * - Scheduled/delayed messages
 * - Transaction messages
 * - Async sending
 * 
 * Usage example:
 * $producer = Producer::getInstance($endpoints, $topic);
 * $producer->sendNormalMessage($body, $tag, $keys);
 */
class Producer implements ProducerInterface
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
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var RetryPolicy Retry policy
     */
    private $retryPolicy;
    
    /**
     * @var string Client state: CREATED, STARTING, RUNNING, STOPPING, TERMINATED
     */
    private $state = 'CREATED';
    
    /**
     * @var RouteCache Route cache instance
     */
    private $routeCache;
    
    /**
     * @var TransactionChecker|null Transaction checker
     */
    private $transactionChecker = null;
    
    /**
     * @var HealthChecker|null Health checker
     */
    private $healthChecker = null;
    
    /**
     * @var MessageInterceptor[] Message interceptor list
     */
    private $interceptors = [];
    
    /**
     * @var MetricsCollector Metrics collector
     */
    private $metricsCollector;
    
    /**
     * @var ClientMeterManager|null Client meter manager (optional)
     */
    private $meterManager = null;
    
    /**
     * @var int Max attempts for message sending
     */
    private $maxAttempts = 3;
    
    /**
     * @var PublishingLoadBalancer|null Publishing load balancer (for queue selection)
     */
    private $publishingLoadBalancer = null;
    
    /**
     * Private constructor to prevent direct instantiation
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string (backward compatible)
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @param RetryPolicy|null $retryPolicy Retry policy (optional, Used when first parameter is endpoints string)
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    private function __construct($configOrEndpoints, $topic = null, $retryPolicy = null)
    {
        // Supports two construction methods:
        // 1. New configuration object method: __construct(ClientConfiguration $config, $topic)
        // 2. Legacy compatible method: __construct($endpoints, $topic, $retryPolicy)
        
        if ($configOrEndpoints instanceof ClientConfiguration) {
            // New configuration object method
            $this->config = $configOrEndpoints;
            $this->topic = $topic;
            $this->retryPolicy = $this->config->getOrCreateRetryPolicy();
        } else {
            // Legacy compatible method
            if (empty($topic)) {
                throw new \InvalidArgumentException("Topic cannot be empty when using legacy constructor");
            }
            
            $this->config = new ClientConfiguration($configOrEndpoints);
            if ($retryPolicy !== null) {
                $this->config->withRetryPolicy($retryPolicy);
            }
            $this->topic = $topic;
            $this->retryPolicy = $this->config->getOrCreateRetryPolicy();
        }
        
        $this->clientId = $this->generateClientId();
        $this->routeCache = RouteCache::getInstance();
        // Set route cache configuration from client configuration
        $this->routeCache->setConfigFromClientConfiguration($this->config);
        $this->metricsCollector = new MetricsCollector($this->clientId);
        
        // Auto-initialize ClientMeterManager for metrics support
        $this->meterManager = new ClientMeterManager($this->clientId, $this->metricsCollector);
    }
    
    /**
     * Get Producer instance (configuration object recommended)
     * 
     * Usage example:
     * ```php
     * // Method 1: Using configuration object with topic (recommended)
     * $config = new ClientConfiguration('127.0.0.1:8080');
     * $producer = Producer::getInstance($config, 'my-topic');
     * 
     * // Method 2: Using legacy method (backward compatible)
     * $producer = Producer::getInstance('127.0.0.1:8080', 'my-topic');
     * 
     * // Note: Topic parameter is optional but highly recommended.
     * // It allows the producer to prefetch topic route before message publishing,
     * // which improves performance and helps discover configuration errors early.
     * ```
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string
     * @param string|null $topic Topic name (optional but recommended for better performance)
     * @param RetryPolicy|null $retryPolicy Retry policy (optional, only effective when using endpoint string)
     * @return Producer Producer instance
     */
    public static function getInstance($configOrEndpoints, $topic = null, $retryPolicy = null)
    {
        return new self($configOrEndpoints, $topic, $retryPolicy);
    }
    
    /**
     * Get Producer singleton instance for transaction messages (configuration object recommended)
     * 
     * Usage example:
     * ```php
     * // Method 1: Using configuration object (recommended)
     * $config = new ClientConfiguration('127.0.0.1:8080');
     * $checker = new MyTransactionChecker();
     * $producer = Producer::getTransactionalInstance($config, 'my-topic', $checker);
     * 
     * // Method 2: Using legacy method (backward compatible)
     * $producer = Producer::getTransactionalInstance('127.0.0.1:8080', 'my-topic', $checker);
     * ```
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
     * @param TransactionChecker|null $checker Transaction checker
     * @param RetryPolicy|null $retryPolicy Retry policy (optional, only effective when using endpoint string)
     * @return Producer Producer instance
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public static function getTransactionalInstance($configOrEndpoints, $topic = null, $checker = null, $retryPolicy = null)
    {
        // Handle parameter order issue: if the third parameter is RetryPolicy, it means using old API
        if ($checker instanceof RetryPolicy) {
            $retryPolicy = $checker;
            $checker = null;
        }
        
        $producer = new self($configOrEndpoints, $topic, $retryPolicy);
        
        if ($checker !== null && !($checker instanceof TransactionChecker)) {
            throw new \InvalidArgumentException("Checker must be an instance of TransactionChecker");
        }
        
        if ($checker !== null) {
            $producer->transactionChecker = $checker;
        }
        
        return $producer;
    }
    
    /**
     * Start Producer
     * 
     * @return void
     * @throws \Exception If startup fails or state is incorrect
     */
    public function start()
    {
        // Check state transition validity
        ClientState::checkState($this->state, [ClientState::CREATED], 'start');
        
        Logger::info("Begin to start the rocketmq producer, clientId={}", [$this->clientId]);
        
        $this->state = ClientState::STARTING;
        
        try {
            // Initialize health checker for connection monitoring
            $this->healthChecker = new HealthChecker(
                $this->config->getEndpoints(),
                $this->clientId,
                $this->config->getRequestTimeout()
            );
            Logger::debug("Health checker initialized, endpoints={}, clientId={}", [
                $this->config->getEndpoints(),
                $this->clientId
            ]);
            
            // Verify connection and get route information (with retry)
            $maxStartupAttempts = $this->config->getMaxStartupAttempts();
            $lastException = null;
            
            for ($attempt = 1; $attempt <= $maxStartupAttempts; $attempt++) {
                try {
                    Logger::debug("Fetching topic route from remote, topic={}, attempt={}, maxAttempts={}, clientId={}", [
                        $this->topic,
                        $attempt,
                        $maxStartupAttempts,
                        $this->clientId
                    ]);
                    
                    $this->queryRoute();
                    
                    Logger::info(
                        "Fetch topic route data from remote successfully during startup, topic={}, attempt={}, clientId={}",
                        [$this->topic, $attempt, $this->clientId]
                    );
                    break;
                } catch (\Throwable $e) {
                    $lastException = $e;
                    Logger::warn(
                        "Failed to fetch topic route during startup, topic={}, attempt={}, maxAttempts={}, error={}, clientId={}",
                        [$this->topic, $attempt, $maxStartupAttempts, $e->getMessage(), $this->clientId]
                    );
                    
                    if ($attempt === $maxStartupAttempts) {
                        throw new \Exception(
                            "Failed to fetch topics after {$maxStartupAttempts} attempts",
                            0,
                            $e
                        );
                    }
                    
                    // Wait before retry (exponential backoff)
                    $delayMs = min(1000 * pow(2, $attempt - 1), 5000); // Max 5s
                    usleep($delayMs * 1000);
                }
            }
            
            // Transition state to RUNNING
            if (ClientState::canTransition($this->state, ClientState::RUNNING)) {
                $this->state = ClientState::RUNNING;
                Logger::info("The rocketmq producer starts successfully, clientId={}", [$this->clientId]);
            }
        } catch (\Throwable $e) {
            // Transition state to FAILED
            if (ClientState::canTransition($this->state, ClientState::FAILED)) {
                $this->state = ClientState::FAILED;
            }
            Logger::error("Failed to start the rocketmq producer, try to shutdown it, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
            throw new \Exception(
                "Failed to start producer: " . $e->getMessage(), 
                0, 
                $e
            );
        }
    }
    
    /**
     * Add message interceptor
     * 
     * @param MessageInterceptor $interceptor Interceptor instance
     * @return self Return current instance for chainable calls
     */
    public function addInterceptor($interceptor)
    {
        if (!($interceptor instanceof MessageInterceptor)) {
            throw new \InvalidArgumentException("Interceptor must implement MessageInterceptor interface");
        }
        
        $this->interceptors[] = $interceptor;
        return $this;
    }
    
    /**
     * Remove message interceptor
     * 
     * @param MessageInterceptor $interceptor Interceptor instance
     * @return bool Whether successfully removed
     */
    public function removeInterceptor($interceptor)
    {
        $index = array_search($interceptor, $this->interceptors, true);
        if ($index !== false) {
            unset($this->interceptors[$index]);
            $this->interceptors = array_values($this->interceptors); // Re-index
            return true;
        }
        return false;
    }
    
    /**
     * Clear all interceptors
     * 
     * @return void
     */
    public function clearInterceptors()
    {
        $this->interceptors = [];
    }
    
    /**
     * Get interceptor list
     * 
     * @return MessageInterceptor[]
     */
    public function getInterceptors()
    {
        return $this->interceptors;
    }
    
    /**
     * Enable metrics collection with automatic meter interceptor
     * 
     * This method:
     * 1. Creates ClientMeterManager if not exists
     * 2. Enables metrics collection
     * 3. Adds MessageMeterInterceptor to the interceptor chain
     * 
     * @param string|null $exportEndpoint OTLP export endpoint (optional)
     * @param int $exportInterval Export interval in seconds (default: 60)
     * @return self Return current instance for chainable calls
     */
    public function enableMetrics(?string $exportEndpoint = null, int $exportInterval = 60): self
    {
        // Create meter manager if not exists
        if ($this->meterManager === null) {
            $this->meterManager = new ClientMeterManager($this->clientId, $this->metricsCollector);
        }
        
        // Enable metrics
        $this->meterManager->enable($exportEndpoint, $exportInterval);
        
        // Check if MessageMeterInterceptor already exists
        $hasMeterInterceptor = false;
        foreach ($this->interceptors as $interceptor) {
            if ($interceptor instanceof MessageMeterInterceptor) {
                $hasMeterInterceptor = true;
                break;
            }
        }
        
        // Add MessageMeterInterceptor if not exists
        if (!$hasMeterInterceptor) {
            $meterInterceptor = new MessageMeterInterceptor(
                $this->metricsCollector,
                $this->clientId
            );
            $this->addInterceptor($meterInterceptor);
            Logger::info("MessageMeterInterceptor added automatically, clientId={}", [$this->clientId]);
        }
        
        Logger::info("Metrics enabled for Producer, clientId={}, exportEndpoint={}", [
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
    public function disableMetrics(): self
    {
        if ($this->meterManager !== null) {
            $this->meterManager->disable();
            Logger::info("Metrics disabled for Producer, clientId={}", [$this->clientId]);
        }
        
        return $this;
    }
    
    /**
     * Get client meter manager
     * 
     * @return ClientMeterManager|null Meter manager or null if not enabled
     */
    public function getMeterManager(): ?ClientMeterManager
    {
        return $this->meterManager;
    }
    
    /**
     * Execute before-send interceptors
     * 
     * @param array $messages Message array
     * @return MessageInterceptorContext Interceptor context
     */
    private function doBeforeSend($messages)
    {
        $context = new MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        
        foreach ($this->interceptors as $interceptor) {
            try {
                $interceptor->doBefore($context, $messages);
            } catch (\Throwable $e) {
                // Log error but do not interrupt flow
                Logger::error("Exception raised while executing before-send interceptor, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                $context->setStatus(MessageHookPointsStatus::ERROR);
                $context->setException($e);
            }
        }
        
        return $context;
    }
    
    /**
     * Execute after-send interceptors
     * 
     * @param MessageInterceptorContext $context Interceptor context
     * @param array $messages Message array
     * @param mixed $result Send result
     * @param \Exception|null $exception Exception information
     * @return void
     */
    private function doAfterSend($context, $messages, $result = null, $exception = null)
    {
        if ($exception !== null) {
            $context->setStatus(MessageHookPointsStatus::ERROR);
            $context->setException($exception);
        }
        
        $context->setResult($result);
        
        foreach ($this->interceptors as $interceptor) {
            try {
                $interceptor->doAfter($context, $messages);
            } catch (\Throwable $e) {
                // Log error but do not interrupt flow
                Logger::error("Exception raised while executing after-send interceptor, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            }
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
            // Get connection from pool
            $pool = ConnectionPool::getInstance();
            // Set connection pool configuration from client configuration
            $pool->setConfigFromClientConfiguration($this->config);
            $this->client = $pool->getConnection($this->config);
        }
        return $this->client;
    }
    
    /**
     * Query route information (with cache)
     * 
     * @param bool $forceRefresh Whether to force refresh
     * @return \Apache\Rocketmq\V2\QueryRouteResponse Route response
     * @throws \Exception If query fails
     */
    public function queryRoute($forceRefresh = false)
    {
        // If force refresh, clear cache and load balancer
        if ($forceRefresh) {
            $this->routeCache->invalidate($this->topic);
            $this->publishingLoadBalancer = null;
        }
        
        // Use cache to get route
        $response = $this->routeCache->getOrCreate($this->topic, function() {
            $qr = new QueryRouteRequest();
            $rs = new Resource();
            $rs->setResourceNamespace($this->config->getNamespace());
            $rs->setName($this->topic);
            $qr->setTopic($rs);
            $qr->setEndpoints($this->config->getEndpoints()->toProtobuf());
            
            list($response, $status) = $this->getClient()->QueryRoute($qr)->wait();
            
            if ($status->code !== STATUS_OK) {
                throw new \Exception(
                    "Failed to query route: " . $status->details,
                    $status->code
                );
            }
            
            return $response;
        });
        
        // Create or update PublishingLoadBalancer from route response
        $messageQueuesRepeated = $response->getMessageQueues();
        if (!empty($messageQueuesRepeated)) {
            try {
                // Convert Protobuf RepeatedField to PHP array
                $messageQueues = iterator_to_array($messageQueuesRepeated);
                
                $this->publishingLoadBalancer = new PublishingLoadBalancer($messageQueues, $this->topic);
                Logger::debug("PublishingLoadBalancer created/updated, topic={}, queueCount={}, clientId={}", [
                    $this->topic,
                    count($messageQueues),
                    $this->clientId
                ]);
            } catch (\InvalidArgumentException $e) {
                Logger::warn("Failed to create PublishingLoadBalancer, topic={}, error={}, clientId={}", [
                    $this->topic,
                    $e->getMessage(),
                    $this->clientId
                ]);
                // Don't throw exception here - allow sending to continue with fallback logic
            }
        }
        
        return $response;
    }
    
    /**
     * Query route information (no cache, direct query)
     * 
     * @return \Apache\Rocketmq\V2\QueryRouteResponse Route response
     * @throws \Exception If query fails
     * @deprecated It is recommended to use queryRoute() method
     */
    public function queryRouteDirect()
    {
        $qr = new QueryRouteRequest();
        $rs = new Resource();
        $rs->setResourceNamespace($this->config->getNamespace());
        $rs->setName($this->topic);
        $qr->setTopic($rs);
        $qr->setEndpoints($this->config->getEndpoints()->toProtobuf());
        
        list($response, $status) = $this->getClient()->QueryRoute($qr)->wait();
        
        if ($status->code !== STATUS_OK) {
            throw new \Exception(
                "Failed to query route: " . $status->details,
                $status->code
            );
        }
        
        return $response;
    }
    
    /**
     * Generate unique message ID
     * 
     * Format: V1 + timestamp (4 bytes) + sequence (4 bytes) in hex
     * Total length: 32 characters
     * 
     * @return string Unique message ID
     */
    /**
     * Generate unique message ID (compatible with Java client v5)
     * 
     * Format: 34 characters hex string
     * - Version (2 chars): "01"
     * - MAC address (12 chars): Lower 6 bytes of MAC address
     * - Process ID (4 chars): Lower 2 bytes of PID
     * - Timestamp (8 chars): Seconds since 2021-01-01 00:00:00 UTC
     * - Sequence (8 chars): Auto-increment counter
     * 
     * Example: 0156F7E71C361B21BC024CCDBE00000000
     * 
     * @return string 34-character hex message ID
     */
    private function generateMessageId(): string
    {
        // Custom epoch: 2021-01-01 00:00:00 UTC
        static $customEpoch = 1609459200; // strtotime('2021-01-01 00:00:00 UTC')
        static $sequence = 0;
        static $macAddress = null;
        static $processId = null;
        
        // Initialize MAC address and process ID once
        if ($macAddress === null) {
            // Get MAC address (lower 6 bytes)
            $macAddress = $this->getMacAddress();
            
            // Get process ID (lower 2 bytes)
            $processId = getmypid() & 0xFFFF;
        }
        
        // Calculate seconds since custom epoch
        $deltaSeconds = time() - $customEpoch;
        
        // Increment sequence (wrap around at 32-bit)
        $sequence = ($sequence + 1) & 0xFFFFFFFF;
        
        // Build message ID
        // Version (2 chars) + MAC (12 chars) + PID (4 chars) + Time (8 chars) + Seq (8 chars) = 34 chars
        $messageId = sprintf(
            '01%s%04X%08X%08X',
            $macAddress,
            $processId,
            $deltaSeconds,
            $sequence
        );
        
        return strtoupper($messageId);
    }
    
    /**
     * Get MAC address in hex format (lower 6 bytes, 12 hex chars)
     * 
     * @return string 12-character hex MAC address
     */
    private function getMacAddress(): string
    {
        // Try to get real MAC address
        $mac = '000000000000'; // Default fallback
        
        // macOS
        if (strtoupper(substr(PHP_OS, 0, 6)) === 'DARWIN') {
            $output = [];
            @exec('ifconfig en0 | grep ether', $output);
            if (!empty($output)) {
                preg_match('/([0-9a-f]{2}[:-]){5}[0-9a-f]{2}/i', $output[0], $matches);
                if (!empty($matches)) {
                    $mac = str_replace([':', '-'], '', $matches[0]);
                }
            }
        } else {
            // Linux
            $output = [];
            @exec('cat /sys/class/net/eth0/address', $output);
            if (!empty($output)) {
                $mac = str_replace([':', '-'], '', trim($output[0]));
            }
        }
        
        // Ensure exactly 12 characters
        return strtoupper(substr(str_pad($mac, 12, '0'), 0, 12));
    }
    
    /**
     * Build message object
     * 
     * @param string $body Message body
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @param string|null $messageGroup FIFO message group
     * @param int|null $deliveryTimestamp Scheduled message delivery timestamp (milliseconds)
     * @return Message Message object
     */
    private function buildMessage($body, $tag = null, $keys = null, $messageGroup = null, $deliveryTimestamp = null, $liteTopic = null, $priority = null, $properties = [])
    {
        $message = new Message();
        
        // Set topic
        $topic = new Resource();
        $topic->setName($this->topic);
        $message->setTopic($topic);
        
        // Set message body
        $message->setBody($body);
        
        // Generate unique message ID
        $messageId = $this->generateMessageId();
        
        // Set system properties
        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        
        if ($tag !== null) {
            $systemProperties->setTag($tag);
        }
        
        if ($keys !== null) {
            if (is_array($keys)) {
                $systemProperties->setKeys($keys);
            } else {
                $systemProperties->setKeys([$keys]);
            }
        }
        
        if ($messageGroup !== null) {
            $systemProperties->setMessageGroup($messageGroup);
        }
        
        if ($deliveryTimestamp !== null) {
            // Set scheduled delivery timestamp
            $timestamp = new \Google\Protobuf\Timestamp();
            $timestamp->setSeconds(intval($deliveryTimestamp / 1000));
            $timestamp->setNanos(($deliveryTimestamp % 1000) * 1000000);
            $systemProperties->setDeliveryTimestamp($timestamp);
        }
        
        if ($liteTopic !== null) {
            $systemProperties->setLiteTopic($liteTopic);
        }
        
        if ($priority !== null) {
            $systemProperties->setPriority($priority);
        }
        
        $message->setSystemProperties($systemProperties);
        
        // Set custom properties
        if (!empty($properties)) {
            $message->setUserProperties($properties);
        }
        
        return $message;
    }
    
    /**
     * Send normal message (synchronous, with retry)
     * 
     * @param string $body Message body
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendNormalMessage($body, $tag = null, $keys = null)
    {
        $message = $this->buildMessage($body, $tag, $keys);
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send FIFO message (ordered message, with retry)
     * 
     * FIFO messages guarantee strict ordering within the same message group.
     * Messages with the same messageGroup are delivered and consumed in FIFO order.
     * 
     * Important notes:
     * 1. messageGroup is required and cannot be empty
     * 2. Messages with the same messageGroup are routed to the same queue
     * 3. FIFO messages do not support batch sending with different messageGroups
     * 4. If send fails, the entire messageGroup is blocked until retry succeeds
     * 
     * @param string $body Message body
     * @param string $messageGroup Message group ID (messages in the same group are delivered in order)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \InvalidArgumentException If messageGroup is empty
     * @throws \Exception If send fails
     */
    public function sendFifoMessage($body, $messageGroup, $tag = null, $keys = null)
    {
        // Validate messageGroup - FIFO messages MUST have a message group
        if (empty($messageGroup)) {
            throw new \InvalidArgumentException(
                "FIFO message requires a non-empty messageGroup. " .
                "Messages with the same messageGroup are delivered in strict FIFO order."
            );
        }
        
        Logger::debug("Preparing to send FIFO message, topic={}, messageGroup={}, tag={}, keys={}", [
            $this->topic,
            $messageGroup,
            $tag ?? 'null',
            $keys ?? 'null'
        ]);
        
        $message = $this->buildMessage($body, $tag, $keys, $messageGroup);
        
        // Set message type to FIFO
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::FIFO);
        $message->setSystemProperties($systemProperties);
        
        Logger::info("Sending FIFO message, topic={}, messageGroup={}, messageId={}", [
            $this->topic,
            $messageGroup,
            method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
        ]);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send scheduled/delayed message (with retry)
     * 
     * Delay messages are delivered to consumers after a specified delay time.
     * The message is stored in RocketMQ and becomes visible to consumers only after
     * the delivery timestamp is reached.
     * 
     * Important notes:
     * 1. delaySeconds must be positive (> 0)
     * 2. Maximum delay time depends on broker configuration (typically up to 40 days)
     * 3. Delay messages do not support transaction semantics
     * 4. Delivery timestamp = current time + delaySeconds
     * 
     * @param string $body Message body
     * @param int $delaySeconds Delay seconds (must be > 0)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \InvalidArgumentException If delaySeconds is invalid
     * @throws \Exception If send fails
     */
    public function sendDelayMessage($body, $delaySeconds, $tag = null, $keys = null)
    {
        // Validate delaySeconds
        if (!is_numeric($delaySeconds) || $delaySeconds <= 0) {
            throw new \InvalidArgumentException(
                "Delay seconds must be a positive number. Got: {$delaySeconds}"
            );
        }
        
        // Calculate delivery timestamp (current time + delay)
        $deliveryTimestamp = (time() + $delaySeconds) * 1000; // Convert to milliseconds
        
        Logger::debug("Preparing to send delay message, topic={}, delaySeconds={}, deliveryTimestamp={}, tag={}, keys={}", [
            $this->topic,
            $delaySeconds,
            $deliveryTimestamp,
            $tag ?? 'null',
            $keys ?? 'null'
        ]);
        
        $message = $this->buildMessage($body, $tag, $keys, null, $deliveryTimestamp);
        
        Logger::info("Sending delay message, topic={}, delaySeconds={}, deliveryTimestamp={}, messageId={}", [
            $this->topic,
            $delaySeconds,
            $deliveryTimestamp,
            method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
        ]);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send scheduled message with specific delivery timestamp
     * 
     * This method allows you to specify an exact delivery timestamp instead of
     * calculating from delay seconds. Useful for scheduling messages at specific times.
     * 
     * Example usage:
     * - Schedule message for tomorrow: time() + 86400 (seconds)
     * - Schedule message for next week: time() + 604800 (seconds)
     * 
     * @param string $body Message body
     * @param int $deliveryTimestamp Delivery timestamp in milliseconds (Unix timestamp * 1000)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \InvalidArgumentException If deliveryTimestamp is invalid
     * @throws \Exception If send fails
     */
    public function sendScheduledMessage($body, $deliveryTimestamp, $tag = null, $keys = null)
    {
        // Validate deliveryTimestamp
        if (!is_numeric($deliveryTimestamp) || $deliveryTimestamp <= 0) {
            throw new \InvalidArgumentException(
                "Delivery timestamp must be a positive number (milliseconds since epoch). Got: {$deliveryTimestamp}"
            );
        }
        
        $currentTimeMs = time() * 1000;
        $delaySeconds = round(($deliveryTimestamp - $currentTimeMs) / 1000);
        
        Logger::debug("Preparing to send scheduled message, topic={}, deliveryTimestamp={}, delaySeconds={}, tag={}, keys={}", [
            $this->topic,
            $deliveryTimestamp,
            $delaySeconds,
            $tag ?? 'null',
            $keys ?? 'null'
        ]);
        
        $message = $this->buildMessage($body, $tag, $keys, null, $deliveryTimestamp);
        
        Logger::info("Sending scheduled message, topic={}, deliveryTimestamp={}, delaySeconds={}, messageId={}", [
            $this->topic,
            $deliveryTimestamp,
            $delaySeconds,
            method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
        ]);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send lite topic message (with retry)
     * 
     * Lite topics are lightweight sub-topics under a parent topic.
     * They provide better resource isolation and quota management compared to regular topics.
     * 
     * Important notes:
     * 1. liteTopic must not be empty
     * 2. Lite topic has quota limits (configurable on broker)
     * 3. LiteTopicQuotaExceededException may be thrown if quota is exceeded
     * 4. Lite messages do not support transaction semantics
     * 
     * @param string $body Message body
     * @param string $liteTopic Lite topic name (sub-topic under parent topic)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @param array $properties Custom properties
     * @return SendReceipt Send result
     * @throws \InvalidArgumentException If liteTopic is empty
     * @throws \Exception If send fails or quota exceeded
     */
    public function sendLiteMessage($body, $liteTopic, $tag = null, $keys = null, $properties = [])
    {
        // Validate liteTopic
        if (empty($liteTopic)) {
            throw new \InvalidArgumentException(
                "Lite topic name must not be empty. Lite topics provide lightweight sub-topic isolation."
            );
        }
        
        Logger::debug("Preparing to send lite message, topic={}, liteTopic={}, tag={}, keys={}", [
            $this->topic,
            $liteTopic,
            $tag ?? 'null',
            $keys ?? 'null'
        ]);
        
        $message = $this->buildMessage($body, $tag, $keys, null, null, $liteTopic, null, $properties);
        
        // Set message type to LITE
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::LITE);
        $message->setSystemProperties($systemProperties);
        
        Logger::info("Sending lite message, topic={}, liteTopic={}, messageId={}", [
            $this->topic,
            $liteTopic,
            method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
        ]);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send priority message (with retry)
     * 
     * @param string $body Message body
     * @param int $priority Message priority (0-10)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @param array $properties Custom properties
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendPriorityMessage($body, $priority, $tag = null, $keys = null, $properties = [])
    {
        $message = $this->buildMessage($body, $tag, $keys, null, null, null, $priority, $properties);
        
        // Set message type to PRIORITY
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::PRIORITY);
        $message->setSystemProperties($systemProperties);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send message with custom properties (with retry)
     * 
     * @param string $body Message body
     * @param array $properties Custom properties
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendMessageWithProperties($body, $properties, $tag = null, $keys = null)
    {
        $message = $this->buildMessage($body, $tag, $keys, null, null, null, null, $properties);
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * {@inheritdoc}
     */
    public function send(MessageInterface $message, Transaction $transaction = null): SendReceipt
    {
        // Convert MessageInterface to gRPC Message if needed
        if ($message instanceof MessageInterface && !($message instanceof Message)) {
            // Pass transaction flag to convertToGrpcMessage so it sets the correct message type
            $grpcMessage = $this->convertToGrpcMessage($message, $transaction !== null);
        } else {
            $grpcMessage = $message;
        }
        
        if ($transaction !== null) {
            // Validate transaction state
            $txState = method_exists($transaction, 'getState') ? $transaction->getState() : 'UNKNOWN';
            if ($txState === 'COMMITTED' || $txState === 'ROLLED_BACK') {
                throw new \Exception(
                    "Cannot send message to transaction in terminal state: {$txState}. " .
                    "Create a new transaction using beginTransaction()."
                );
            }
            
            Logger::debug("Sending transactional half message, topic={}, transactionId={}, txState={}, messageId={}", [
                $this->topic,
                method_exists($transaction, 'getTransactionId') ? $transaction->getTransactionId() : 'unknown',
                $txState,
                method_exists($grpcMessage, 'getMessageId') ? $grpcMessage->getMessageId() : 'unknown'
            ]);
            
            // Send transaction message (half message)
            $sendReceipt = $this->sendMessageWithRetry($grpcMessage);
            
            Logger::info("Transactional half message sent successfully, messageId={}, transactionId={}, topic={}", [
                $sendReceipt->getMessageId(),
                method_exists($transaction, 'getTransactionId') ? $transaction->getTransactionId() : 'unknown',
                $this->topic
            ]);
            
            // Add message to transaction (using tryAddMessage and tryAddReceipt)
            $transaction->tryAddMessage($message);
            $transaction->tryAddReceipt($message, $sendReceipt);
            
            Logger::debug("Message added to transaction, messageId={}, transactionId={}, messageCount={}", [
                $sendReceipt->getMessageId(),
                method_exists($transaction, 'getTransactionId') ? $transaction->getTransactionId() : 'unknown',
                method_exists($transaction, 'getMessageCount') ? $transaction->getMessageCount() : 1
            ]);
            
            return $sendReceipt;
        }
        
        return $this->sendMessageWithRetry($grpcMessage);
    }



    /**
     * {@inheritdoc}
     */
    public function beginTransaction(): Transaction
    {
        if ($this->transactionChecker === null) {
            throw new ClientException("Transaction checker is not set. Use getTransactionalInstance() to create a transactional producer.");
        }
        
        return new TransactionImpl($this);
    }

    /**
     * {@inheritdoc}
     */
    public function recallMessage(string $topic, string $recallHandle): RecallReceipt
    {
        // Check state
        ClientState::checkState($this->state, [ClientState::RUNNING], 'recall message');
        
        // Validate parameters
        if (empty($topic)) {
            throw new \InvalidArgumentException("Topic cannot be empty");
        }
        
        if (empty($recallHandle)) {
            throw new \InvalidArgumentException("Recall handle cannot be empty. Recall handle is returned when sending delay messages.");
        }
        
        Logger::info("Attempting to recall delay message, topic={}, recallHandle={}, clientId={}", [
            $topic,
            $recallHandle,
            $this->clientId
        ]);
        
        // Record recall start time
        $startTime = microtime(true);
        
        try {
            // Increment total recall counter
            $this->metricsCollector->incrementCounter(MetricName::RECALL_TOTAL, [
                MetricLabels::TOPIC => $topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            // Build recall request
            $request = new \Apache\Rocketmq\V2\RecallMessageRequest();
            
            $topicResource = new \Apache\Rocketmq\V2\Resource();
            $topicResource->setName($topic);
            $request->setTopic($topicResource);
            $request->setRecallHandle($recallHandle);
            
            Logger::debug("Sending recall message request, topic={}, recallHandle={}", [
                $topic,
                $recallHandle
            ]);
            
            // Send recall request
            $call = $this->getClient()->RecallMessage($request);
            list($response, $grpcStatus) = $call->wait();
            
            // Check response status
            $status = $response->getStatus();
            if ($status->getCode() !== \Apache\Rocketmq\V2\Code::OK) {
                $errorMessage = "Failed to recall message: " . $status->getMessage();
                Logger::error("Failed to recall message, topic={}, recallHandle={}, code={}, message={}", [
                    $topic,
                    $recallHandle,
                    $status->getCode(),
                    $status->getMessage()
                ]);
                
                // Record failure metrics
                $this->metricsCollector->incrementCounter(MetricName::RECALL_FAILURE_TOTAL, [
                    MetricLabels::TOPIC => $topic,
                    MetricLabels::CLIENT_ID => $this->clientId,
                ]);
                
                throw new ClientException($errorMessage, $status->getCode());
            }
            
            // Create recall receipt
            $messageId = $response->getMessageId();
            $receipt = new RecallReceipt($messageId, true);
            
            // Calculate cost time
            $costTime = (microtime(true) - $startTime) * 1000; // Milliseconds
            
            Logger::info("Recall delay message successfully, topic={}, recallHandle={}, recalledMessageId={}, costTime={}ms, clientId={}", [
                $topic,
                $recallHandle,
                $messageId,
                round($costTime, 2),
                $this->clientId
            ]);
            
            // Record success metrics
            $this->metricsCollector->incrementCounter(MetricName::RECALL_SUCCESS_TOTAL, [
                MetricLabels::TOPIC => $topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            // Record cost time histogram
            $this->metricsCollector->observeHistogram(MetricName::RECALL_COST_TIME, [
                MetricLabels::TOPIC => $topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ], $costTime);
            
            return $receipt;
            
        } catch (ClientException $e) {
            throw $e;
        } catch (\Throwable $e) {
            // Record failure metrics
            $this->metricsCollector->incrementCounter(MetricName::RECALL_FAILURE_TOTAL, [
                MetricLabels::TOPIC => $topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            Logger::error("Failed to recall message, topic={}, recallHandle={}, error={}, clientId={}", [
                $topic,
                $recallHandle,
                $e->getMessage(),
                $this->clientId
            ]);
            throw new ClientException("Failed to recall message: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function recallMessageAsync(string $topic, string $recallHandle)
    {
        // Check state
        ClientState::checkState($this->state, [ClientState::RUNNING], 'recall message async');
        
        // Validate parameters
        if (empty($topic)) {
            throw new \InvalidArgumentException("Topic cannot be empty");
        }
        
        if (empty($recallHandle)) {
            throw new \InvalidArgumentException("Recall handle cannot be empty. Recall handle is returned when sending delay messages.");
        }
        
        Logger::debug("Attempting to recall delay message asynchronously, topic={}, recallHandle={}, clientId={}", [
            $topic,
            $recallHandle,
            $this->clientId
        ]);
        
        try {
            // Build recall request
            $request = new \Apache\Rocketmq\V2\RecallMessageRequest();
            
            $topicResource = new \Apache\Rocketmq\V2\Resource();
            $topicResource->setName($topic);
            $request->setTopic($topicResource);
            $request->setRecallHandle($recallHandle);
            
            // Send async recall request
            $call = $this->getClient()->RecallMessage($request);
            
            Logger::debug("Recall message async request sent, topic={}, recallHandle={}", [
                $topic,
                $recallHandle
            ]);
            
            // Return call object for async handling
            return $call;
            
        } catch (\Throwable $e) {
            Logger::error("Failed to send async recall message request, topic={}, recallHandle={}, error={}, clientId={}", [
                $topic,
                $recallHandle,
                $e->getMessage(),
                $this->clientId
            ]);
            throw new ClientException("Failed to send async recall message request: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function close()
    {
        $this->shutdown();
    }

    /**
     * Convert MessageInterface to gRPC Message
     *
     * @param MessageInterface $message
     * @param bool $isTransaction Whether this is a transaction message
     * @return Message
     */
    private function convertToGrpcMessage(MessageInterface $message, bool $isTransaction = false): Message
    {
        $grpcMessage = new Message();
        
        // Set topic
        $topic = new Resource();
        $topic->setName($message->getTopic());
        $grpcMessage->setTopic($topic);
        
        // Set message body
        $grpcMessage->setBody($message->getBody());
        
        // Set system properties
        $systemProperties = new SystemProperties();
        
        // Generate unique message ID
        $messageId = $this->generateMessageId();
        $systemProperties->setMessageId($messageId);
        
        // Set message type based on message properties (similar to Node.js logic)
        $messageGroup = $message->getMessageGroup();
        $deliveryTimestamp = $message->getDeliveryTimestamp();
        
        if (!$messageGroup && !$deliveryTimestamp && !$isTransaction) {
            // Normal message
            $systemProperties->setMessageType(MessageType::NORMAL);
        } elseif ($messageGroup && !$isTransaction) {
            // FIFO message - validate messageGroup is not empty
            if (empty($messageGroup)) {
                throw new ClientException(
                    "FIFO message requires a non-empty messageGroup. " .
                    "Messages with the same messageGroup are delivered in strict FIFO order."
                );
            }
            $systemProperties->setMessageType(MessageType::FIFO);
            Logger::debug("Converted to FIFO message, messageGroup={}, messageId={}", [
                $messageGroup,
                $messageId
            ]);
        } elseif ($deliveryTimestamp && !$isTransaction) {
            // Delay message
            $systemProperties->setMessageType(MessageType::DELAY);
        } elseif (!$messageGroup && !$deliveryTimestamp && $isTransaction) {
            // Transaction message
            $systemProperties->setMessageType(MessageType::TRANSACTION);
        } else {
            // Conflict: transaction with fifo/delay
            Logger::error(
                "Transactional message should not set messageGroup or deliveryTimestamp, messageGroup={}, deliveryTimestamp={}",
                [$messageGroup ?? 'null', $deliveryTimestamp ?? 'null']
            );
            throw new ClientException("Transactional message should not set messageGroup or deliveryTimestamp");
        }
        
        $tag = $message->getTag();
        if ($tag !== null) {
            $systemProperties->setTag($tag);
        }
        
        $keys = $message->getKeys();
        if (!empty($keys)) {
            $systemProperties->setKeys($keys);
        }
        
        if ($messageGroup !== null) {
            $systemProperties->setMessageGroup($messageGroup);
        }
        
        if ($deliveryTimestamp !== null) {
            $timestamp = new \Google\Protobuf\Timestamp();
            $timestamp->setSeconds(intval($deliveryTimestamp / 1000));
            $timestamp->setNanos(($deliveryTimestamp % 1000) * 1000000);
            $systemProperties->setDeliveryTimestamp($timestamp);
        }
        
        $liteTopic = $message->getLiteTopic();
        if ($liteTopic !== null) {
            $systemProperties->setLiteTopic($liteTopic);
            Logger::debug("Set lite topic, liteTopic={}, messageId={}", [
                $liteTopic,
                $messageId
            ]);
        }
        
        $priority = $message->getPriority();
        if ($priority !== null) {
            // Validate priority range
            if ($priority < 0 || $priority > 10) {
                Logger::warn(
                    "Priority value out of recommended range (0-10), priority={}, messageId={}",
                    [$priority, $messageId]
                );
            }
            $systemProperties->setPriority($priority);
            Logger::debug("Set message priority, priority={}, messageId={}", [
                $priority,
                $messageId
            ]);
        }
        
        $grpcMessage->setSystemProperties($systemProperties);
        
        // Set custom properties
        $properties = $message->getProperties();
        if (!empty($properties)) {
            $grpcMessage->setUserProperties($properties);
        }
        
        return $grpcMessage;
    }
    

    
    /**
     * Batch send messages
     * 
     * Send multiple messages in one gRPC request to improve throughput
     * 
     * Notes:
     * 1. All messages must have the same topic
     * 2. Maximum batch size is 100 messages by default
     * 3. Maximum total message size is 4MB by default
     * 4. FIFO messages do not support batch sending
     * 
     * @param array $messages Message array [['body' => '...', 'tag' => '...', 'keys' => '...'], ...]
     * @param int $maxBatchSize Maximum batch size, default 100
     * @param int $maxMessageSize Maximum total message size in bytes, default 4MB
     * @return array Send result array
     * @throws \Exception If send fails or parameters are invalid
     */
    public function sendBatchMessages($messages, $maxBatchSize = 100, $maxMessageSize = 4 * 1024 * 1024)
    {
        // Validate input
        if (empty($messages)) {
            throw new \Exception("Messages array cannot be empty");
        }
        
        if (!is_array($messages)) {
            throw new \Exception("Messages must be an array");
        }
        
        // Limit batch size
        $totalMessages = count($messages);
        if ($totalMessages > $maxBatchSize) {
            throw new \Exception(
                "Batch size exceeds limit. Count: {$totalMessages}, Max: {$maxBatchSize}"
            );
        }
        
        // Build message object array
        $messageObjects = [];
        $totalSize = 0;
        $hasFifoMessage = false;
        $fifoMessageGroup = null;
        
        // Pre-allocate array for better performance
        $messageObjects = array_fill(0, $totalMessages, null);
        
        foreach ($messages as $index => $msgData) {
            if (!is_array($msgData) || !isset($msgData['body'])) {
                throw new \Exception("Invalid message format at index {$index}. Expected: ['body' => '...', 'tag' => '...', 'keys' => '...']");
            }
            
            $body = $msgData['body'];
            $tag = isset($msgData['tag']) ? $msgData['tag'] : null;
            $keys = isset($msgData['keys']) ? $msgData['keys'] : null;
            $messageGroup = isset($msgData['messageGroup']) ? $msgData['messageGroup'] : null;
            $deliveryTimestamp = isset($msgData['deliveryTimestamp']) ? $msgData['deliveryTimestamp'] : null;
            
            // Check if it is a FIFO message
            if ($messageGroup !== null) {
                $hasFifoMessage = true;
                
                // Validate all FIFO messages have the same messageGroup (Java behavior)
                if ($fifoMessageGroup === null) {
                    $fifoMessageGroup = $messageGroup;
                } elseif ($fifoMessageGroup !== $messageGroup) {
                    throw new \InvalidArgumentException(
                        "FIFO messages in batch must have the same messageGroup. " .
                        "Expected: '{$fifoMessageGroup}', Got: '{$messageGroup}' at index {$index}"
                    );
                }
            }
            
            $message = $this->buildMessage($body, $tag, $keys, $messageGroup, $deliveryTimestamp);
            $messageObjects[$index] = $message;
            
            // Calculate total size
            $totalSize += strlen($body);
        }
        
        // FIFO messages do not support batch sending with different groups
        // But single messageGroup FIFO batch is allowed
        if ($hasFifoMessage && $totalMessages > 1) {
            Logger::warn(
                "Batch sending FIFO messages with same messageGroup={}, count={}. Note: FIFO ordering is guaranteed within the group.",
                [$fifoMessageGroup, $totalMessages]
            );
        }
        
        // Check total size
        if ($totalSize > $maxMessageSize) {
            throw new \Exception(
                "Total message size exceeds limit. Size: " . round($totalSize / 1024 / 1024, 2) . "MB, Max: " . round($maxMessageSize / 1024 / 1024, 2) . "MB"
            );
        }
        
        // Send batch messages
        return $this->sendBatchMessagesInternal($messageObjects);
    }
    
    /**
     * Batch send message objects (internal method)
     * 
     * @param array $messageObjects Message object array
     * @return array Send result
     * @throws \Exception If send fails
     */
    private function sendBatchMessagesInternal($messageObjects)
    {
        // Check state
        ClientState::checkState($this->state, [ClientState::RUNNING], 'send batch messages');
        
        // Execute before-send interceptors
        $context = null;
        if (!empty($this->interceptors)) {
            $context = $this->doBeforeSend($messageObjects);
        }
        
        try {
            // Build request
            $request = new SendMessageRequest();
            $request->setMessages($messageObjects);
            
            // Send request
            $call = $this->getClient()->SendMessage($request);
            $response = $call->wait();
            
            // Check response status
            if ($response->getStatus()->getCode() !== 0) {
                throw new \Exception(
                    "Failed to batch send messages: " . $response->getStatus()->getMessage(),
                    $response->getStatus()->getCode()
                );
            }
            
            // Parse send results
            $results = [];
            foreach ($response->getEntries() as $entry) {
                $results[] = [
                    'messageId' => $entry->getMessageId(),
                    'transactionId' => $entry->getTransactionId(),
                    'target' => $entry->getTarget(),
                    'status' => $entry->getStatus()
                ];
            }
            
            // Execute after-send interceptors
            if (!empty($this->interceptors) && $context !== null) {
                $this->doAfterSend($context, $messageObjects, $results);
            }
            
            return $results;
        } catch (\Throwable $e) {
            // Execute after-send interceptors (exception case)
            if (!empty($this->interceptors) && $context !== null) {
                $this->doAfterSend($context, $messageObjects, null, $e);
            }
            throw $e;
        }
    }
    
    /**
     * Send message to gRPC server (no retry)
     * 
     * @param Message $message Message object
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    private function sendMessage($message)
    {
        // Check state - only RUNNING state can send messages
        ClientState::checkState($this->state, [ClientState::RUNNING], 'send message');
        
        // Record send start time
        $startTime = microtime(true);
        
        // Execute before-send interceptors
        $context = null;
        if (!empty($this->interceptors)) {
            $context = $this->doBeforeSend([$message]);
        }
        
        try {
            // Increment total send counter
            $this->metricsCollector->incrementCounter(MetricName::SEND_TOTAL, [
                MetricLabels::TOPIC => $this->topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            // Validate message topic matches producer topic
            $messageTopic = method_exists($message, 'getTopic') ? $message->getTopic() : null;
            if ($messageTopic !== null && is_object($messageTopic)) {
                // If messageTopic is a Resource object, get its name
                $messageTopic = method_exists($messageTopic, 'getName') ? $messageTopic->getName() : (string)$messageTopic;
            }
            if ($messageTopic !== null && $messageTopic !== $this->topic) {
                throw new \InvalidArgumentException(
                    "Message topic '{$messageTopic}' does not match producer topic '{$this->topic}'"
                );
            }
            
            Logger::debug("Sending message, topic={}, messageId={}", [
                $this->topic,
                method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
            ]);
            
            $request = new SendMessageRequest();
            $request->setMessages([$message]);
            
            $call = $this->getClient()->SendMessage($request);
            list($response, $status) = $call->wait();
            
            // Check gRPC status
            if ($status->code !== STATUS_OK) {
                throw new \Exception(
                    "gRPC call failed: " . $status->details,
                    $status->code
                );
            }
            
            // Calculate cost time
            $costTime = (microtime(true) - $startTime) * 1000; // Milliseconds
            
            // Check response status
            $status = $response->getStatus();
            if ($status === null) {
                throw new \Exception("Response status is null");
            }
            
            $statusCode = $status->getCode();
            $statusMessage = $status->getMessage();
            
            // In RocketMQ, code 20000 means OK (success)
            if ($statusCode !== 20000) {
                throw new \Exception(
                    "Failed to send message: " . $response->getStatus()->getMessage(),
                    $response->getStatus()->getCode()
                );
            }
            
            // Parse send results
            $entries = $response->getEntries();
            if (empty($entries)) {
                throw new \Exception("No send result returned");
            }
            
            $entry = $entries[0];
            $messageId = $entry->getMessageId();
            $topic = $this->topic;
            $queueId = 0; // Default queue ID
            $offset = 0; // Default offset
            
            Logger::info("Message sent successfully, messageId={}, topic={}, costTime={}ms", [
                $messageId,
                $topic,
                round($costTime, 2)
            ]);
            
            // Extract transaction ID (for transaction messages)
            $transactionId = null;
            if (method_exists($entry, 'getTransactionId')) {
                $transactionId = $entry->getTransactionId();
                if ($transactionId === '') {
                    $transactionId = null;
                }
            }
            
            // Extract recall handle (for message recall)
            $recallHandle = null;
            if (method_exists($entry, 'getRecallHandle')) {
                $recallHandle = $entry->getRecallHandle();
                if ($recallHandle === '') {
                    $recallHandle = null;
                }
            }
            
            // Get current client's endpoints for transaction commit/rollback
            // This ensures we send commit/rollback to the same broker that handled the half message
            $endpoints = $this->config->getEndpoints();
            
            // Create send receipt with transaction support
            $sendReceipt = new SendReceipt(
                $messageId, 
                $topic, 
                $queueId, 
                $offset,
                $transactionId,
                $recallHandle,
                $endpoints  // Pass endpoints for transaction operations
            );
            
            // Record success metrics
            $this->metricsCollector->incrementCounter(MetricName::SEND_SUCCESS_TOTAL, [
                MetricLabels::TOPIC => $this->topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            // Record cost time histogram
            $this->metricsCollector->observeHistogram(MetricName::SEND_COST_TIME, [
                MetricLabels::TOPIC => $this->topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ], $costTime);
            
            // Execute after-send interceptors
            if (!empty($this->interceptors) && $context !== null) {
                $this->doAfterSend($context, [$message], [$sendReceipt]);
            }
            
            return $sendReceipt;
        } catch (\Throwable $e) {
            // Record failure metrics
            $this->metricsCollector->incrementCounter(MetricName::SEND_FAILURE_TOTAL, [
                MetricLabels::TOPIC => $this->topic,
                MetricLabels::CLIENT_ID => $this->clientId,
            ]);
            
            // Log error with context
            Logger::error("Failed to send message, topic={}, error={}", [
                $this->topic,
                $e->getMessage()
            ]);
            
            // Execute after-send interceptors (exception case)
            if (!empty($this->interceptors) && $context !== null) {
                $this->doAfterSend($context, [$message], null, $e);
            }
            throw $e;
        }
    }
    
    /**
     * Send message (with retry mechanism)
     * 
     * @param Message $message Message object
     * @param int $attempt Current attempt count (starting from 1)
     * @return SendReceipt Send result
     * @throws \Exception If send fails and cannot retry
     */
    private function sendMessageWithRetry($message, $attempt = 1)
    {
        try {
            $receipt = $this->sendMessage($message);
            
            // Log successful resend (not first attempt)
            if ($attempt > 1) {
                Logger::info(
                    "Resend message successfully, topic={}, messageId={}, maxAttempts={}, attempt={}",
                    [
                        $this->topic,
                        $receipt->getMessageId(),
                        $this->maxAttempts,
                        $attempt
                    ]
                );
            }
            
            return $receipt;
        } catch (\Throwable $e) {
            // Determine whether to retry
            if (!$this->retryPolicy->shouldRetry($attempt, $e)) {
                // Not retryable or max retries reached, log final failure
                Logger::error(
                    "Failed to send message finally, run out of attempt times, maxAttempts={}, attempt={}, topic={}, messageId={}, error={}",
                    [
                        $this->maxAttempts,
                        $attempt,
                        $this->topic,
                        $message->getMessageId() ?? 'unknown',
                        $e->getMessage()
                    ]
                );
                throw $e;
            }
            
            // Calculate backoff time
            $delayMs = $this->retryPolicy->getNextAttemptDelay($attempt);
            
            // Check if this is a throttling error (TooManyRequests)
            $isThrottled = ($e->getCode() === 429 || strpos($e->getMessage(), 'too many requests') !== false);
            
            if ($isThrottled && $delayMs > 0) {
                // For throttling, log warning with delay info
                Logger::warn(
                    "Failed to send message due to too many requests, would attempt to resend after {}ms, maxAttempts={}, attempt={}, topic={}, messageId={}",
                    [
                        $delayMs,
                        $this->maxAttempts,
                        $attempt,
                        $this->topic,
                        $message->getMessageId() ?? 'unknown'
                    ]
                );
            } else {
                // For other errors, log warning for immediate retry
                Logger::warn(
                    "Failed to send message, would attempt to resend right now, maxAttempts={}, attempt={}, topic={}, messageId={}, error={}",
                    [
                        $this->maxAttempts,
                        $attempt,
                        $this->topic,
                        $message->getMessageId() ?? 'unknown',
                        $e->getMessage()
                    ]
                );
            }
            
            // Execute backoff wait
            if ($delayMs > 0) {
                usleep($delayMs * 1000); // Convert to microseconds
            }
            
            // Recursive retry
            return $this->sendMessageWithRetry($message, $attempt + 1);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function sendAsync($message, $callback = null)
    {
        // Check state
        ClientState::checkState($this->state, [ClientState::RUNNING], 'send async message');
        
        if ($message instanceof MessageInterface) {
            Logger::debug("Sending async message, topic={}, messageId={}", [
                $this->topic,
                method_exists($message, 'getMessageId') ? $message->getMessageId() : 'unknown'
            ]);
            
            // Build gRPC message
            $grpcMessage = $this->convertToGrpcMessage($message);
            
            $request = new SendMessageRequest();
            $request->setMessages([$grpcMessage]);
            
            $call = $this->getClient()->SendMessage($request);
            
            if ($callback !== null) {
                // Use gRPC async callback
                $call->wait(function($response, $error) use ($callback, $grpcMessage) {
                    if ($error) {
                        Logger::error("Failed to send async message, messageId={}, error={}", [
                            method_exists($grpcMessage, 'getMessageId') ? $grpcMessage->getMessageId() : 'unknown',
                            $error->getMessage()
                        ]);
                        call_user_func($callback, null, $error);
                    } else {
                        Logger::info("Async message sent successfully, messageId={}", [
                            method_exists($grpcMessage, 'getMessageId') ? $grpcMessage->getMessageId() : 'unknown'
                        ]);
                        call_user_func($callback, $response, null);
                    }
                });
                return null;
            }
            
            // Return a promise-like object
            return $call;
        } else {
            // Backward compatibility: old signature with body, callback, tag, keys
            $body = $message;
            $callback = func_get_arg(1);
            $tag = func_get_arg(2) ?? null;
            $keys = func_get_arg(3) ?? null;
            
            Logger::debug("Sending async message (legacy API), topic={}, tag={}, keys={}", [
                $this->topic,
                $tag ?? 'null',
                $keys ?? 'null'
            ]);
            
            $messageObj = $this->buildMessage($body, $tag, $keys);
            
            $request = new SendMessageRequest();
            $request->setMessages([$messageObj]);
            
            $call = $this->getClient()->SendMessage($request);
            
            // Use gRPC async callback
            $call->wait(function($response, $error) use ($callback, $messageObj) {
                if ($error) {
                    Logger::error("Failed to send async message (legacy API), error={}", [
                        $error->getMessage()
                    ]);
                    call_user_func($callback, null, $error);
                } else {
                    Logger::info("Async message sent successfully (legacy API), messageId={}", [
                        method_exists($messageObj, 'getMessageId') ? $messageObj->getMessageId() : 'unknown'
                    ]);
                    call_user_func($callback, $response, null);
                }
            });
            
            return null;
        }
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
     * Get Route cache instance
     * 
     * @return RouteCache Route cache
     */
    public function getRouteCache()
    {
        return $this->routeCache;
    }
    
    /**
     * Refresh route cache
     * 
     * @return void
     * @throws \Exception If refresh fails
     */
    public function refreshRouteCache()
    {
        $this->routeCache->invalidate($this->topic);
        $this->queryRoute(true); // Force refresh
    }
    
    /**
     * Get route cache statistics
     * 
     * @return array Statistics
     */
    public function getRouteCacheStats()
    {
        return $this->routeCache->getStats();
    }
    
    // ========================================
    // Transaction message related methods
    // ========================================
    

    
    /**
     * Send transaction message (half message)
     * 
     * Based on Java implementation:
     * 1. Add message to transaction
     * 2. Send half message
     * 3. Add receipt to transaction
     * 
     * @param string $body Message body
     * @param Transaction $transaction Transaction object
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return array Send result
     * @throws \Exception If send fails
     */
    public function sendTransactionMessage($body, Transaction $transaction, $tag = null, $keys = null)
    {
        if ($this->transactionChecker === null) {
            throw new \Exception("Transaction checker is not set");
        }
        
        // Check if transaction is TransactionImpl
        if (!($transaction instanceof TransactionImpl)) {
            throw new \InvalidArgumentException("Invalid transaction type");
        }
        
        // Build message
        $message = $this->buildMessage($body, $tag, $keys);
        
        // Try to add message to transaction (Java: tryAddMessage)
        $transaction->tryAddMessage($message);
        
        // Set as transaction message type
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::TRANSACTION);
        $message->setSystemProperties($systemProperties);
        
        // Send half message
        $receipt = $this->sendMessageWithRetry($message);
        
        // Try to add receipt to transaction (Java: tryAddReceipt)
        $transaction->tryAddReceipt($message, $receipt);  // receipt is object, not array
        
        return [$receipt];  // Return as array for consistency
    }
    
    /**
     * End transaction (internal method)
     * 
     * @param mixed $endpoints Target endpoints (from SendReceipt)
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $resolution Transaction resolution (COMMIT/ROLLBACK)
     * @return void
     * @throws \Exception If ending transaction fails
     */
    /**
     * End transaction (internal method)
     * 
     * @param mixed $endpoints Target broker endpoints
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $resolution Transaction resolution (COMMIT/ROLLBACK)
     * @param string|null $topic Topic name (optional, defaults to producer's topic)
     * @return void
     * @throws \Exception If ending transaction fails
     */
    public function endTransactionInternal($endpoints, $messageId, $transactionId, $resolution, $topic = null)
    {
        $request = new \Apache\Rocketmq\V2\EndTransactionRequest();
        
        // Set topic - use provided topic or fallback to producer's topic
        $topicResource = new Resource();
        $topicResource->setName($topic !== null ? $topic : $this->topic);
        $topicResource->setResourceNamespace('');
        $request->setTopic($topicResource);
        
        // Set message ID and transaction ID
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        
        // Set transaction resolution
        switch ($resolution) {
            case TransactionResolution::COMMIT:
                $request->setResolution(\Apache\Rocketmq\V2\TransactionResolution::COMMIT);
                break;
            case TransactionResolution::ROLLBACK:
                $request->setResolution(\Apache\Rocketmq\V2\TransactionResolution::ROLLBACK);
                break;
            default:
                throw new \Exception("Invalid transaction resolution: {$resolution}");
        }
        
        // Set source (client-initiated commit/rollback)
        $request->setSource(\Apache\Rocketmq\V2\TransactionSource::SOURCE_CLIENT);
        
        // Log begin transaction (aligned with Java/Node.js)
        $resolutionStr = $resolution === TransactionResolution::COMMIT ? 'COMMIT' : 'ROLLBACK';
        Logger::info("Begin to end transaction, messageId={$messageId}, transactionId={$transactionId}, resolution={$resolutionStr}, source=SOURCE_CLIENT, clientId={$this->clientId}");
        
        // Get the correct client for the target endpoints
        // For transaction commit/rollback, we need to send to the same broker that handled the half message
        $client = $this->getClient();
        
        // Send request to the correct broker
        list($response, $status) = $client->EndTransaction($request)->wait();
        
        if ($status->code !== STATUS_OK) {
            throw new \Exception(
                "Failed to end transaction: " . $status->details,
                $status->code
            );
        }
        
        if ($response->getStatus()->getCode() !== 20000) {
            throw new \Exception(
                "Failed to end transaction: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
        
        // Log success (aligned with Java/Node.js)
        Logger::info("End transaction successfully, messageId={$messageId}, transactionId={$transactionId}, resolution={$resolutionStr}, source=SOURCE_CLIENT, clientId={$this->clientId}");
    }
    
    /**
     * Handle orphaned transaction check command (called by server)
     * 
     * @param \Apache\Rocketmq\V2\Message $message Half message
     * @param string $transactionId Transaction ID
     * @return void
     */
    public function handleOrphanedTransactionCheck($message, $transactionId)
    {
        $messageId = $message->getSystemProperties()->getMessageId();
        
        if ($this->transactionChecker === null) {
            Logger::error("No transaction checker registered, ignore it, messageId={$messageId}, transactionId={$transactionId}, clientId={$this->clientId}");
            return;
        }
        
        try {
            // Call user-defined checker
            $resolution = $this->transactionChecker->check($message);
            
            // End transaction based on check result
            $this->endTransactionInternal(
                null, // Get endpoints from route
                $messageId,
                $transactionId,
                $resolution
            );
            
            Logger::info("Recover orphaned transaction message success, transactionId={$transactionId}, resolution={$resolution}, messageId={$messageId}, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Exception raised while checking the transaction, messageId={$messageId}, transactionId={$transactionId}, clientId={$this->clientId}", ['error' => $e->getMessage()]);
        }
    }
    
    /**
     * Close Producer (graceful shutdown)
     * 
     * Performs the following operations:
     * 1. Stop accepting new requests
     * 2. Wait for in-progress requests to complete
     * 3. Clean up resources (connections, caches, etc.)
     * 4. Transition state to TERMINATED
     * 
     * @param int $timeoutSeconds Timeout in seconds, default 5 seconds
     * @return void
     * @throws \Exception If shutdown fails
     */
    public function shutdown($timeoutSeconds = 5)
    {
        // Check if can be closed
        ClientState::checkState(
            $this->state, 
            [ClientState::RUNNING, ClientState::STARTING, ClientState::CREATED], 
            'shutdown'
        );
        
        Logger::info("Begin to shutdown the rocketmq producer, clientId={}", [$this->clientId]);
        
        // If already in terminal state, return directly
        if (ClientState::isTerminalState($this->state)) {
            Logger::debug("The rocketmq producer already in terminal state, clientId={}", [$this->clientId]);
            return;
        }
        
        $this->state = ClientState::STOPPING;
        
        try {
            // 1. Clear route cache
            if ($this->routeCache !== null) {
                Logger::debug("Clearing route cache, topic={}, clientId={}", [
                    $this->topic,
                    $this->clientId
                ]);
                $this->routeCache->invalidate($this->topic);
            }
            
            // 2. Return gRPC connection to pool
            if ($this->client !== null) {
                try {
                    Logger::debug("Returning connection to pool, endpoints={}, clientId={}", [
                        $this->config->getEndpoints(),
                        $this->clientId
                    ]);
                    $pool = ConnectionPool::getInstance();
                    $pool->returnConnection($this->config, $this->client);
                    Logger::debug("Connection returned to pool successfully, clientId={}", [$this->clientId]);
                } catch (\Throwable $e) {
                    // Ignore exception when returning connection
                    Logger::error("Failed to return connection to pool, clientId={}", [
                        $this->clientId,
                        'error' => $e->getMessage()
                    ]);
                } finally {
                    $this->client = null;
                }
            }
            
            // 3. Clean up transaction-related resources
            if ($this->transactionChecker !== null) {
                Logger::debug("Cleaning up transaction checker, clientId={}", [$this->clientId]);
                $this->transactionChecker = null;
            }
            
            // 4. Clean up Health checker
            if ($this->healthChecker !== null) {
                Logger::debug("Resetting health checker, clientId={}", [$this->clientId]);
                $this->healthChecker->reset();
                $this->healthChecker = null;
            }
            
            // 5. Clear interceptors
            if (!empty($this->interceptors)) {
                Logger::debug("Clearing {} interceptors, clientId={}", [
                    count($this->interceptors),
                    $this->clientId
                ]);
                $this->interceptors = [];
            }
            
            // 6. Transition state to TERMINATED
            if (ClientState::canTransition($this->state, ClientState::TERMINATED)) {
                $this->state = ClientState::TERMINATED;
                
                // Shutdown meter manager
                if ($this->meterManager !== null) {
                    $this->meterManager->shutdown();
                }
                
                Logger::info("Shutdown the rocketmq producer successfully, clientId={}", [$this->clientId]);
            }
            
        } catch (\Throwable $e) {
            // Even if cleanup fails, set to TERMINATED
            $this->state = ClientState::TERMINATED;
            Logger::error("Failed to shutdown the rocketmq producer, clientId={}", [
                $this->clientId,
                'error' => $e->getMessage()
            ]);
            throw new \Exception("Failed to shutdown producer: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * Get current state
     * 
     * @return string Current state
     */
    public function getState()
    {
        return $this->state;
    }
    
    /**
     * Check if is running
     * 
     * @return bool Is running
     */
    public function isRunning()
    {
        return $this->state === ClientState::RUNNING;
    }
    
    /**
     * Check if is shutdown
     * 
     * @return bool Whether is shutdown
     */
    public function isShutdown()
    {
        return ClientState::isTerminalState($this->state);
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
     * Get client ID
     * 
     * @return string Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    // ========================================
    // Health check related methods
    // ========================================
    
    /**
     * Initialize Health checker
     * 
     * @return void
     */
    private function initHealthChecker()
    {
        if ($this->healthChecker === null) {
            $this->healthChecker = new HealthChecker(
                $this->config->getEndpoints(),
                $this->clientId,
                \Apache\Rocketmq\V2\ClientType::PRODUCER
            );
        }
    }
    
    /**
     * Execute health check
     * 
     * @return HealthCheckResult Check result
     * @throws \Exception If not started
     */
    public function healthCheck()
    {
        ClientState::checkState($this->state, [ClientState::RUNNING], 'health check');
        
        $this->initHealthChecker();
        return $this->healthChecker->check();
    }
    
    /**
     * Get last health check result
     * 
     * @return HealthCheckResult|null
     */
    public function getLastHealthCheckResult()
    {
        if ($this->healthChecker === null) {
            return null;
        }
        return $this->healthChecker->getLastResult();
    }
    
    /**
     * Is healthy
     * 
     * @return bool
     */
    public function isHealthy()
    {
        if ($this->healthChecker === null) {
            return false;
        }
        return $this->healthChecker->isHealthy();
    }
    
    /**
     * Get health check statistics
     * 
     * @return array|null Statistics
     */
    public function getHealthCheckStats()
    {
        if ($this->healthChecker === null) {
            return null;
        }
        return $this->healthChecker->getStats();
    }
    
    /**
     * Send heartbeat (alias method)
     * 
     * @return HealthCheckResult
     */
    public function heartbeat()
    {
        return $this->healthCheck();
    }
    
    /**
     * Destructor - ensure resources are released
     */
    public function __destruct()
    {
        // If shutdown is not called explicitly, clean up here
        if (!ClientState::isTerminalState($this->state)) {
            try {
                $this->shutdown(1); // Quick shutdown
            } catch (\Throwable $e) {
                // Ignore exceptions in destructor
                Logger::error("Error during producer destruction, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            }
        }
    }
    
    // ========================================
    // Metrics related methods
    // ========================================
    
    /**
     * Get Metrics collector
     * 
     * @return MetricsCollector Metrics collector
     */
    public function getMetricsCollector()
    {
        return $this->metricsCollector;
    }
    
    /**
     * Get all metrics data
     * 
     * @return array Metrics data array
     */
    public function getAllMetrics()
    {
        return $this->metricsCollector->getAllMetrics();
    }
    
    /**
     * Export metrics to Prometheus format
     * 
     * @return string Prometheus format metrics text
     */
    public function exportMetricsToPrometheus()
    {
        return $this->metricsCollector->exportToPrometheus();
    }
    
    /**
     * Export metrics to JSON format
     * 
     * @return string JSON format metrics data
     */
    public function exportMetricsToJson()
    {
        return $this->metricsCollector->exportToJson();
    }
    
    /**
     * Calculate send QPS
     * 
     * @return float|null QPS (messages per second)
     */
    public function calculateSendQps()
    {
        return $this->metricsCollector->calculateRate(MetricName::SEND_TOTAL, [
            MetricLabels::TOPIC => $this->topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
    }
    
    /**
     * Get send success rate
     * 
     * @return float|null Success rate (between 0-1)
     */
    public function getSendSuccessRate()
    {
        $successMetric = $this->metricsCollector->getMetric(MetricName::SEND_SUCCESS_TOTAL, [
            MetricLabels::TOPIC => $this->topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        $failureMetric = $this->metricsCollector->getMetric(MetricName::SEND_FAILURE_TOTAL, [
            MetricLabels::TOPIC => $this->topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        if (!$successMetric || !$failureMetric) {
            return null;
        }
        
        $total = $successMetric['value'] + $failureMetric['value'];
        if ($total == 0) {
            return null;
        }
        
        return $successMetric['value'] / $total;
    }
    
    /**
     * Get average send cost time
     * 
     * @return float|null Average cost time (milliseconds)
     */
    public function getAvgSendCostTime()
    {
        $metric = $this->metricsCollector->getMetric(MetricName::SEND_COST_TIME, [
            MetricLabels::TOPIC => $this->topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        if (!$metric || $metric['count'] == 0) {
            return null;
        }
        
        return $metric['sum'] / $metric['count'];
    }
    
    /**
     * Calculate recall QPS
     * 
     * @param string $topic Topic name
     * @return float|null QPS (recalls per second)
     */
    public function calculateRecallQps($topic = null)
    {
        $topic = $topic ?? $this->topic;
        return $this->metricsCollector->calculateRate(MetricName::RECALL_TOTAL, [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
    }
    
    /**
     * Get recall success rate
     * 
     * @param string $topic Topic name
     * @return float|null Success rate (between 0-1)
     */
    public function getRecallSuccessRate($topic = null)
    {
        $topic = $topic ?? $this->topic;
        
        $successMetric = $this->metricsCollector->getMetric(MetricName::RECALL_SUCCESS_TOTAL, [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        $failureMetric = $this->metricsCollector->getMetric(MetricName::RECALL_FAILURE_TOTAL, [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        if (!$successMetric || !$failureMetric) {
            return null;
        }
        
        $total = $successMetric['value'] + $failureMetric['value'];
        if ($total == 0) {
            return null;
        }
        
        return $successMetric['value'] / $total;
    }
    
    /**
     * Get average recall cost time
     * 
     * @param string $topic Topic name
     * @return float|null Average cost time (milliseconds)
     */
    public function getAvgRecallCostTime($topic = null)
    {
        $topic = $topic ?? $this->topic;
        
        $metric = $this->metricsCollector->getMetric(MetricName::RECALL_COST_TIME, [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CLIENT_ID => $this->clientId,
        ]);
        
        if (!$metric || $metric['count'] == 0) {
            return null;
        }
        
        return $metric['sum'] / $metric['count'];
    }
    
    /**
     * Set max attempts for message sending
     *
     * @param int $maxAttempts
     * @return void
     */
    public function setMaxAttempts($maxAttempts)
    {
        $this->maxAttempts = $maxAttempts;
    }
    
    /**
     * Set transaction checker
     *
     * @param TransactionChecker $transactionChecker
     * @return void
     */
    public function setTransactionChecker(TransactionChecker $transactionChecker)
    {
        $this->transactionChecker = $transactionChecker;
    }
    
    /**
     * Get max attempts
     *
     * @return int
     */
    public function getMaxAttempts()
    {
        return $this->maxAttempts;
    }
}
