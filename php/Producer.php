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

require_once __DIR__ . '/Util.php';
require_once __DIR__ . '/RouteCache.php';
require_once __DIR__ . '/ClientState.php';
require_once __DIR__ . '/Producer/Producer.php';

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
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\SystemProperties;
use Grpc\ChannelCredentials;
use const Grpc\STATUS_OK;

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
     * @var int Max attempts for message sending
     */
    private $maxAttempts = 3;
    
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
    }
    
    /**
     * Get Producer singleton instance (configuration object recommended)
     * 
     * Usage example:
     * ```php
     * // Method 1: Using configuration object (recommended)
     * $config = new ClientConfiguration('127.0.0.1:8080');
     * $producer = Producer::getInstance($config, 'my-topic');
     * 
     * // Method 2: Using legacy method (backward compatible)
     * $producer = Producer::getInstance('127.0.0.1:8080', 'my-topic');
     * ```
     * 
     * @param ClientConfiguration|string $configOrEndpoints Client configuration object or server endpoint string
     * @param string|null $topic Topic name（Used when first parameter is endpoints string）
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
        
        $this->state = ClientState::STARTING;
        
        try {
            // Verify connection and get route information
            $this->queryRoute();
            
            // Transition state to RUNNING
            if (ClientState::canTransition($this->state, ClientState::RUNNING)) {
                $this->state = ClientState::RUNNING;
            }
        } catch (\Exception $e) {
            // Transition state to FAILED
            if (ClientState::canTransition($this->state, ClientState::FAILED)) {
                $this->state = ClientState::FAILED;
            }
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
     * Execute before-send interceptors
     * 
     * @param array $messages Message array
     * @return MessageInterceptorContext Interceptor context
     */
    private function doBeforeSend($messages)
    {
        $context = new MessageInterceptorContext(MessageHookPoints::SEND, MessageHookPointsStatus::OK);
        
        foreach ($this->interceptors as $interceptor) {
            try {
                $interceptor->doBefore($context, $messages);
            } catch (\Exception $e) {
                // Log error but do not interrupt flow
                error_log("Interceptor before-send failed: " . $e->getMessage());
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
            } catch (\Exception $e) {
                // Log error but do not interrupt flow
                error_log("Interceptor after-send failed: " . $e->getMessage());
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
        // If force refresh, clear cache
        if ($forceRefresh) {
            $this->routeCache->invalidate($this->topic);
        }
        
        // Use cache to get route
        return $this->routeCache->getOrCreate($this->topic, function() {
            $qr = new QueryRouteRequest();
            $rs = new Resource();
            $rs->setResourceNamespace('');
            $rs->setName($this->topic);
            $qr->setTopic($rs);
            
            list($response, $status) = $this->getClient()->QueryRoute($qr)->wait();
            
            if ($status->code !== STATUS_OK) {
                throw new \Exception(
                    "Failed to query route: " . $status->details,
                    $status->code
                );
            }
            
            return $response;
        });
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
        $rs->setResourceNamespace('');
        $rs->setName($this->topic);
        $qr->setTopic($rs);
        
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
            $message->setProperties($properties);
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
     * @param string $body Message body
     * @param string $messageGroup Message group ID (messages in the same group are delivered in order)
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendFifoMessage($body, $messageGroup, $tag = null, $keys = null)
    {
        $message = $this->buildMessage($body, $tag, $keys, $messageGroup);
        
        // Set message type to FIFO
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::FIFO);
        $message->setSystemProperties($systemProperties);
        
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send scheduled/delayed message (with retry)
     * 
     * @param string $body Message body
     * @param int $delaySeconds Delay seconds
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendDelayMessage($body, $delaySeconds, $tag = null, $keys = null)
    {
        $deliveryTimestamp = (time() + $delaySeconds) * 1000; // Convert to milliseconds
        $message = $this->buildMessage($body, $tag, $keys, null, $deliveryTimestamp);
        return $this->sendMessageWithRetry($message);
    }
    
    /**
     * Send lite topic message (with retry)
     * 
     * @param string $body Message body
     * @param string $liteTopic Lite topic name
     * @param string|null $tag Message tag
     * @param string|null $keys Message keys
     * @param array $properties Custom properties
     * @return SendReceipt Send result
     * @throws \Exception If send fails
     */
    public function sendLiteMessage($body, $liteTopic, $tag = null, $keys = null, $properties = [])
    {
        $message = $this->buildMessage($body, $tag, $keys, null, null, $liteTopic, null, $properties);
        
        // Set message type to LITE
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::LITE);
        $message->setSystemProperties($systemProperties);
        
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
            $grpcMessage = $this->convertToGrpcMessage($message);
        } else {
            $grpcMessage = $message;
        }
        
        if ($transaction !== null) {
            // Set message type to TRANSACTION
            $systemProperties = $grpcMessage->getSystemProperties();
            $systemProperties->setMessageType(MessageType::TRANSACTION);
            $grpcMessage->setSystemProperties($systemProperties);
            
            // Send transaction message
            $sendReceipt = $this->sendMessageWithRetry($grpcMessage);
            
            // Add message to transaction
            $transaction->addMessage($message, $sendReceipt);
            
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
        // TODO: Implement recall message functionality
        throw new ClientException("Recall message functionality not implemented yet");
    }

    /**
     * {@inheritdoc}
     */
    public function recallMessageAsync(string $topic, string $recallHandle)
    {
        // TODO: Implement async recall message functionality
        throw new ClientException("Async recall message functionality not implemented yet");
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
     * @return Message
     */
    private function convertToGrpcMessage(MessageInterface $message): Message
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
        
        $tag = $message->getTag();
        if ($tag !== null) {
            $systemProperties->setTag($tag);
        }
        
        $keys = $message->getKeys();
        if (!empty($keys)) {
            $systemProperties->setKeys($keys);
        }
        
        $messageGroup = $message->getMessageGroup();
        if ($messageGroup !== null) {
            $systemProperties->setMessageGroup($messageGroup);
        }
        
        $deliveryTimestamp = $message->getDeliveryTimestamp();
        if ($deliveryTimestamp !== null) {
            $timestamp = new \Google\Protobuf\Timestamp();
            $timestamp->setSeconds(intval($deliveryTimestamp / 1000));
            $timestamp->setNanos(($deliveryTimestamp % 1000) * 1000000);
            $systemProperties->setDeliveryTimestamp($timestamp);
        }
        
        $liteTopic = $message->getLiteTopic();
        if ($liteTopic !== null) {
            $systemProperties->setLiteTopic($liteTopic);
        }
        
        $priority = $message->getPriority();
        if ($priority !== null) {
            $systemProperties->setPriority($priority);
        }
        
        $grpcMessage->setSystemProperties($systemProperties);
        
        // Set custom properties
        $properties = $message->getProperties();
        if (!empty($properties)) {
            $grpcMessage->setProperties($properties);
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
                throw new \Exception("FIFO messages do not support batch sending. Message index: {$index}");
            }
            
            $message = $this->buildMessage($body, $tag, $keys, $messageGroup, $deliveryTimestamp);
            $messageObjects[$index] = $message;
            
            // Calculate total size
            $totalSize += strlen($body);
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
        } catch (\Exception $e) {
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
            
            // Create send receipt
            $sendReceipt = new SendReceipt($messageId, $topic, $queueId, $offset);
            
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
        } catch (\Exception $e) {
            // Record failure metrics
            $this->metricsCollector->incrementCounter(MetricName::SEND_FAILURE_TOTAL, [
                MetricLabels::TOPIC => $this->topic,
                MetricLabels::CLIENT_ID => $this->clientId,
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
            return $this->sendMessage($message);
        } catch (\Exception $e) {
            // Determine whether to retry
            if (!$this->retryPolicy->shouldRetry($attempt, $e)) {
                // Not retryable or max retries reached, throw exception directly
                throw $e;
            }
            
            // Calculate backoff time
            $delayMs = $this->retryPolicy->getNextAttemptDelay($attempt);
            
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
        if ($message instanceof MessageInterface) {
            // Build gRPC message
            $grpcMessage = $this->convertToGrpcMessage($message);
            
            $request = new SendMessageRequest();
            $request->setMessages([$grpcMessage]);
            
            $call = $this->getClient()->SendMessage($request);
            
            if ($callback !== null) {
                // Use gRPC async callback
                $call->wait(function($response, $error) use ($callback) {
                    if ($error) {
                        call_user_func($callback, null, $error);
                    } else {
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
            
            $messageObj = $this->buildMessage($body, $tag, $keys);
            
            $request = new SendMessageRequest();
            $request->setMessages([$messageObj]);
            
            $call = $this->getClient()->SendMessage($request);
            
            // Use gRPC async callback
            $call->wait(function($response, $error) use ($callback) {
                if ($error) {
                    call_user_func($callback, null, $error);
                } else {
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
        
        // Build message
        $message = $this->buildMessage($body, $tag, $keys);
        
        // Set as transaction message type
        $systemProperties = $message->getSystemProperties();
        $systemProperties->setMessageType(MessageType::TRANSACTION);
        $message->setSystemProperties($systemProperties);
        
        // Send half message
        $receipt = $this->sendMessageWithRetry($message);
        
        // Add message to transaction
        $transaction->addMessage($message, $receipt[0]);
        
        return $receipt;
    }
    
    /**
     * End transaction (internal method)
     * 
     * @param mixed $endpoints Target endpoints
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $resolution Transaction resolution (COMMIT/ROLLBACK)
     * @return void
     * @throws \Exception If ending transaction fails
     */
    public function endTransactionInternal($endpoints, $messageId, $transactionId, $resolution)
    {
        $request = new \Apache\Rocketmq\V2\EndTransactionRequest();
        
        // Set topic
        $topic = new Resource();
        $topic->setName($this->topic);
        $topic->setResourceNamespace('');
        $request->setTopic($topic);
        
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
        
        // Send request
        list($response, $status) = $this->getClient()->EndTransaction($request)->wait();
        
        if ($status->code !== STATUS_OK) {
            throw new \Exception(
                "Failed to end transaction: " . $status->details,
                $status->code
            );
        }
        
        if ($response->getStatus()->getCode() !== 0) {
            throw new \Exception(
                "Failed to end transaction: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
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
        if ($this->transactionChecker === null) {
            error_log("No transaction checker registered, ignore orphaned transaction check");
            return;
        }
        
        try {
            // Call user-defined checker
            $resolution = $this->transactionChecker->check($message);
            
            // End transaction based on check result
            $messageId = $message->getSystemProperties()->getMessageId();
            $this->endTransactionInternal(
                null, // Get endpoints from route
                $messageId,
                $transactionId,
                $resolution
            );
            
            error_log("Orphaned transaction checked: messageId={$messageId}, transactionId={$transactionId}, resolution={$resolution}");
        } catch (\Exception $e) {
            error_log("Failed to check orphaned transaction: " . $e->getMessage());
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
        
        // If already in terminal state, return directly
        if (ClientState::isTerminalState($this->state)) {
            return;
        }
        
        $this->state = ClientState::STOPPING;
        
        try {
            // 1. Clear route cache
            if ($this->routeCache !== null) {
                $this->routeCache->invalidate($this->topic);
            }
            
            // 2. Return gRPC connection to pool
            if ($this->client !== null) {
                try {
                    $pool = ConnectionPool::getInstance();
                    $pool->returnConnection($this->config, $this->client);
                } catch (\Exception $e) {
                    // Ignore exception when returning connection
                    error_log("Failed to return connection to pool: " . $e->getMessage());
                } finally {
                    $this->client = null;
                }
            }
            
            // 3. Clean up transaction-related resources
            $this->transactionChecker = null;
            
            // 4. Clean up Health checker
            if ($this->healthChecker !== null) {
                $this->healthChecker->reset();
                $this->healthChecker = null;
            }
            
            // 4. Transition state to TERMINATED
            if (ClientState::canTransition($this->state, ClientState::TERMINATED)) {
                $this->state = ClientState::TERMINATED;
            }
            
        } catch (\Exception $e) {
            // Even if cleanup fails, set to TERMINATED
            $this->state = ClientState::TERMINATED;
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
            } catch (\Exception $e) {
                // Ignore exceptions in destructor
                error_log("Error during producer destruction: " . $e->getMessage());
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
