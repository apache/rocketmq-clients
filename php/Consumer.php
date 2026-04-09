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

use Apache\Rocketmq\Connection\ConnectionPool;
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
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;

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
            // Get connection from pool
            $pool = ConnectionPool::getInstance();
            // Set connection pool configuration from client configuration
            $pool->setConfigFromClientConfiguration($this->config);
            $this->client = $pool->getConnection($this->config);
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
        
        $settings = new Settings();
        $settings->setClientType(\Apache\Rocketmq\V2\ClientType::SIMPLE_CONSUMER);
        
        $request->setSettings($settings);
        
        $call = $this->getClient()->Heartbeat($request);
        $response = $call->wait();
        
        return $response;
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
        $maxMessageNum = $maxMessageNum ?: $this->maxMessageNum;
        $invisibleDuration = $invisibleDuration ?: $this->invisibleDuration;
        
        $request = new ReceiveMessageRequest();
        
        // Set message queue
        $mq = new MessageQueue();
        $resource = new Resource();
        $resource->setName($this->topic);
        $mq->setTopic($resource);
        $mq->setAcceptMessageTypes([MessageType::NORMAL]);
        
        $request->setMessageQueue($mq);
        $request->setMaxAttempts(16); // Maximum retry count
        $request->setInvisibleDuration(
            (new \Google\Protobuf\Duration())->setSeconds($invisibleDuration)
        );
        $request->setBatchSize($maxMessageNum);
        
        // Set long polling
        $request->setLongPollingTimeoutSeconds($this->awaitDuration);
        
        $call = $this->getClient()->ReceiveMessage($request);
        
        $messages = [];
        
        // Read response stream
        foreach ($call->responses() as $response) {
            if ($response->hasMessage()) {
                $messages[] = $response->getMessage();
            }
        }
        
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
        $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
        
        if (empty($receiptHandle)) {
            throw new \Exception("Message has no receipt_handle, cannot ACK");
        }
        
        $request = new AckMessageRequest();
        
        $entry = new AckMessageEntry();
        $entry->setReceiptHandle($receiptHandle);
        $entry->setMessageId($message->getSystemProperties()->getMessageId());
        
        $request->setEntries([$entry]);
        
        $call = $this->getClient()->AckMessage($request);
        $response = $call->wait();
        
        // Check response status
        if ($response->getStatus()->getCode() !== 0) {
            throw new \Exception(
                "Failed to ACK message: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
        
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
        $receiptHandle = $message->getSystemProperties()->getReceiptHandle();
        
        if (empty($receiptHandle)) {
            throw new \Exception("Message has no receipt_handle, cannot change visibility duration");
        }
        
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
        $response = $call->wait();
        
        // Check response status
        if ($response->getStatus()->getCode() !== 0) {
            throw new \Exception(
                "Failed to change visibility duration: " . $response->getStatus()->getMessage(),
                $response->getStatus()->getCode()
            );
        }
        
        return $response->getReceiptHandle();
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
}

/**
 * RocketMQ PushConsumer Implementation
 * 
 * Supports the following features:
 * - Message listener
 * - Automatic ACK
 * - Concurrent consumption
 * 
 * Usage example:
 * $consumer = PushConsumer::getInstance($endpoints, $consumerGroup, $topic);
 * $consumer->setMessageListener(function($message) {
 *     // Process message
 *     return true; // Return true to indicate successful consumption
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
     * @var int Maximum message count
     */
    private $maxMessageNum = 16;
    
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
            // Get connection from pool
            $pool = ConnectionPool::getInstance();
            // Set connection pool configuration from client configuration
            $pool->setConfigFromClientConfiguration($this->config);
            $this->client = $pool->getConnection($this->config);
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
     * Start consumer
     * 
     * @return void
     * @throws \Exception If message listener is not set
     */
    public function start()
    {
        if ($this->messageListener === null) {
            throw new \Exception("Message listener must be set");
        }
        
        $this->running = true;
        
        echo "PushConsumer started, waiting for messages...\n";
        
        // Create SimpleConsumer to pull messages
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
                        error_log("Failed to process message: " . $e->getMessage());
                        // Do not ACK, let message be redelivered
                    }
                }
            } catch (\Exception $e) {
                error_log("Failed to receive message: " . $e->getMessage());
                sleep(1); // Avoid frequent retries
            }
        }
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
                error_log("Failed to return connection to pool: " . $e->getMessage());
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