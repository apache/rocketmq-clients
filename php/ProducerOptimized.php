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

require_once __DIR__ . '/autoload.php';
require_once __DIR__ . '/MessageId.php';
require_once __DIR__ . '/MessageIdImpl.php';
require_once __DIR__ . '/MessageIdCodec.php';
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Signature.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\EndTransactionRequest;
use Apache\Rocketmq\V2\RecallMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;
use Google\Protobuf\Duration;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

/**
 * Producer - Message producer
 *
 * Core features:
 * 1. Singleton TelemetrySession management
 * 2. PublishingLoadBalancer (Topic-level MessageQueue load balancing)
 * 3. Complete state management (FSM)
 * 4. Transaction message support
 * 5. Delayed message recall
 * 6. Interceptor support (Hook Points)
 */
class ProducerOptimized
{
    private $client;
    private $endpoints;
    private $clientId;
    private $telemetrySession;
    private $publishingRouteDataCache = [];
    private $isRunning = false;
    private $maxAttempts = 3;
    private $requestTimeout = 3000; // ms
    private $topics = [];
    private $isolatedEndpoints = [];
    private $namespace = '';
    private $logger;
    private $credentials = null; // SessionCredentials for AK/SK auth

    /**
     * Constructor
     *
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        $this->maxAttempts = $options['maxAttempts'] ?? 3;
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->topics = $options['topics'] ?? [];
        $this->namespace = $options['namespace'] ?? '';

        // Set AK/SK credentials if provided
        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        $this->logger = Logger::getInstance('Producer');

        // Create gRPC client
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials);
    }
    
    /**
     * Start the Producer
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }
        
        try {
            Logger::getInstance('Producer')->info("Begin to start the rocketmq producer, clientId={$this->clientId}");

            // Establish Telemetry Session
            $this->establishTelemetrySession();

            // Warm up route cache
            foreach ($this->topics as $topic) {
                $this->getPublishingLoadBalancer($topic);
            }

            $this->isRunning = true;

            Logger::getInstance('Producer')->info("The rocketmq producer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            Logger::getInstance('Producer')->error("Failed to start: " . $e->getMessage());
            $this->shutdown();
            throw $e;
        }
    }
    
    /**
     * Synchronously send a message
     *
     * @param Message $message Message object
     * @return array Send result ['messageId' => ..., 'transactionId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        // Check Producer status
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        // Validate message
        $this->validateMessage($message);

        // Get Topic
        $topic = $message->getTopic()->getName();

        // Get PublishingLoadBalancer
        $loadBalancer = $this->getPublishingLoadBalancer($topic);

        // Select MessageQueue (round-robin)
        $messageQueue = $loadBalancer->takeMessageQueue($this->isolatedEndpoints, 1);
        
        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }
        
        // Build send request
        $request = $this->wrapSendMessageRequest([$message], $messageQueue[0]);

        // Execute send (with retry)
        return $this->sendMessageWithRetry($request, $message, $this->maxAttempts);
    }
    
    /**
     * Asynchronously send a message (TODO: Requires Swoole coroutine support)
     *
     * @param Message $message Message object
     * @return \Generator Coroutine generator
     */
    public function sendAsync(Message $message)
    {
        // TODO: Use Swoole Coroutine for true async implementation
        yield $this->send($message);
    }
    
    /**
     * Send a transaction message
     *
     * @param Message $message Message object
     * @param Transaction $transaction Transaction object
     * @return array Send result
     */
    public function sendWithTransaction(Message $message, $transaction)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $this->validateMessage($message);

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->getPublishingLoadBalancer($topic);
        $messageQueue = $loadBalancer->takeMessageQueue($this->isolatedEndpoints, 1);

        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }

        $request = $this->wrapTransactionMessageRequest([$message], $messageQueue[0]);
        $result = $this->sendMessageWithRetry($request, $message, $this->maxAttempts);

        // Record the receipt for later commit/rollback
        if (isset($result['transactionId'])) {
            $transaction->tryAddReceipt($message, $result);
        }

        return $result;
    }

    /**
     * Begin a transaction
     *
     * @return Transaction Transaction object
     */
    public function beginTransaction()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        return new Transaction($this);
    }
    
    /**
     * Commit a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     */
    public function commitTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::COMMIT);
    }
    
    /**
     * Rollback a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     */
    public function rollbackTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::ROLLBACK);
    }
    
    /**
     * Send a priority message.
     *
     * @param string $topic Topic name
     * @param string $body Message body
     * @param int $priority Priority value (lower value = higher priority)
     * @param string $tag Optional tag
     * @return array Send result
     */
    public function sendPriorityMessage($topic, $body, $priority, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setPriority($priority);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    /**
     * Send a delayed/scheduled message with recall support.
     *
     * @param string $topic Topic name
     * @param string $body Message body
     * @param int $deliveryTimestampUnixSec Delivery timestamp (unix seconds)
     * @param string $tag Optional tag
     * @return array Send result with recallHandle
     */
    public function sendDelayedMessage($topic, $body, $deliveryTimestampUnixSec, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $ts = new Timestamp();
        $ts->setSeconds($deliveryTimestampUnixSec);
        $ts->setNanos(0);

        $sysProps = new SystemProperties();
        $sysProps->setDeliveryTimestamp($ts);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    /**
     * Send a FIFO message.
     *
     * @param string $topic Topic name
     * @param string $body Message body
     * @param string $messageGroup Message group for FIFO ordering
     * @param string $tag Optional tag
     * @return array Send result
     */
    public function sendFifoMessage($topic, $body, $messageGroup, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setMessageGroup($messageGroup);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    /**
     * Recall a delayed message
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle
     * @return array Recall result
     */
    public function recallMessage($topic, $recallHandle)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new RecallMessageRequest();
        $request->setTopic($topicResource);
        $request->setRecallHandle($recallHandle);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->RecallMessage($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Recall message failed: " . $status->details);
        }

        $messageId = '';
        if (method_exists($response, 'getMessageId')) {
            $messageId = $response->getMessageId();
        }

        return [
            'messageId' => $messageId,
            'status' => $response->getStatus(),
        ];
    }
    
    /**
     * Asynchronously recall a message (TODO: Requires Swoole coroutine support)
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle
     * @return \Generator
     */
    public function recallMessageAsync($topic, $recallHandle)
    {
        yield $this->recallMessage($topic, $recallHandle);
    }
    
    /**
     * Shutdown the Producer
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }
        
        $this->logger->info("Begin to shutdown the rocketmq producer, clientId={$this->clientId}");
        
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isRunning = false;
        
        $this->logger->info("Shutdown the rocketmq producer successfully, clientId={$this->clientId}");
    }
    
    /**
     * Get Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * Check if running
     */
    public function isRunning()
    {
        return $this->isRunning;
    }
    
    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->shutdown();
    }
    
    // ==================== Private Methods ====================
    
    /**
     * Establish Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // Create UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // Create Publishing configuration
        $publishing = new Publishing();
        
        // Add Topics
        $topicResources = [];
        foreach ($this->topics as $topicName) {
            $topicResource = new Resource();
            $topicResource->setName($topicName);
            $topicResources[] = $topicResource;
        }
        $publishing->setTopics($topicResources);
        
        // Create Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);
        $settings->setPublishing($publishing);
        
        // Create TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Synchronously send Settings
        $success = $this->telemetrySession->syncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        
        // Wait for server processing
        usleep(500000); // 500ms
    }
    
    /**
     * Validate message
     */
    private function validateMessage(Message $message)
    {
        // Check Topic
        if (!$message->hasTopic() || empty(trim($message->getTopic()->getName()))) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        
        // Check message body
        if (empty($message->getBody())) {
            throw new \InvalidArgumentException("Message body is required");
        }
        
        // Check message size (default 4MB)
        $maxSize = 4 * 1024 * 1024;
        if (strlen($message->getBody()) > $maxSize) {
            throw new \InvalidArgumentException("Message size exceeds limit (4MB)");
        }
    }
    
    /**
     * Get PublishingLoadBalancer
     */
    private function getPublishingLoadBalancer($topic)
    {
        if (!isset($this->publishingRouteDataCache[$topic])) {
            // Query route and create load balancer
            $routeData = $this->queryRoute($topic);
            $this->publishingRouteDataCache[$topic] = new PublishingLoadBalancer($routeData);
        }
        
        return $this->publishingRouteDataCache[$topic];
    }
    
    /**
     * Query route
     */
    private function queryRoute($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }
        
        return $response;
    }
    
    /**
     * Convert a Message to enriched protobuf Message
     * Convert message to protocol format
     *
     * @param Message $msg Input message
     * @param mixed $messageQueue Target message queue
     * @param bool $txEnabled Whether this is a transaction message
     */
    private function toProtobufMessage(Message $msg, $messageQueue, $txEnabled = false)
    {
        // Generate message ID
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        // Build SystemProperties
        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');
        $systemProperties->setBodyEncoding(Encoding::IDENTITY);
        $systemProperties->setQueueId($messageQueue->getId());
        $systemProperties->setMessageType($this->detectMessageType($msg, $txEnabled));

        // Copy optional system properties from input message
        $inputSysProps = $msg->getSystemProperties();
        if ($inputSysProps) {
            if (method_exists($inputSysProps, 'getTag') && $inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (method_exists($inputSysProps, 'getKeys')) {
                $keys = $inputSysProps->getKeys();
                if (!empty($keys)) {
                    $systemProperties->setKeys($keys);
                }
            }
            if (method_exists($inputSysProps, 'getMessageGroup') && $inputSysProps->hasMessageGroup()) {
                $systemProperties->setMessageGroup($inputSysProps->getMessageGroup());
            }
            if (method_exists($inputSysProps, 'getDeliveryTimestamp') && $inputSysProps->hasDeliveryTimestamp()) {
                $systemProperties->setDeliveryTimestamp($inputSysProps->getDeliveryTimestamp());
            }
            if (method_exists($inputSysProps, 'getLiteTopic') && $inputSysProps->hasLiteTopic()) {
                $systemProperties->setLiteTopic($inputSysProps->getLiteTopic());
            }
            if (method_exists($inputSysProps, 'getPriority') && $inputSysProps->hasPriority()) {
                $systemProperties->setPriority($inputSysProps->getPriority());
            }
            if (method_exists($inputSysProps, 'getTraceContext') && $inputSysProps->hasTraceContext()) {
                $systemProperties->setTraceContext($inputSysProps->getTraceContext());
            }
        }

        // Build topic Resource with namespace
        $topicResource = new Resource();
        $topicResource->setName($msg->getTopic()->getName());
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        // Build protobuf Message
        $protoMsg = new Message();
        $protoMsg->setTopic($topicResource);
        $protoMsg->setBody($msg->getBody());
        $protoMsg->setSystemProperties($systemProperties);

        // Copy user properties
        $userProps = $msg->getUserProperties();
        if (!empty($userProps)) {
            foreach ($userProps as $key => $value) {
                $protoMsg->getUserProperties()[$key] = $value;
            }
        }

        return $protoMsg;
    }

    /**
     * Detect message type based on message properties
     * Create publishing message
     *
     * @param Message $msg Input message
     * @param bool $txEnabled Whether this is a transaction message
     */
    private function detectMessageType(Message $msg, $txEnabled = false)
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();

        // Normal message (no special properties, not transaction)
        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::NORMAL;
        }
        // FIFO message
        if ($hasMessageGroup && !$txEnabled) {
            return V2MessageType::FIFO;
        }
        // Lite message
        if ($hasLiteTopic && !$txEnabled) {
            return V2MessageType::LITE;
        }
        // Delay message
        if ($hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::DELAY;
        }
        // Priority message
        if ($hasPriority && !$txEnabled) {
            return V2MessageType::PRIORITY;
        }
        // Transaction message (txEnabled and no conflicting properties)
        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && $txEnabled) {
            return V2MessageType::TRANSACTION;
        }

        return V2MessageType::NORMAL;
    }

    /**
     * Create a Protobuf Timestamp with current time
     */
    private function createTimestamp()
    {
        $now = microtime(true);
        $seconds = (int)$now;
        $nanos = (int)(($now - $seconds) * 1000000000);

        $timestamp = new Timestamp();
        $timestamp->setSeconds($seconds);
        $timestamp->setNanos($nanos);
        return $timestamp;
    }

    /**
     * Build SendMessageRequest
     */
    private function wrapSendMessageRequest($messages, $messageQueue)
    {
        $enrichedMessages = [];
        foreach ($messages as $msg) {
            $enrichedMessages[] = $this->toProtobufMessage($msg, $messageQueue);
        }

        $request = new SendMessageRequest();
        $request->setMessages($enrichedMessages);

        return $request;
    }

    /**
     * Build SendMessageRequest for transaction messages
     */
    private function wrapTransactionMessageRequest($messages, $messageQueue)
    {
        $enrichedMessages = [];
        foreach ($messages as $msg) {
            $enrichedMessages[] = $this->toProtobufMessage($msg, $messageQueue, true);
        }

        $request = new SendMessageRequest();
        $request->setMessages($enrichedMessages);

        return $request;
    }
    
    /**
     * Send message (with retry)
     */
    private function sendMessageWithRetry($request, $message, $maxAttempts)
    {
        $lastException = null;
        
        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $metadata = $this->buildMetadata();
                
                list($response, $status) = $this->client->SendMessage($request, $metadata)->wait();
                
                if ($status->code !== 0) {
                    throw new \RuntimeException("Send message failed: " . $status->details);
                }
                
                // Parse response
                $entries = $response->getEntries();
                $entryCount = count($entries);

                // Check overall response status first (e.g., topic type validation)
                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException("SendMessage failed with code: " . $respStatus->getCode() . ", message: " . $respStatus->getMessage());
                    }
                }

                $this->logger->debug("SendMessage response: {$entryCount} entries");

                if ($entryCount > 0) {
                    $entry = $entries[0];
                    $resultStatus = $entry->getStatus();
                    
                    // Check status code
                    if ($resultStatus->getCode() !== 20000) {
                        throw new \RuntimeException("Send message failed with code: " . $resultStatus->getCode());
                    }
                    
                    return [
                        'messageId' => $entry->getMessageId(),
                        'transactionId' => $entry->getTransactionId(),
                        'recallHandle' => $entry->getRecallHandle() ?? '',
                        'code' => $resultStatus->getCode(),
                        'message' => $resultStatus->getMessage(),
                    ];
                }
                
                throw new \RuntimeException("No response entries");
                
            } catch (\Exception $e) {
                $lastException = $e;
                $this->logger->error("Send attempt {$attempt} failed: " . $e->getMessage());
                
                // If not the last attempt, wait and retry
                if ($attempt < $maxAttempts) {
                    usleep(pow(2, $attempt) * 100000); // Exponential backoff
                }
            }
        }
        
        throw $lastException;
    }
    
    /**
     * End transaction
     */
    private function endTransaction($messageId, $transactionId, $topic, $resolution)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new EndTransactionRequest();
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        $request->setTopic($topicResource);
        $request->setResolution($resolution);
        $request->setSource(TransactionSource::SOURCE_CLIENT);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->EndTransaction($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("End transaction failed: " . $status->details);
        }
        
        // Check response status
        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("End transaction failed with code: " . $statusCode);
            }
        }
    }
    
    /**
     * Build metadata using Signature class for gRPC calls.
     * Mirrors Java's client.sign() pattern.
     */
    private function buildMetadata()
    {
        return Signature::sign(
            $this->credentials,
            $this->clientId,
            'PHP',
            '5.0.0',
            $this->namespace,
            'v2'
        );
    }
}

/**
 * PublishingLoadBalancer - Publishing load balancer
 */
class PublishingLoadBalancer
{
    private $index;
    private $messageQueues = [];
    
    public function __construct($routeData)
    {
        // Initialize random index
        $this->index = rand(0, PHP_INT_MAX);

        // Filter writable MessageQueues
        if ($routeData && method_exists($routeData, 'getMessageQueues')) {
            $allQueues = $routeData->getMessageQueues();
            $writableCount = 0;

            foreach ($allQueues as $queue) {
                // Permission: 1=READ_ONLY, 2=WRITE_ONLY, 4=NONE, 6=READ_WRITE
                $permission = $queue->getPermission();
                // Accept WRITE_ONLY(2), READ_WRITE(6), or NONE(4) for compatibility
                if ($permission == 2 || $permission == 4 || $permission == 6) {
                    $this->messageQueues[] = $queue;
                    $writableCount++;
                }
            }

            Logger::getInstance('PublishingLoadBalancer')->info("Topic queues: {$writableCount} writable / " . count($allQueues) . " total");
        }

        if (empty($this->messageQueues)) {
            throw new \InvalidArgumentException("No writable message queue found");
        }
    }
    
    /**
     * Select MessageQueue by message group (FIFO messages)
     *
     * @param string $messageGroup Message group
     * @return object MessageQueue
     */
    public function takeMessageQueueByMessageGroup($messageGroup)
    {
        if (empty($this->messageQueues)) {
            throw new \RuntimeException("No message queues available");
        }
        
        // Use SipHash to calculate hash (simplified version, using crc32)
        $hashCode = crc32($messageGroup);
        $index = abs($hashCode) % count($this->messageQueues);
        
        return $this->messageQueues[$index];
    }
    
    /**
     * Select MessageQueue list (round-robin + exclude isolated Endpoints)
     *
     * @param array $excluded Endpoints to exclude
     * @param int $count Number needed
     * @return array MessageQueue list
     */
    public function takeMessageQueue($excluded = [], $count = 1)
    {
        if (empty($this->messageQueues)) {
            return [];
        }
        
        $candidates = [];
        $candidateBrokerNames = [];
        
        $next = $this->index++;
        
        // Round one: exclude isolated Endpoints
        for ($i = 0; $i < count($this->messageQueues); $i++) {
            $queueIndex = $next++ % count($this->messageQueues);
            $messageQueue = $this->messageQueues[$queueIndex];
            
            $broker = $messageQueue->getBroker();
            $brokerName = $broker->getName();
            
            // Check if excluded
            $isExcluded = false;
            foreach ($excluded as $endpoint) {
                // TODO: Compare Endpoints
                if ($brokerName === $endpoint) {
                    $isExcluded = true;
                    break;
                }
            }
            
            if (!$isExcluded && !in_array($brokerName, $candidateBrokerNames)) {
                $candidateBrokerNames[] = $brokerName;
                $candidates[] = $messageQueue;
            }
            
            if (count($candidates) >= $count) {
                return $candidates;
            }
        }
        
        // Round two: if all Endpoints are isolated, use all queues
        if (empty($candidates)) {
            for ($i = 0; $i < count($this->messageQueues); $i++) {
                $queueIndex = $next++ % count($this->messageQueues);
                $messageQueue = $this->messageQueues[$queueIndex];
                
                $broker = $messageQueue->getBroker();
                $brokerName = $broker->getName();
                
                if (!in_array($brokerName, $candidateBrokerNames)) {
                    $candidateBrokerNames[] = $brokerName;
                    $candidates[] = $messageQueue;
                }
                
                if (count($candidates) >= $count) {
                    break;
                }
            }
        }
        
        return $candidates;
    }
    
    /**
     * Get all MessageQueues
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}

/**
 * Transaction - Transaction state management for transactional messages.
 *
 * Tracks sent messages and their receipts, enabling commit or rollback.
 */
class Transaction
{
    private static $MAX_MESSAGE_NUM = 1;

    private $producer;
    private $messages = [];
    private $receipts = [];

    public function __construct($producer)
    {
        $this->producer = $producer;
    }

    /**
     * Add a message to this transaction.
     * Mirrors Java: tryAddMessage with single-message limit.
     */
    public function tryAddMessage(Message $message)
    {
        if (count($this->messages) >= self::$MAX_MESSAGE_NUM) {
            throw new \InvalidArgumentException(
                "Message in transaction has exceeded the threshold: " . self::$MAX_MESSAGE_NUM
            );
        }
        $this->messages[] = $message;
    }

    /**
     * Record a send receipt for a message in this transaction (alias for compatibility).
     */
    public function addReceipt(Message $message, array $sendResult)
    {
        $this->tryAddReceipt($message, $sendResult);
    }

    /**
     * Record a send receipt for a message in this transaction.
     * Mirrors Java: tryAddReceipt with containment check.
     */
    public function tryAddReceipt(Message $message, array $sendResult)
    {
        if (!$this->containsMessage($message)) {
            throw new \InvalidArgumentException("Message not in transaction");
        }
        $this->receipts[] = [
            'message' => $message,
            'sendResult' => $sendResult,
        ];
    }

    /**
     * Check if a message has been added to this transaction.
     */
    private function containsMessage(Message $message)
    {
        foreach ($this->messages as $existing) {
            if ($existing === $message) {
                return true;
            }
        }
        return false;
    }

    /**
     * Commit all messages in this transaction.
     * Mirrors Java: commit with empty-receipts check.
     */
    public function commit()
    {
        if (empty($this->receipts)) {
            throw new \RuntimeException("Transactional message has not been sent yet");
        }
        foreach ($this->receipts as $receipt) {
            $sr = $receipt['sendResult'];
            $msg = $receipt['message'];
            $topic = $msg->getTopic()->getName();

            $this->producer->commitTransaction(
                $sr['messageId'],
                $sr['transactionId'] ?? '',
                $topic
            );
        }
        $this->receipts = [];
        $this->messages = [];
    }

    /**
     * Rollback all messages in this transaction.
     * Mirrors Java: rollback with empty-receipts check.
     */
    public function rollback()
    {
        if (empty($this->receipts)) {
            throw new \RuntimeException("Transactional message has not been sent yet");
        }
        foreach ($this->receipts as $receipt) {
            $sr = $receipt['sendResult'];
            $msg = $receipt['message'];
            $topic = $msg->getTopic()->getName();

            $this->producer->rollbackTransaction(
                $sr['messageId'],
                $sr['transactionId'] ?? '',
                $topic
            );
        }
        $this->receipts = [];
        $this->messages = [];
    }
}
