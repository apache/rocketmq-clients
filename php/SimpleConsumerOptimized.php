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

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/Logger.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\ChangeInvisibleDurationRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;
use Google\Protobuf\Duration;

/**
 * SimpleConsumer - Simple Consumer (optimized)
 *
 * Core features:
 * 1. Singleton TelemetrySession management
 * 2. Topic-level SubscriptionLoadBalancer (round-robin MessageQueue assignment)
 * 3. Async receive support
 * 4. ACK and invisible duration modification
 * 5. Complete state management
 */
class SimpleConsumerOptimized
{
    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $telemetrySession;
    private $subscriptionExpressions = [];
    private $subscriptionRouteDataCache = [];
    private $topicIndex = 0;
    private $awaitDuration = 30; // seconds
    private $isRunning = false;
    private $logger;
    private $namespace = '';
    
    /**
     * Constructor
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $consumerGroup, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->consumerGroup = $consumerGroup;
        $this->clientId = $options['clientId'] ?? ('php-consumer-' . getmypid() . '-' . time());
        $this->awaitDuration = $options['awaitDuration'] ?? 30;
        $this->subscriptionExpressions = $options['subscriptionExpressions'] ?? [];

        // Create gRPC client
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // Initialize Telemetry Session (singleton, with Settings sync confirmation)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId);
        $this->logger = Logger::getInstance('SimpleConsumer');
    }
    
    /**
     * Subscribe to a Topic
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->checkRunning();
        
        // Get route data
        $this->getRouteData($topic);
        
        // Save subscription expression (new subscription overwrites the old one)
        $this->subscriptionExpressions[$topic] = $expression;
        
        return $this;
    }
    
    /**
     * Unsubscribe from a Topic
     *
     * @param string $topic Topic name
     * @return $this
     */
    public function unsubscribe($topic)
    {
        $this->checkRunning();
        
        unset($this->subscriptionExpressions[$topic]);
        unset($this->subscriptionRouteDataCache[$topic]);
        
        return $this;
    }
    
    /**
     * Get all subscription expressions
     *
     * @return array
     */
    public function getSubscriptionExpressions()
    {
        return $this->subscriptionExpressions;
    }
    
    /**
     * Start the Consumer
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }

        if (empty($this->subscriptionExpressions)) {
            throw new \RuntimeException("SimpleConsumerOptimized has no subscriptions");
        }

        try {
            $this->logger->info("Begin to start the rocketmq simple consumer, clientId={$this->clientId}");

            // Establish Telemetry Session
            $this->establishTelemetrySession();

            $this->isRunning = true;

            $this->logger->info("The rocketmq simple consumer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            $this->logger->error("Failed to start: " . $e->getMessage());
            throw $e;
        }
    }
    
    /**
     * Synchronously receive messages
     *
     * @param int $maxMessageNum Maximum number of messages
     * @param int $invisibleDuration Invisible duration in seconds
     * @return array List of messages
     */
    public function receive($maxMessageNum, $invisibleDuration = 30)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        if ($maxMessageNum <= 0) {
            throw new \InvalidArgumentException("maxMessageNum must be greater than 0");
        }
        
        // Copy subscription expressions
        $topics = array_keys($this->subscriptionExpressions);
        
        if (empty($topics)) {
            throw new \RuntimeException("There is no topic to receive message");
        }
        
        // Round-robin topic selection
        $topicIndex = $this->topicIndex++;
        $topic = $topics[$topicIndex % count($topics)];
        $expression = $this->subscriptionExpressions[$topic];
        
        $this->logger->info("Receiving messages from topic: {$topic}");
        
        // Get SubscriptionLoadBalancer
        $loadBalancer = $this->getSubscriptionLoadBalancer($topic);
        
        // Get a MessageQueue from the load balancer
        $messageQueue = $loadBalancer->takeMessageQueue();
        
        if (!$messageQueue) {
            $this->logger->warning("No message queue available for topic: {$topic}");
            return [];
        }
        
        // Build receive request
        $request = $this->wrapReceiveMessageRequest(
            $maxMessageNum,
            $messageQueue,
            $expression,
            $invisibleDuration,
            $this->awaitDuration
        );
        
        // Send request (using Broker Endpoints from MessageQueue)
        // Reference Node.js: receiveMessage(request, mq, awaitDuration)
        return $this->receiveMessage($request, $messageQueue, $this->awaitDuration);
    }
    
    /**
     * Asynchronously receive messages (TODO: requires Swoole coroutine support)
     *
     * @param int $maxMessageNum Maximum number of messages
     * @param int $invisibleDuration Invisible duration in seconds
     * @return \Generator Coroutine generator
     */
    public function receiveAsync($maxMessageNum, $invisibleDuration = 30)
    {
        // TODO: Use Swoole Coroutine for true async
        // This returns a generator as a placeholder
        yield $this->receive($maxMessageNum, $invisibleDuration);
    }
    
    /**
     * Synchronously ACK a message
     *
     * @param object $messageView Message object
     */
    public function ack($messageView)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        $receiptHandle = $this->extractReceiptHandle($messageView);
        
        if (!$receiptHandle) {
            throw new \InvalidArgumentException("Invalid message view, receipt handle not found");
        }
        
        $this->ackMessage($receiptHandle, $messageView);
    }
    
    /**
     * Asynchronously ACK a message (TODO: requires Swoole coroutine support)
     *
     * @param object $messageView Message object
     * @return \Generator
     */
    public function ackAsync($messageView)
    {
        yield $this->ack($messageView);
    }
    
    /**
     * Change message visibility duration
     *
     * @param object $messageView Message object
     * @param int $invisibleDuration New invisible duration in seconds
     */
    public function changeInvisibleDuration($messageView, $invisibleDuration)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        $receiptHandle = $this->extractReceiptHandle($messageView);
        
        if (!$receiptHandle) {
            throw new \InvalidArgumentException("Invalid message view, receipt handle not found");
        }
        
        $this->changeInvisibleDuration0($receiptHandle, $messageView, $invisibleDuration);
    }
    
    /**
     * Asynchronously change visibility duration (TODO: requires Swoole coroutine support)
     *
     * @param object $messageView Message object
     * @param int $invisibleDuration New invisible duration in seconds
     * @return \Generator
     */
    public function changeInvisibleDurationAsync($messageView, $invisibleDuration)
    {
        yield $this->changeInvisibleDuration($messageView, $invisibleDuration);
    }
    
    /**
     * Shutdown the Consumer
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }
        
        $this->logger->info("Begin to shutdown the rocketmq simple consumer, clientId={$this->clientId}");
        
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isRunning = false;
        
        $this->logger->info("Shutdown the rocketmq simple consumer successfully, clientId={$this->clientId}");
    }
    
    /**
     * Get Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * Get consumer group
     */
    public function getConsumerGroup()
    {
        return $this->consumerGroup;
    }
    
    /**
     * Check if the consumer is running
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
     * Check running state
     */
    private function checkRunning()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
    }
    
    /**
     * Establish Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // Create UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // Create SubscriptionEntry list
        $subscriptionEntries = [];
        foreach ($this->subscriptionExpressions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);
            
            $topicResource = new Resource();
            $topicResource->setName($topic);
            
            $subscriptionEntry = new SubscriptionEntry();
            $subscriptionEntry->setTopic($topicResource);
            $subscriptionEntry->setExpression($filterExpression);
            
            $subscriptionEntries[] = $subscriptionEntry;
        }
        
        // Create Subscription configuration
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);
        
        // Create Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);
        
        // Create TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Send and wait for Settings confirmation (blocking, up to 5 seconds)
        $success = $this->telemetrySession->establishAndSyncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish and sync Telemetry Session");
        }
    }
    
    /**
     * Get Topic route data
     */
    private function getRouteData($topic)
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
     * Get SubscriptionLoadBalancer
     */
    private function getSubscriptionLoadBalancer($topic)
    {
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            // Query route and create load balancer
            $routeData = $this->getRouteData($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($routeData);
        }
        
        return $this->subscriptionRouteDataCache[$topic];
    }
    
    /**
     * Build ReceiveMessageRequest
     */
    private function wrapReceiveMessageRequest($maxMessageNum, $messageQueue, $expression, $invisibleDuration, $awaitDuration)
    {
        // Create FilterExpression
        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);
        $filterExpression->setType(0); // TAG type

        // Create Group Resource
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        // Create Request
        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessageNum);
        $request->setAutoRenew(false); // SimpleConsumer does not use auto-renewal

        // Set AttemptId
        $attemptId = 'php-' . uniqid('', true);
        $request->setAttemptId($attemptId);

        // Set InvisibleDuration
        $invisibleDurationObj = $this->createDurationFromSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);

        // Set LongPollingTimeout
        $awaitDurationObj = $this->createDurationFromSeconds($awaitDuration);
        $request->setLongPollingTimeout($awaitDurationObj);
        
        $this->logger->debug("ReceiveMessageRequest: batchSize={$maxMessageNum}, invisibleDuration={$invisibleDuration}s");

        return $request;
    }
    
    /**
     * Create a Duration object from seconds
     *
     * @param int|float $seconds Seconds (can be decimal)
     * @return Duration
     */
    private function createDurationFromSeconds($seconds)
    {
        $duration = new Duration();
        
        // Separate seconds and nanoseconds
        $secs = intval($seconds);
        $nanos = intval(($seconds - $secs) * 1000000000);
        
        $duration->setSeconds($secs);
        $duration->setNanos($nanos);
        
        return $duration;
    }
    
    /**
     * Receive messages (reference Node.js receiveMessage implementation)
     */
    private function receiveMessage($request, $messageQueue, $awaitDuration = 30)
    {
        // Get Broker Endpoints from MessageQueue
        $broker = $messageQueue->getBroker();
        if (!$broker) {
            $this->logger->warning("Broker not available in message queue");
            return [];
        }
        
        if (!$broker->hasEndpoints()) {
            $this->logger->warning("Broker has no endpoints");
            $brokerAddress = $this->endpoints;
            $this->logger->debug("Using default endpoints: {$brokerAddress}");
        } else {
            $endpoints = $broker->getEndpoints();
            $addresses = $endpoints->getAddresses();

            $addressCount = count($addresses);
            $this->logger->debug("Broker endpoints addresses count: {$addressCount}");

            if ($addressCount === 0) {
                $this->logger->debug("No addresses found in broker endpoints, using default");
                $brokerAddress = $this->endpoints;
            } else {
                // Build broker address string
                $address = $addresses[0];
                if ($address === null) {
                    $this->logger->debug("First address is null, using default endpoints");
                    $brokerAddress = $this->endpoints;
                } else {
                    $brokerAddress = $address->getHost() . ':' . $address->getPort();
                }
            }
        }
        
        // Calculate total timeout (reference Node.js: timeout = requestTimeout + awaitDuration)
        // requestTimeout defaults to 3000ms, awaitDuration defaults to 30000ms
        $requestTimeoutMs = 3000;  // 3 seconds
        $awaitDurationMs = $awaitDuration * 1000;  // Convert to milliseconds
        $totalTimeoutMs = $requestTimeoutMs + $awaitDurationMs;
        $totalTimeoutSecs = $totalTimeoutMs / 1000.0;
        
        $this->logger->info("Sending ReceiveMessage request to broker: {$brokerAddress}");
        $this->logger->debug("  Topic: " . $messageQueue->getTopic()->getName());
        $this->logger->debug("  Batch Size: " . $request->getBatchSize());
        $this->logger->debug("  Request Timeout: {$requestTimeoutMs}ms");
        $this->logger->debug("  Await Duration: {$awaitDurationMs}ms");
        $this->logger->debug("  Total Timeout: {$totalTimeoutMs}ms ({$totalTimeoutSecs}s)");
        $this->logger->debug("  Long Polling Timeout: " . $request->getLongPollingTimeout()->getSeconds() . "s");
        
        // [KEY FIX] Use existing $this->client instead of creating a new client
        // This ensures Telemetry Stream and Business RPC share the same gRPC Channel
        // So the Broker can associate Settings and timeout state via Client ID
        $this->logger->debug("Using shared client (same as Telemetry Session)");
        
        $metadata = $this->buildMetadata();
        
        $messages = [];
        $statuses = [];
        $responseCount = 0;
        
        try {
            // Use $this->client instead of creating a new brokerClient
            // [KEY FIX] Set gRPC call timeout (deadline) so the server-side ContextInitPipeline can get remainingMs
            // Server code: ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS)
            // Without deadline, timeRemaining is null, causing NPE
            $callOptions = ['timeout' => $totalTimeoutMs * 1000]; // PHP gRPC timeout is in microseconds
            $call = $this->client->ReceiveMessage($request, $metadata, $callOptions);
            
            foreach ($call->responses() as $response) {
                $responseCount++;
                
                // Handle STATUS type response
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $statusCode = $status->getCode();
                    $statusMessage = $status->getMessage();
                    
                    $this->logger->info("Response #{$responseCount}: STATUS - Code: {$statusCode}, Message: {$statusMessage}");
                    $statuses[] = $status;

                    // If status is not OK, log but don't fail immediately (may just mean no messages)
                    if ($statusCode !== 20000 && $statusCode !== 40404) { // 20000=OK, 40404=NOT_FOUND
                        $this->logger->warning("Non-OK status received: {$statusCode} - {$statusMessage}");
                    }
                }
                
                // Handle MESSAGE type response
                if ($response->hasMessage()) {
                    $message = $response->getMessage();
                    $this->logger->info("Response #{$responseCount}: MESSAGE received");

                    if ($message->hasSystemProperties()) {
                        $sysProps = $message->getSystemProperties();
                        if ($sysProps->getMessageId() !== null && $sysProps->getMessageId() !== '') {
                            $this->logger->debug("  Message ID: " . $sysProps->getMessageId());
                        }
                    }

                    $messages[] = $message;
                }

                // Handle DELIVERY_TIMESTAMP type response
                if ($response->hasDeliveryTimestamp()) {
                    $timestamp = $response->getDeliveryTimestamp();
                    $this->logger->debug("Response #{$responseCount}: DELIVERY_TIMESTAMP - " . $timestamp->getSeconds());
                }
            }

            $this->logger->debug("Total responses: {$responseCount}, Messages received: " . count($messages));

        } catch (\Exception $e) {
            $this->logger->error("Error receiving messages: " . $e->getMessage());
            
            // Ignore timeout errors (normal behavior for long polling)
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }
        
        return $messages;
    }
    
    /**
     * ACK message
     */
    private function ackMessage($receiptHandle, $messageView)
    {
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $topic = $this->extractTopic($messageView);
        $topicResource = new Resource();
        $topicResource->setName($topic);

        // Build AckMessageEntry
        $entry = new \Apache\Rocketmq\V2\AckMessageEntry();
        $messageId = $this->extractMessageId($messageView);
        if ($messageId) {
            $entry->setMessageId($messageId);
        }
        $entry->setReceiptHandle($receiptHandle);

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries([$entry]);

        $metadata = $this->buildMetadata();

        list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Ack message failed: " . $status->details);
        }

        // Check response entries for individual ack results
        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("Ack message failed with code: " . $statusCode);
            }
        }
        // Also check individual entry results
        $entries = $response->getEntries();
        if (!empty($entries)) {
            $resultEntry = $entries[0];
            if ($resultEntry->hasStatus()) {
                $entryCode = $resultEntry->getStatus()->getCode();
                if ($entryCode !== 20000) {
                    throw new \RuntimeException("Ack entry failed with code: " . $entryCode);
                }
            }
        }
    }
    
    /**
     * Change visibility duration
     */
    private function changeInvisibleDuration0($receiptHandle, $messageView, $invisibleDuration)
    {
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        $topic = $this->extractTopic($messageView);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $duration = new Duration();
        $duration->setSeconds($invisibleDuration);
        
        $request = new ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);

        $messageId = $this->extractMessageId($messageView);
        if ($messageId) {
            $request->setMessageId($messageId);
        }
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->ChangeInvisibleDuration($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Change invisible duration failed: " . $status->details);
        }
    }
    
    /**
     * Extract Receipt Handle
     */
    private function extractReceiptHandle($messageView)
    {
        // Extract receipt_handle from message system properties
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
        }
        return null;
    }

    /**
     * Extract Message ID
     */
    private function extractMessageId($messageView)
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getMessageId')) {
                return $sysProps->getMessageId();
            }
        }
        return null;
    }
    
    /**
     * Extract Topic
     */
    private function extractTopic($messageView)
    {
        if (method_exists($messageView, 'getTopic')) {
            $topic = $messageView->getTopic();
            if (method_exists($topic, 'getName')) {
                return $topic->getName();
            }
        }
        return null;
    }
    
    /**
     * Build metadata
     */
    private function buildMetadata()
    {
        // Required fields according to Java implementation
        $dateTime = gmdate('Ymd\THis\Z'); // Format: yyyyMMdd'T'HHmmss'Z'
        $requestId = sprintf('%08x-%04x-%04x-%04x-%012x',
            mt_rand(0, 0xffffffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) & 0x0fff | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000,
            mt_rand(0, 0xffffffffffff)
        );
        
        return [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
            'x-mq-protocol' => ['v2'],
            'x-mq-date-time' => [$dateTime],
            'x-mq-request-id' => [$requestId],
            'x-mq-namespace' => [$this->namespace ?? ''],
        ];
    }
}

/**
 * SubscriptionLoadBalancer - Subscription load balancer
 */
class SubscriptionLoadBalancer
{
    private $messageQueues = [];
    private $queueIndex = 0;
    
    public function __construct($routeData)
    {
        if ($routeData && method_exists($routeData, 'getMessageQueues')) {
            $allQueues = $routeData->getMessageQueues();
            $readableCount = 0;

            // Permission: 1=READ_ONLY, 2=WRITE_ONLY, 4=NONE, 6=READ_WRITE
            foreach ($allQueues as $queue) {
                $permission = $queue->getPermission();
                // Accept READ_ONLY(1), READ_WRITE(6), or NONE(4) for compatibility
                if ($permission == 1 || $permission == 4 || $permission == 6) {
                    $this->messageQueues[] = $queue;
                    $readableCount++;
                }
            }

            Logger::getInstance('SubscriptionLoadBalancer')->info("Topic queues: {$readableCount} readable / " . count($allQueues) . " total");
        }
    }
    
    /**
     * Get next MessageQueue (round-robin)
     */
    public function takeMessageQueue()
    {
        if (empty($this->messageQueues)) {
            Logger::getInstance('SubscriptionLoadBalancer')->warning("No message queues available");
            return null;
        }
        
        $index = $this->queueIndex++ % count($this->messageQueues);
        $queue = $this->messageQueues[$index];
        
        Logger::getInstance('SubscriptionLoadBalancer')->debug("Selected queue index: {$index}");
        
        return $queue;
    }
    
    /**
     * Get all MessageQueues
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}
