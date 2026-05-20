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
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/ClientTrait.php';
require_once __DIR__ . '/SwooleCompat.php';
require_once __DIR__ . '/ProtobufUtil.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\QueryAssignmentRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Grpc\ChannelCredentials;
use Google\Protobuf\Duration;

/**
 * SimpleConsumer
 *
 * Integrates TelemetrySession to implement:
 * 1. Singleton Stream management
 * 2. Serial write
 * 3. Backpressure handling
 */
class SimpleConsumer
{
    use ClientTrait;

    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $telemetrySession;
    private $subscriptions = [];
    private $isStarted = false;
    private $logger;
    private $credentials = null;
    private $namespace = '';
    private $requestTimeout = 3000; // ms
    private $interceptors = [];
    private $parsedEndpoints = null;
    private $awaitDuration = 30; // seconds
    private $lastHeartbeatTime = 0;
    private $brokerClients = [];

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
        $this->namespace = $options['namespace'] ?? '';
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->awaitDuration = $options['awaitDuration'] ?? 30;

        // Set AK/SK credentials if provided
        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        // Create gRPC client via connection pool
        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials, $this->namespace);
        $this->logger = Logger::getInstance('SimpleConsumer');
    }

    /**
     * Subscribe to a topic
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->subscriptions[$topic] = $expression;
        return $this;
    }

    /**
     * Unsubscribe from a topic
     *
     * @param string $topic Topic name
     * @return $this
     */
    public function unsubscribe($topic)
    {
        unset($this->subscriptions[$topic]);
        return $this;
    }

    /**
     * Get all subscription expressions
     *
     * @return array
     */
    public function getSubscriptionExpressions()
    {
        return $this->subscriptions;
    }

    /**
     * Start the consumer
     */
    public function start()
    {
        if ($this->isStarted) {
            return;
        }

        if (empty($this->subscriptions)) {
            throw new \RuntimeException("No subscriptions configured");
        }

        // Establish Telemetry Session
        $this->establishTelemetrySession();

        $this->isStarted = true;

        // Start periodic heartbeat via SIGALRM timer
        $this->startHeartbeat();
    }

    /**
     * Heartbeat tick handler - call from main loop to keep connection alive.
     * Sends heartbeat every 10 seconds to the broker.
     */
    public function onHeartbeatTick()
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            $this->doHeartbeat();
            $this->lastHeartbeatTime = $now;
        }
    }

    /**
     * Send heartbeat to the broker to keep connection alive.
     */
    public function doHeartbeat()
    {
        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::SIMPLE_CONSUMER);

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->Heartbeat($request, $metadata)->wait();
            if ($status->code === 0) {
                $this->logger->debug("Heartbeat sent successfully");
            } else {
                $this->logger->warning("Heartbeat failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("Heartbeat failed: " . $e->getMessage());
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
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

        // Create SubscriptionEntry list
        $subscriptionEntries = [];
        foreach ($this->subscriptions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);
            $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);

            $topicResource = new Resource();
            $topicResource->setName($topic);
            if (!empty($this->namespace)) {
                $topicResource->setResourceNamespace($this->namespace);
            }

            $subscriptionEntry = new SubscriptionEntry();
            $subscriptionEntry->setTopic($topicResource);
            $subscriptionEntry->setExpression($filterExpression);

            $subscriptionEntries[] = $subscriptionEntry;
        }

        // Create Subscription configuration
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }
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

        // Send Settings synchronously
        $success = $this->telemetrySession->syncSettings($command);

        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }

        // Wait for server processing
        usleep(500000); // 500ms
    }

    /**
     * Register a message interceptor.
     *
     * @param MessageInterceptor $interceptor
     * @return $this
     */
    public function addInterceptor(MessageInterceptor $interceptor)
    {
        $this->interceptors[] = $interceptor;
        return $this;
    }

    /**
     * Execute interceptors at a given hook point.
     *
     * @param string $hookPoint One of MessageHookPoints constants
     * @param array $context Context data for the hook point
     */
    public function executeInterceptors($hookPoint, $context = [])
    {
        if (empty($this->interceptors)) {
            return;
        }
        foreach ($this->interceptors as $interceptor) {
            try {
                $interceptor->intercept($hookPoint, $context);
            } catch (\Exception $e) {
                $this->logger->warning("Interceptor failed at {$hookPoint}: " . $e->getMessage());
            }
        }
    }

    /**
     * Receive messages
     *
     * @param int $maxMessages Maximum number of messages
     * @param int $invisibleDuration Invisible duration (seconds)
     * @return array Message list
     */
    public function receive($maxMessages = 10, $invisibleDuration = 30)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        $startTime = microtime(true);
        $messages = [];

        // Iterate over all subscribed topics
        foreach ($this->subscriptions as $topic => $expression) {
            $received = $this->receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration);
            $messages = array_merge($messages, $received);

            // Stop if maximum count is reached
            if (count($messages) >= $maxMessages) {
                break;
            }
        }

        $latencyMs = (microtime(true) - $startTime) * 1000;
        $this->executeInterceptors(MessageHookPoints::RECEIVE, [
            'success' => true,
            'latencyMs' => $latencyMs,
            'messageCount' => count($messages),
        ]);

        return $messages;
    }

    /**
     * Receive messages from a specific topic
     */
    private function receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration)
    {
        // Query route to get MessageQueue first
        $messageQueue = $this->getMessageQueue($topic);

        if (!$messageQueue) {
            return [];
        }

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);
        $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }

        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessages);
        $request->setAutoRenew(false);

        $invisibleDurationObj = new Duration();
        $invisibleDurationObj->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);

        $longPollingTimeout = new Duration();
        $longPollingTimeout->setSeconds($this->awaitDuration);
        $request->setLongPollingTimeout($longPollingTimeout);

        $attemptId = $this->generateAttemptId();
        $request->setAttemptId($attemptId);

        // Route ReceiveMessage to broker endpoints from MessageQueue
        $receiveClient = $this->getBrokerClient($messageQueue);

        // Calculate total timeout: requestTimeout + awaitDuration
        $requestTimeoutMs = $this->requestTimeout;
        $awaitDurationMs = $this->awaitDuration * 1000;
        $totalTimeoutMs = $requestTimeoutMs + $awaitDurationMs;
        $totalTimeoutSecs = $totalTimeoutMs / 1000.0;

        // Use signed metadata via ClientTrait
        $metadata = $this->buildMetadata();

        $callOptions = ['timeout' => $totalTimeoutMs * 1000];

        $this->logger->debug("ReceiveMessage: topic={$topic}, batchSize={$maxMessages}, timeout={$totalTimeoutSecs}s, attemptId={$attemptId}");

        // Receive messages
        $call = $receiveClient->ReceiveMessage($request, $metadata, $callOptions);

        $messages = [];
        try {
            foreach ($call->responses() as $response) {
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $statusCode = $status->getCode();
                    if ($statusCode !== 20000 && $statusCode !== 40404) {
                        $this->logger->warning("Non-OK status from ReceiveMessage: code={$statusCode}, message=" . $status->getMessage());
                    }
                }

                if ($response->hasMessage()) {
                    $messages[] = $response->getMessage();
                }

                // Check if maximum count is reached
                if (count($messages) >= $maxMessages) {
                    break;
                }
            }
        } catch (\Exception $e) {
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }

        return $messages;
    }

    /**
     * Get MessageQueue via QueryAssignment (includes consumer group).
     * Unlike QueryRoute, QueryAssignment tells the server about the consumer
     * group and returns queues assigned to this specific consumer.
     */
    private function getMessageQueue($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }

        $request = new QueryAssignmentRequest();
        $request->setTopic($topicResource);
        $request->setGroup($groupResource);
        $request->setEndpoints($this->getParsedEndpoints());

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->QueryAssignment($request, $metadata)->wait();

            if ($status->code !== 0) {
                $this->logger->warning("QueryAssignment failed for topic={$topic}: " . $status->details);
                return null;
            }

            $assignments = $response->getAssignments();
            if (ProtobufUtil::isRepeatedFieldEmpty($assignments)) {
                $this->logger->debug("No assignments for topic={$topic}");
                return null;
            }

            // Return the MessageQueue from the first assignment
            $firstAssignment = $assignments[0];
            if (method_exists($firstAssignment, 'getMessageQueue')) {
                return $firstAssignment->getMessageQueue();
            }

            return null;
        } catch (\Exception $e) {
            $this->logger->warning("QueryAssignment exception for topic={$topic}: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Acknowledge messages
     *
     * @param array $messages List of message objects to acknowledge
     */
    public function ack($messages)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        if (empty($messages)) {
            return;
        }

        // Group messages by topic for batch ack
        $messagesByTopic = [];
        foreach ($messages as $message) {
            $topic = $this->extractTopic($message);
            if (!$topic) {
                continue;
            }
            if (!isset($messagesByTopic[$topic])) {
                $messagesByTopic[$topic] = [];
            }
            $messagesByTopic[$topic][] = $message;
        }

        foreach ($messagesByTopic as $topic => $topicMessages) {
            $this->ackMessagesForTopic($topic, $topicMessages);
        }
    }

    /**
     * Acknowledge messages for a specific topic
     */
    private function ackMessagesForTopic($topic, $messages)
    {
        $entries = [];
        foreach ($messages as $message) {
            $receiptHandle = $this->extractReceiptHandle($message);
            $messageId = $this->extractMessageId($message);

            if (!$receiptHandle) {
                $this->logger->warning("Skip ack: no receipt handle for message");
                continue;
            }

            $entry = new AckMessageEntry();
            if ($messageId) {
                $entry->setMessageId($messageId);
            }
            $entry->setReceiptHandle($receiptHandle);
            $entries[] = $entry;
        }

        if (empty($entries)) {
            return;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries($entries);

        $metadata = $this->buildMetadata();

        $maxRetries = 3;
        $attempt = 0;

        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("AckMessage attempt {$attempt}: status error: " . $status->details);
                } else {
                    $responseEntries = $response->getEntries();
                    if (!ProtobufUtil::isRepeatedFieldEmpty($responseEntries)) {
                        foreach ($responseEntries as $resultEntry) {
                            if ($resultEntry->hasStatus()) {
                                $entryCode = $resultEntry->getStatus()->getCode();
                                if ($entryCode !== 20000) {
                                    if ($entryCode == 40003) {
                                        $this->logger->warning("AckMessage invalid receipt handle, giving up");
                                    } else {
                                        $this->logger->warning("AckMessage entry failed with code: " . $entryCode);
                                    }
                                }
                            }
                        }
                    }
                }
                return;
            } catch (\Exception $e) {
                $this->logger->warning("AckMessage attempt {$attempt} failed: " . $e->getMessage());
                $attempt++;
                if ($attempt < $maxRetries) {
                    usleep(pow(2, $attempt) * 100000);
                }
            }
        }

        $this->logger->error("AckMessage failed after {$maxRetries} retries for topic={$topic}");
    }

    /**
     * Change message visibility duration
     *
     * @param object $message Message to modify
     * @param int $invisibleDurationSeconds New invisible duration
     * @return bool
     */
    public function changeInvisibleDuration($message, $invisibleDurationSeconds)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        $receiptHandle = $this->extractReceiptHandle($message);
        $messageId = $this->extractMessageId($message);
        $topic = $this->extractTopic($message);

        if (!$receiptHandle) {
            $this->logger->warning("SimpleConsumer changeInvisibleDuration: no receipt handle, skipping");
            return false;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        $duration = new Duration();
        $duration->setSeconds($invisibleDurationSeconds);
        $duration->setNanos(0);

        $request = new \Apache\Rocketmq\V2\ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);
        if ($messageId) {
            $request->setMessageId($messageId);
        }

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->ChangeInvisibleDuration($request, $metadata)->wait();
            if ($status->code !== 0) {
                $this->logger->warning("SimpleConsumer changeInvisibleDuration failed: " . $status->details);
                return false;
            }
            return true;
        } catch (\Exception $e) {
            $this->logger->error("SimpleConsumer changeInvisibleDuration exception: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Notify server that this client is terminating.
     */
    private function notifyClientTermination()
    {
        $request = new NotifyClientTerminationRequest();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }
        $request->setGroup($groupResource);

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->NotifyClientTermination($request, $metadata)->wait();
            if ($status->code === 0) {
                $this->logger->debug("NotifyClientTermination sent successfully");
            } else {
                $this->logger->warning("NotifyClientTermination failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("NotifyClientTermination exception: " . $e->getMessage());
        }
    }

    /**
     * Shut down the consumer
     */
    public function shutdown()
    {
        if (!$this->isStarted) {
            return;
        }

        $this->stopHeartbeat();

        $this->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->isStarted = false;
    }

    /**
     * Start periodic heartbeat via SIGALRM timer.
     * Sends initial heartbeat then schedules recurring heartbeat every 10s.
     */
    public function startHeartbeat()
    {
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(SIGALRM, function () use ($self) {
                $self->doHeartbeat();
                $self->scheduleNextHeartbeat();
            });
        }

        // Send initial heartbeat
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();

        // Schedule recurring heartbeat
        $this->scheduleNextHeartbeat();
    }

    /**
     * Schedule next heartbeat alarm in 10 seconds.
     */
    private function scheduleNextHeartbeat()
    {
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(10);
        }
    }

    /**
     * Stop heartbeat timer (cancel SIGALRM).
     */
    public function stopHeartbeat()
    {
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(0);
        }
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGALRM, SIG_DFL);
        }
        $this->logger->debug("Heartbeat timer stopped");
    }

    /**
     * Asynchronously receive messages via Swoole coroutine.
     *
     * @param int $maxMessages Maximum messages
     * @param int $invisibleDuration Invisible duration in seconds
     * @return \Generator|array
     */
    public function receiveAsync($maxMessages = 10, $invisibleDuration = 30)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $maxMessages, $invisibleDuration, $channel) {
                try {
                    $messages = $self->receive($maxMessages, $invisibleDuration);
                    $channel->push(['result' => $messages]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'];
        }
        yield $this->receive($maxMessages, $invisibleDuration);
    }

    /**
     * Asynchronously acknowledge messages via Swoole coroutine.
     *
     * @param array $messages Messages to ack
     * @return \Generator
     */
    public function ackAsync($messages)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $messages, $channel) {
                try {
                    $self->ack($messages);
                    $channel->push(['success' => true]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        yield $this->ack($messages);
    }

    /**
     * Asynchronously change invisible duration via Swoole coroutine.
     *
     * @param object $message Message to modify
     * @param int $invisibleDurationSeconds New invisible duration
     * @return \Generator
     */
    public function changeInvisibleDurationAsync($message, $invisibleDurationSeconds)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $message, $invisibleDurationSeconds, $channel) {
                try {
                    $success = $self->changeInvisibleDuration($message, $invisibleDurationSeconds);
                    $channel->push(['success' => $success]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        yield $this->changeInvisibleDuration($message, $invisibleDurationSeconds);
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
     * Get the namespace.
     *
     * @return string
     */
    public function getNamespace()
    {
        return $this->namespace;
    }

    /**
     * Set the namespace.
     *
     * @param string $namespace
     * @return $this
     */
    public function setNamespace(string $namespace)
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Set credentials.
     *
     * @param SessionCredentials $credentials
     * @return $this
     */
    public function setCredentials(SessionCredentials $credentials)
    {
        $this->credentials = $credentials;
        return $this;
    }

    /**
     * Get the parsed endpoints Endpoints protobuf object.
     *
     * @return \Apache\Rocketmq\V2\Endpoints
     */
    public function getParsedEndpoints()
    {
        if ($this->parsedEndpoints === null) {
            $this->parsedEndpoints = $this->parseEndpoints($this->endpoints);
        }
        return $this->parsedEndpoints;
    }

    /**
     * Get the await duration in seconds.
     *
     * @return int
     */
    public function getAwaitDuration()
    {
        return $this->awaitDuration;
    }

    /**
     * Get a gRPC client connected to the broker endpoints from a MessageQueue.
     * Reuses cached client if available, creates new one otherwise.
     * Falls back to proxy client if broker endpoints not available.
     *
     * @param MessageQueue $messageQueue
     * @return MessagingServiceClient
     */
    public function getBrokerClient($messageQueue)
    {
        $broker = $messageQueue->getBroker();
        if (!$broker || !$broker->hasEndpoints()) {
            $this->logger->debug("Broker has no endpoints, falling back to proxy client");
            return $this->client;
        }

        $endpointsProto = $broker->getEndpoints();
        $addresses = $endpointsProto->getAddresses();
        $addressArray = ProtobufUtil::repeatedFieldToArray($addresses);
        if (empty($addressArray) || $addressArray[0] === null) {
            $this->logger->debug("No addresses in broker endpoints, falling back to proxy client");
            return $this->client;
        }

        $address = $addressArray[0];
        $brokerKey = $address->getHost() . ':' . $address->getPort();

        // Reuse cached client if available
        if (isset($this->brokerClients[$brokerKey])) {
            $this->logger->debug("Reusing cached broker client for {$brokerKey}");
            return $this->brokerClients[$brokerKey];
        }

        // Create new gRPC client to broker
        $this->logger->info("Creating new broker client for {$brokerKey}");
        $this->brokerClients[$brokerKey] = RpcClientManager::getInstance()->getClient($brokerKey, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        return $this->brokerClients[$brokerKey];
    }

    /**
     * Get the underlying MessagingServiceClient.
     *
     * @return MessagingServiceClient
     */
    public function getClient()
    {
        return $this->client;
    }

    // ClientTrait required methods
    protected function getCredentials(): ?SessionCredentials
    {
        return $this->credentials;
    }

    protected function getClientIdValue(): string
    {
        return $this->clientId;
    }

    protected function getNamespaceValue(): string
    {
        return $this->namespace;
    }

    /**
     * Generate a unique attempt ID for receiveMessage retry tracking.
     *
     * @return string
     */
    protected function generateAttemptId(): string
    {
        return 'php-' . uniqid('', true);
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
