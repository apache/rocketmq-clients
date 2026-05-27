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
require_once __DIR__ . '/RpcClientManager.php';
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/ClientTrait.php';
require_once __DIR__ . '/SwooleCompat.php';
require_once __DIR__ . '/ProtobufUtil.php';
require_once __DIR__ . '/SubscriptionLoadBalancer.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
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
    private $subscriptionRouteDataCache = [];
    private $topicIndex = 0;
    private $heartbeatCoroutineId = null;

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
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request->setGroup($groupResource);

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

        if ($maxMessages <= 0) {
            throw new \InvalidArgumentException("Invalid maxMessages must be greater than 0");
        }

        $startTime = microtime(true);

        $topics = array_keys($this->subscriptions);
        if (empty($topics)) {
            return [];
        }
        $topicIndex = $this->topicIndex++;
        $index = $topicIndex % count($topics);
        $topic = $topics[$index];
        $expression = $this->subscriptions[$topic];
        $topicCount = count($topics);
        $longPollingTimeout = $topicCount > 1 ? max(1, (int)($this->awaitDuration / $topicCount)) : $this->awaitDuration;
        $messages = $this->receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration, $longPollingTimeout);

        $latencyMs = (microtime(true) - $startTime) * 1000;
        $this->executeInterceptors(MessageHookPoints::RECEIVE, [
            'success' => true,
            'latencyMs' => $latencyMs,
            'messageCount' => count($messages),
        ]);

        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            $this->doHeartbeat();
            $this->lastHeartbeatTime = $now;
        }
        return $messages;
    }

    /**
     * Receive messages from a specific topic
     */
    private function receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration, $longPollingTimeout = null)
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

        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessages);
        $request->setAutoRenew(false);

        $invisibleDurationObj = new Duration();
        $invisibleDurationObj->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);

        $effectiveLongPollingTimeout = $longPollingTimeout ?? $this->awaitDuration;
        $longPollingTimeoutObj = new Duration();
        $longPollingTimeoutObj->setSeconds($effectiveLongPollingTimeout);
        $request->setLongPollingTimeout($longPollingTimeoutObj);

        $attemptId = $this->generateAttemptId();
        $request->setAttemptId($attemptId);

        // Route ReceiveMessage to broker endpoints from MessageQueue
        $receiveClient = $this->getBrokerClient($messageQueue);

        // gRPC timeout in microseconds — server reads this via ctx.getRemainingMs()
        $grpcTimeoutMicroseconds = ($this->requestTimeout + $effectiveLongPollingTimeout * 1000) * 1000;
        $grpcTimeoutMills = (int)($grpcTimeoutMicroseconds / 1000);

        // Use signed metadata via ClientTrait
        $metadata = $this->buildMetadata();
        $metadata['grpc-timeout'] = ["{$grpcTimeoutMills}m"];

        $this->logger->debug("ReceiveMessage: topic={$topic}, batchSize={$maxMessages}, grpcTimeout={$grpcTimeoutMills}us, attemptId={$attemptId}");

        // Receive messages
        $call = $receiveClient->ReceiveMessage($request, $metadata);

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
     * Get MessageQueue via QueryRoute + SubscriptionLoadBalancer.
     * SimpleConsumer uses QueryRoute (not QueryAssignment) like Java's SimpleConsumerImpl.
     * Round-robin across subscribed topics.
     */
    private function getMessageQueue($topic)
    {
        $loadBalancer = $this->getSubscriptionLoadBalancer($topic);
        return $loadBalancer->takeMessageQueue();
    }

    /**
     * Get SubscriptionLoadBalancer for a topic.
     * Caches route data per topic, creating load balancer on first access.
     */
    private function getSubscriptionLoadBalancer($topic)
    {
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            $routeData = $this->getRouteData($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($routeData);
        }

        return $this->subscriptionRouteDataCache[$topic];
    }

    /**
     * Query route data for a topic via QueryRoute RPC.
     */
    private function getRouteData($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->getParsedEndpoints());

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();

            if ($status->code !== 0) {
                $this->logger->warning("QueryRoute failed for topic={$topic}: " . $status->details);
                return null;
            }

            return $response;
        } catch (\Exception $e) {
            $this->logger->warning("QueryRoute exception for topic={$topic}: " . $e->getMessage());
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

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries($entries);

        $metadata = $this->buildMetadata();

        $attempt = 0;

        while (true) {
            try {
                list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("AckMessage attempt {$attempt}: status error: " . $status->details);
                } else {
                    $responseEntries = $response->getEntries();
                    if (!ProtobufUtil::isRepeatedFieldEmpty($responseEntries)) {
                        $hasRetryableError = false;
                        $hasInvalidHandleOnly = true;
                        foreach ($responseEntries as $resultEntry) {
                            if ($resultEntry->hasStatus()) {
                                $entryCode = $resultEntry->getStatus()->getCode();
                                if ($entryCode !== 20000 && $entryCode !== 40003) {
                                    $hasRetryableError = true;
                                    $hasInvalidHandleOnly = false;
                                }
                            }
                        }
                        if ($hasInvalidHandleOnly && !$hasRetryableError) {
                            return;
                        }
                        if ($hasRetryableError) {
                            $entries = $this->filterRetryableEntries($entries, $responseEntries);
                            if (empty($entries)) {
                                return;
                            }
                            $request->setEntries($entries);
                            $attempt++;
                            usleep(1000000);
                            continue;
                        }
                    }
                    return;
                }
            } catch (\Exception $e) {
                $this->logger->warning("AckMessage attempt {$attempt} failed: " . $e->getMessage());
            }
            $attempt++;
            usleep(1000000);
        }
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

        $topicResource = new Resource();
        $topicResource->setName($topic);

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
            // Update receipt handle with the new one returned by server
            if (method_exists($response, 'getReceiptHandle') && $response->getReceiptHandle() !== '') {
                $message->setReceiptHandle($response->getReceiptHandle());
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
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(SIGALRM, function () use ($self) {
                $self->doHeartbeat();
                $self->scheduleNextHeartbeat();
            });
        }

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
        if ($this->heartbeatCoroutineId !== null) {
            \Swoole\Coroutine::cancel($this->heartbeatCoroutineId);
            $this->logger->info("Swoole coroutine heartbeat stopped, coroutine_id=" . $this->heartbeatCoroutineId);
            $this->heartbeatCoroutineId = null;
        }
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

    private function filterRetryableEntries($entries, $responseEntries)
    {
        $responseCodes = [];
        foreach ($responseEntries as $i => $resultEntry) {
            if ($resultEntry->hasStatus()) {
                $responseCodes[$i] = $resultEntry->getStatus()->getCode();
            }
        }
        $retryable = [];
        foreach ($entries as $i => $entry) {
            if (!isset($responseCodes[$i]) || ($responseCodes[$i] !== 20000 && $responseCodes[$i] !== 40003)) {
                $retryable[] = $entry;
            }
        }
        return $retryable;
    }
}
