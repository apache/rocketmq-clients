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
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/ConsumeResultSuspend.php';
require_once __DIR__ . '/ProcessQueue.php';
require_once __DIR__ . '/ConsumeService.php';
require_once __DIR__ . '/LiteFifoConsumeService.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/SwooleCompat.php';
require_once __DIR__ . '/ClientTrait.php';
require_once __DIR__ . '/ProtobufUtil.php';
require_once __DIR__ . '/ExponentialBackoffRetryPolicy.php';
require_once __DIR__ . '/CustomizedBackoffRetryPolicy.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryAssignmentRequest;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\QueryRouteRequest;
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
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Google\Protobuf\Duration;
use Grpc\ChannelCredentials;

/**
 * PushConsumer - Push-style consumer referencing Java PushConsumerImpl.
 *
 * Architecture (adapted for PHP single-threaded model):
 * - Blocks in start() with a main polling loop
 * - Periodically scans assignments via QueryAssignment gRPC
 * - Creates/drops ProcessQueue per assigned MessageQueue
 * - ProcessQueue caches messages and dispatches to ConsumeService
 * - ConsumeService invokes user callback sequentially (no thread pool in PHP)
 *
 * Configuration options mirror Java PushConsumerBuilderImpl.
 */
class PushConsumer
{
    use ClientTrait;

    private $client;
    protected $endpoints;
    protected $clientId;
    protected $consumerGroup;
    protected $telemetrySession;
    private $subscriptionExpressions = [];
    private $cacheAssignments = [];
    private $processQueueTable = [];
    protected $consumeService = null;
    protected $isRunning = false;
    protected $shutdownRequested = false;
    protected $logger;

    // Builder options
    protected $messageListener = null;
    private $maxCacheMessageCount = 4096;
    private $maxCacheMessageSizeInBytes = 67108864; // 64MB
    private $awaitDuration = 30; // seconds
    private $scanIntervalSeconds = 5;
    private $fifo = false;
    private $receiveBatchSize = 32;
    protected $enableFifoConsumeAccelerator = false;
    private $isLiteConsumer = false;
    private $credentials = null; // SessionCredentials for AK/SK auth
    private $namespace = '';
    private $lastHeartbeatTime = 0;
    private $shutdownDrainDeadline = null;
    private $retryPolicy = null;

    /**
     * Constructor with builder-style options.
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $consumerGroup, $options = [])
    {
        $this->endpoints = $endpoints;
        if (empty($consumerGroup)) {
            throw new \InvalidArgumentException("PushConsumer consumerGroup cannot be empty");
        }
        $this->consumerGroup = $consumerGroup;
        $this->clientId = $options['clientId'] ?? ('php-push-consumer-' . getmypid() . '-' . time());
        $this->messageListener = $options['messageListener'] ?? null;
        $this->subscriptionExpressions = $options['subscriptionExpressions'] ?? [];
        $this->maxCacheMessageCount = $options['maxCacheMessageCount'] ?? 4096;
        $this->maxCacheMessageSizeInBytes = $options['maxCacheMessageSizeInBytes'] ?? 67108864;
        $this->awaitDuration = $options['awaitDuration'] ?? 30;
        $this->scanIntervalSeconds = $options['scanIntervalSeconds'] ?? 5;
        $this->fifo = $options['fifo'] ?? false;
        $this->receiveBatchSize = $options['receiveBatchSize'] ?? 32;
        $this->enableFifoConsumeAccelerator = $options['enableFifoConsumeAccelerator'] ?? false;
        $this->isLiteConsumer = $options['isLiteConsumer'] ?? false;
        $this->namespace = $options['namespace'] ?? '';

        // Set AK/SK credentials if provided
        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        $this->logger = Logger::getInstance('PushConsumer');

        // Use RpcClientManager for connection pooling
        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials, $this->namespace);
        $this->retryPolicy = new ExponentialBackoffRetryPolicy(5, 1000, 30000, 2.0);
    }

    /**
     * Subscribe to a topic.
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe($topic, $expression = '*')
    {
        if ($this->isRunning) {
            // Dynamic runtime subscription: update subscription expressions
            $this->subscriptionExpressions[$topic] = $expression;
            $this->logger->info("Dynamically subscribed to topic: {$topic}");
            return $this;
        }
        $this->subscriptionExpressions[$topic] = $expression;
        return $this;
    }

    public function getRetryPolicy()
    {
        return $this->retryPolicy;
    }

    /**
     * Unsubscribe from a topic.
     */
    public function unsubscribe($topic)
    {
        if ($this->isRunning) {
            // Dynamic runtime unsubscription
            unset($this->subscriptionExpressions[$topic]);
            unset($this->cacheAssignments[$topic]);
            // Drop related ProcessQueues
            foreach ($this->processQueueTable as $key => $pq) {
                $mq = $pq->getMessageQueue();
                if (method_exists($mq, 'getTopic') && $mq->getTopic()->getName() === $topic) {
                    $pq->drop();
                    unset($this->processQueueTable[$key]);
                }
            }
            $this->logger->info("Dynamically unsubscribed from topic: {$topic}");
            return $this;
        }
        unset($this->subscriptionExpressions[$topic]);
        unset($this->cacheAssignments[$topic]);
        return $this;
    }

    /**
     * Set the message listener callback.
     *
     * @param callable $listener function($messageView): int
     * @return $this
     */
    public function setMessageListener(callable $listener)
    {
        $this->checkNotRunning();
        $this->messageListener = $listener;
        return $this;
    }

    /**
     * Start the PushConsumer. Blocks in the main polling loop.
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }

        if ($this->messageListener === null) {
            throw new \RuntimeException("PushConsumer messageListener is not set");
        }

        if (empty($this->subscriptionExpressions)) {
            throw new \RuntimeException("PushConsumer has no subscriptions");
        }

        $this->logger->info("PushConsumer starting, clientId={$this->clientId}");

        try {
            $this->establishTelemetrySession();

            // Register settings change callback
            $this->registerSettingsCallback();

            $this->onStartBeforeLoop();

            // Create consume service (Standard, FIFO, or LiteFIFO)
            if ($this->isLiteConsumer) {
                $this->consumeService = new LiteFifoConsumeService($this->logger, $this->messageListener, $this, $this->enableFifoConsumeAccelerator);
            } elseif ($this->fifo) {
                $this->consumeService = new FifoConsumeService($this->logger, $this->messageListener, $this, $this->enableFifoConsumeAccelerator);
            } else {
                $this->consumeService = new StandardConsumeService($this->logger, $this->messageListener, $this);
            }

            $this->registerSignalHandlers();
            $this->isRunning = true;

            $this->logger->info("PushConsumer started successfully, clientId={$this->clientId}");

            // Initial assignment scan
            $this->scanAssignments();

            // Main polling loop
            $lastScanTime = time();

            while ($this->isRunning && !$this->shutdownRequested) {
                // Dispatch pending signals
                if (function_exists('pcntl_signal_dispatch')) {
                    pcntl_signal_dispatch();
                }

                if ($this->telemetrySession) {
                    $this->telemetrySession->pollTelemetry();
                }
                if ($this->shutdownRequested) {
                    break;
                }

                $now = time();
                if ($now - $lastScanTime >= $this->scanIntervalSeconds) {
                    $this->scanAssignments();
                    $this->onScanCycleComplete();
                    $lastScanTime = $now;
                }

                // Periodic heartbeat
                $this->onHeartbeatTick();

                // Fetch messages from each active ProcessQueue
                $this->fetchMessageInterleavedHeartbeat();
                // Short sleep between iterations
                usleep(100000);

                // Periodic garbage collection
                gc_collect_cycles();
            }

            // Graceful shutdown drain phase
            $this->drainInFlightMessages();

            $this->shutdown();

        } catch (\Exception $e) {
            $this->logger->error("PushConsumer start failed: " . $e->getMessage());
            $this->onStop();
            throw $e;
        }
    }

    public function startWithTimeout(int $seconds)
    {
        if ($this->isRunning()) {
            return;
        }

        if ($this->messageListener === null) {
            throw new \RuntimeException("PushConsumer messageListener is not set");
        }
        if (empty($this->subscriptionExpressions)) {
            throw new \RuntimeException("PushConsumer has no subscriptions");
        }
        $this->logger->info("PushConsumer starting with timeout {$seconds} seconds, clientId={$this->clientId}");
        try {
            $this->establishTelemetrySession();
            $this->registerSettingsCallback();
            $this->onStartBeforeLoop();
            if ($this->isLiteConsumer) {
                $this->consumeService = new LiteFifoConsumeService($this->logger, $this->messageListener, $this, $this->enableFifoConsumeAccelerator);
            } elseif ($this->fifo) {
                $this->consumeService = new FifoConsumeService($this->logger, $this->messageListener, $this, $this->enableFifoConsumeAccelerator);
            } else {
                $this->consumeService = new StandardConsumeService($this->logger, $this->messageListener, $this);
            }
            $this->registerSignalHandlers();
            $this->isRunning = true;
            $this->logger->info("PushConsumer running with  timeout {$seconds} seconds, clientId={$this->clientId}");
            $this->scanAssignments();
            $deadline = time() + $seconds;
            $lastScanTime = time();
            while ($this->isRunning && !$this->shutdownRequested && time() < $deadline) {
                if (function_exists('pcntl_signal_dispatch')) {
                    pcntl_signal_dispatch();
                }
                if ($this->telemetrySession) {
                    $this->telemetrySession->pollTelemetry();
                }
                if ($this->shutdownRequested) {
                    break;
                }
                $now = time();
                if ($now - $lastScanTime >= $this->scanIntervalSeconds) {
                    $this->scanAssignments();
                    $this->onScanCycleComplete();
                    $lastScanTime = $now;
                }

                $this->onHeartbeatTick();

                $this->fetchMessageInterleavedHeartbeat();
                usleep(100000);
                gc_collect_cycles();
            }

            $this->logger->info("PushConsumer startWithTimeout completed after {$seconds}s, clientId={$this->clientId}");
            $this->onStop();
        } catch (\Exception $e) {
            $this->logger->error("PushConsumer startWithTimeout failed: " . $e->getMessage());
            $this->onStop();
            throw $e;
        }
    }

    public function getConsumeService()
    {
        return $this->consumeService;
    }

    protected function onStartBeforeLoop()
    {

    }

    protected function onStop()
    {

    }

    private function wrapHeartbeatRequest()
    {
        $request = new HeartbeatRequest();
        $request->setClientType($this->getClientType());
        $request->setGroup($this->getGroupResource());
        return $request;
    }

    private function fetchMessageInterleavedHeartbeat()
    {
        foreach ($this->processQueueTable as $key => $pq) {
            if ($pq->isDropped() || $pq->expired()) {
                $pq->drop();
                unset($this->processQueueTable[$key]);
                continue;
            }
            if (!$pq->isCacheFull()) {
                $this->onHeartbeatTick();
                $pq->fetchMessages();
            }
        }
    }

    /**
     * Drain in-flight messages before shutdown. Waits up to 30s for cached messages
     * to be consumed, preventing message loss on abrupt termination.
     */
    private function drainInFlightMessages()
    {
        $drainStart = microtime(true);
        $drainTimeout = 30; // seconds
        $drainIterations = 0;

        $this->logger->info("PushConsumer drain phase: waiting for in-flight messages to be consumed");

        // Stop fetching new messages
        foreach ($this->processQueueTable as $pq) {
            $pq->drop();
        }

        // Drain cached messages
        while (microtime(true) - $drainStart < $drainTimeout) {
            $remainingCount = 0;

            foreach ($this->processQueueTable as $pq) {
                $messages = $pq->getCachedMessages();
                $remainingCount += count($messages);

                if (!empty($messages) && $this->consumeService !== null) {
                    $this->consumeService->consume($pq);
                }
            }

            if ($remainingCount === 0) {
                $this->logger->info("PushConsumer drain phase completed after {$drainIterations} iterations");
                return;
            }

            usleep(100000);
            $drainIterations++;
        }

        $this->logger->warning("PushConsumer drain phase timed out after {$drainTimeout}s, {$remainingCount} messages remaining");
    }

    /**
     * Request graceful shutdown.
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }

        $this->logger->info("PushConsumer shutting down, clientId={$this->clientId}");

        $this->isRunning = false;

        // Notify server of client termination
        $this->notifyClientTermination();

        // Drop all ProcessQueues
        foreach ($this->processQueueTable as $pq) {
            $pq->drop();
        }
        $this->processQueueTable = [];

        // Close telemetry session
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->logger->info("PushConsumer shutdown complete, clientId={$this->clientId}");
    }

    /**
     * Signal handler for graceful shutdown.
     */
    public function requestShutdown()
    {
        $this->shutdownRequested = true;
    }

    /**
     * Register a message interceptor.
     *
     * @param MessageInterceptor $interceptor
     * @return $this
     */
    public function addInterceptor(MessageInterceptor $interceptor)
    {
        if (!isset($this->interceptors)) {
            $this->interceptors = [];
        }
        $this->interceptors[] = $interceptor;
        return $this;
    }

    /**
     * Execute interceptors at a given hook point.
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

    protected function getClientType(): int
    {
        return ClientType::PUSH_CONSUMER;
    }

    /**
     * Register SIGTERM/SIGINT signal handlers.
     */
    protected function registerSignalHandlers()
    {
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(SIGTERM, function() use ($self) {
                $self->requestShutdown();
            });
            pcntl_signal(SIGINT, function() use ($self) {
                $self->requestShutdown();
            });
            $this->logger->info("PushConsumer signal handlers registered");
        }
    }

    /**
     * Establish Telemetry Session (same pattern as SimpleConsumerOptimized).
     */
    protected function establishTelemetrySession()
    {
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

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

        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);

        $settings = new Settings();
        $settings->setClientType($this->getClientType());
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);

        $settings->setAccessPoint($this->parseEndpoints($this->endpoints));
        $timeoutDuration = new Duration();
        $timeoutDuration->setSeconds(3);
        $timeoutDuration->setNanos(0);
        $settings->setRequestTimeout($timeoutDuration);

        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $success = $this->telemetrySession->createStreamAndSync($command);
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
    }

    /**
     * Scan assignments for all subscribed topics.
     */
    private function scanAssignments()
    {
        $this->logger->debug("PushConsumer scanning assignments");

        foreach ($this->subscriptionExpressions as $topic => $expression) {
            try {
                $assignments = $this->queryAssignment($topic);
                $newAssignments = $assignments ? $assignments->getAssignments() : [];

                $oldAssignments = isset($this->cacheAssignments[$topic]) ? $this->cacheAssignments[$topic] : null;
                $newIsEmpty = empty($newAssignments);
                $oldIsEmpty = $oldAssignments === null || empty($oldAssignments);
                if ($newIsEmpty && $oldIsEmpty) {
                    $this->logger->debug("PushConsumer acquired empty assignment from remote, would scan later, for topic $topic");
                    continue;
                }
                $this->syncProcessQueues($topic, $newAssignments, $expression);
                $this->cacheAssignments[$topic] = $newAssignments;
            } catch (\Exception $e) {
                $this->logger->warning("PushConsumer scanAssignments failed for topic={$topic}: " . $e->getMessage());
            }
        }
    }

    /**
     * Sync ProcessQueues with the latest assignments.
     */
    private function syncProcessQueues($topic, $newAssignments, $expression)
    {
        $latestMQKeys = [];
        foreach ($newAssignments as $assignment) {
            if (method_exists($assignment, 'getMessageQueue')) {
                $mq = $assignment->getMessageQueue();
                $mqKey = $this->getMqKey($mq);
                $latestMQKeys[$mqKey] = $mq;
            }
        }
        if (empty($newAssignments)) {
            $existingCount = count($this->processQueueTable);
            if ($existingCount > 0) {
                $this->logger->warning("Broker returned 0 assignments for topics={$topic}, keeping {$existingCount} existing ProcessQueues");
            }
            return;
        }

        // Drop ProcessQueues no longer in the latest assignments
        foreach ($this->processQueueTable as $key => $pq) {
            $pqMq = $pq->getMessageQueue();
            $pqTopic = method_exists($pqMq, 'getTopic') ? $pqMq->getTopic()->getName() : null;
            if ($pqTopic !== $topic) {
                continue;
            }
            if (!isset($latestMQKeys[$key])) {
                $pq->drop();
                unset($this->processQueueTable[$key]);
                $this->logger->info("PushConsumer dropped ProcessQueue: {$key}");
            }
        }

        // Create new ProcessQueues for new assignments
        foreach ($latestMQKeys as $key => $mq) {
            $alreadyExists = false;
            foreach ($this->processQueueTable as $existingKey => $existingPq) {
                if ($existingKey === $key) {
                    $alreadyExists = true;
                    break;
                }
            }

            if (!$alreadyExists) {
                $pq = new ProcessQueue($this, $mq, $expression);
                $this->processQueueTable[$key] = $pq;
                $pq->fetchMessageImmediately();
                $this->logger->info("PushConsumer created ProcessQueue: {$key}");
            }
        }
    }

    /**
     * Query assignment for a topic via QueryAssignment gRPC.
     *
     * @param string $topic
     * @return QueryAssignmentResponse|null
     */
    private function queryAssignment($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryAssignmentRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->parseEndpoints($this->endpoints));

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request->setGroup($groupResource);

        $metadata = $this->buildMetadata();

        list($response, $status) = $this->client->QueryAssignment($request, $metadata)->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("QueryAssignment failed for topic={$topic}: " . $status->details);
        }

        $this->logger->debug("PushConsumer QueryAssignment for {$topic}: " . count($response->getAssignments()) . " assignments");

        return $response;
    }

    /**
     * Generate a unique key for a MessageQueue.
     *
     * @param MessageQueue $mq
     * @return string
     */
    private function getMqKey($mq)
    {
        $topicName = $mq->hasTopic() ? $mq->getTopic()->getName() : 'unknown';
        $queueId = $mq->getId() ?? 0;
        $brokerName = 'default';
        if ($mq->hasBroker()) {
            $broker = $mq->getBroker();
            $brokerName = $broker->getName() ?: 'default';
        }
        return "{$topicName}:{$brokerName}:{$queueId}";
    }

    /**
     * @return bool
     */
    public function isRunning()
    {
        return $this->isRunning;
    }

    /**
     * Get the subscription expressions map.
     *
     * @return array ['topic' => 'expression', ...]
     */
    public function getSubscriptionExpressions()
    {
        return $this->subscriptionExpressions;
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

    /**
     * Get the Client ID.
     *
     * @return string
     */
    public function getClientId()
    {
        return $this->clientId;
    }

    /**
     * Get the consumer group Resource object.
     *
     * @return Resource
     */
    public function getGroupResource()
    {
        $resource = new Resource();
        $resource->setName($this->consumerGroup);
        return $resource;
    }

    public function getTopicResource($topic)
    {
        $resource = new Resource();
        if ($this->namespace !== '') {
            $resource->setResourceNamespace($this->namespace);
        }
        $resource->setName($topic);
        return $resource;
    }

    public function getGroupResourceWithNamespace()
    {
        $resource = new Resource();
        if ($this->namespace !== '') {
            $resource->setResourceNamespace($this->namespace);
        }
        $resource->setName($this->consumerGroup);
        return $resource;
    }

    public function getSessionCredentials(): ?SessionCredentials
    {
        return $this->credentials;
    }

    /**
     * Hook called after each scan cycle. Override in subclasses for periodic tasks.
     */
    protected function onScanCycleComplete()
    {
        // No-op in base class
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
     * Get the await duration in seconds.
     *
     * @return int
     */
    public function getAwaitDuration()
    {
        return $this->awaitDuration;
    }

    /**
     * Get the receive batch size.
     *
     * @return int
     */
    public function getReceiveBatchSize()
    {
        return $this->receiveBatchSize;
    }

    /**
     * Get per-queue cache message count threshold.
     *
     * @return int
     */
    public function getCacheMessageCountThresholdPerQueue()
    {
        $size = count($this->processQueueTable);
        if ($size <= 0) {
            return 0;
        }
        return max(1, (int)($this->maxCacheMessageCount / $size));
    }

    /**
     * Get per-queue cache byte size threshold.
     *
     * @return int
     */
    public function getCacheMessageBytesThresholdPerQueue()
    {
        $size = count($this->processQueueTable);
        if ($size <= 0) {
            return 0;
        }
        return max(1, (int)($this->maxCacheMessageSizeInBytes / $size));
    }

    /**
     * Acknowledge a message via gRPC.
     *
     * @param MessageView $messageView
     * @return bool
     */
    public function ackMessage(MessageView $messageView): bool
    {
        if ($this->consumeService === null) {
            $this->logger->warning("PushConsumer ackMessage: consume service not initialized");
            return false;
        }
        $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
        $this->logger->debug("PushConsumer ackMessage: delegating to consumeService for messageId: {$messageId}");
        return $this->consumeService->ackMessage($messageView);
    }

    /**
     * Reject a message (change invisible duration for retry).
     *
     * @param MessageView $messageView
     * @param int $invisibleDuration Next invisible duration in seconds
     * @return bool
     */
    public function nackMessage(MessageView $messageView, int $deliveryAttempt = 1, ?int $invisibleDuration = null): bool
    {
        if ($this->consumeService === null) {
            $this->logger->warning("PushConsumer nackMessage: consume service not initialized");
            return false;
        }
        return $this->consumeService->nackMessage($messageView, $deliveryAttempt, $invisibleDuration);
    }

    /**
     * Check that the consumer is not yet running.
     */
    protected function checkNotRunning()
    {
        if ($this->isRunning) {
            throw new \RuntimeException("PushConsumer is already running");
        }
    }

    /**
     * ClientTrait required methods
     */
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
     * Register settings change callback on the Telemetry session.
     */
    protected function registerSettingsCallback()
    {
        $self = $this;
        $this->telemetrySession->setOnSettingsChange(function ($settings) use ($self) {
            $self->onServerSettings($settings);
        });
        $this->telemetrySession->setOnVerifyMessage(function ($verifyCmd) use ($self) {
            return $self->onVerifyMessage($verifyCmd);
        });
    }

    /**
     * Handle server-pushed Settings.
     */
    private function onServerSettings($settings)
    {
        $this->logger->info("Processing server settings");

        // Process backoff policy
        if (method_exists($settings, 'getBackoffPolicy') && $settings->hasBackoffPolicy()) {
            $serverPolicy = $settings->getBackoffPolicy();
            $this->logger->info("Received backoff policy from server");
            if (method_exists($serverPolicy, 'getDurations') && !ProtobufUtil::isRepeatedFieldEmpty($serverPolicy->getDurations())) {
                $delays = [];
                foreach ($serverPolicy->getDurations() as $dur) {
                    if (method_exists($dur, 'getSeconds')) {
                        $delays[] = $dur->getSeconds() * 1000;
                    }
                }
                if (!empty($delays)) {
                    $this->retryPolicy = CustomizedBackoffRetryPolicy::fromProtobuf($serverPolicy);
                    $this->logger->info("Updated retry policy from server backoff");
                }
            }
        }

        // Process subscription settings
        if ($settings->hasSubscription()) {
            $sub = $settings->getSubscription();
            if (method_exists($sub, 'getReceiveBatchSize') && $sub->getReceiveBatchSize() > 0) {
                $oldBatchSize = $this->receiveBatchSize;
                $this->receiveBatchSize = $sub->getReceiveBatchSize();
                $this->logger->info("Server set receiveBatchSize: {$oldBatchSize} -> {$this->receiveBatchSize}");
            }
        }
    }

    /**
     * Send heartbeat to all route endpoints.
     */
    private function doHeartbeat()
    {
        $metadata = $this->buildMetadata();
        try {
            list($response, $status) = $this->client->Heartbeat($this->wrapHeartbeatRequest(), $metadata)->wait();
            if ($status->code === 0) {
                $this->logger->info("Heartbeat success, broker: {$this->endpoints}");
            } else {
                $this->logger->warning("Heartbeat failed, broker: {$this->endpoints}");
            }
        } catch (\Exception $e) {
            $this->logger->warning("Heartbeat failed, broker: {$this->endpoints}, error: {$e->getMessage()}");
        }
        if (empty($this->processQueueTable)) {
            return;
        }

        $request = $this->wrapHeartbeatRequest();
        $endpointsMap = [];
        foreach ($this->processQueueTable as $pq) {
            $mq = $pq->getMessageQueue();
            $broker = $mq->getBroker();
            if ($broker && $broker->hasEndpoints()) {
                $endpoints = $broker->getEndpoints();
                $addresses = $endpoints->getAddresses();
                if (!empty($addresses) && $addresses[0] !== null) {
                    $key = $addresses[0]->getHost() . ':' . $addresses[0]->getPort();
                    $endpointsMap[$key] = $endpoints;
                }
            }
        }
        foreach ($endpointsMap as $brokerKey => $endpoints) {
            $metadata = $this->buildMetadata();
            try {
                $brokerClient = RpcClientManager::getInstance()->getClient($brokerKey, [
                    'credentials' => ChannelCredentials::createInsecure(),
                ]);
                list($response, $status) = $brokerClient->Heartbeat($request, $metadata)->wait();
                if ($status->code === 0) {
                    $this->logger->info("Heartbeat success, broker: {$brokerKey}");
                } else {
                    $this->logger->warning("Heartbeat failed, broker: {$brokerKey}, status: {$status->code}");
                }
            } catch (\Exception $e) {
                $this->logger->warning("Heartbeat failed, broker: {$brokerKey}, error: {$e->getMessage()}");
            }
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
     * Heartbeat tick handler - called from main loop.
     */
    protected function onHeartbeatTick()
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            $this->doHeartbeat();
            static $lastRouteRefresh = 0;
            if ($now - $lastRouteRefresh >= 30) {
                $this->refreshRouteCache();
                $lastRouteRefresh = $now;
            }
            $this->lastHeartbeatTime = $now;
        }
    }

    public function startAsync(?callable $onDone = null): bool
    {
        if (!SwooleCompat::isAvailable()) {
            $this->logger->warning("startAsync: Swoole/OneSwoole not available, fallback to start()");
            $this->start();
            return false;
        }
        $this->logger->info("PushConsumer starting in async coroutine, clientId={$this->clientId}");
        $self = $this;
        $channel = new \Swoole\Coroutine\Channel(1);
        \Swoole\Coroutine::create(function () use ($self, $channel, $onDone) {
            try {
                $self->start();
            } catch (\Throwable $e) {
                $self->logger->error("PushConsumer startAsync failed, clientId={$self->clientId}, error={$e->getMessage()}");
            }
            if ($onDone !==  null) {
                try {
                    $onDone();
                } catch (\Throwable $e) {
                    $self->logger->error("PushConsumer startAsync onDone failed, clientId={$self->clientId}, error={$e->getMessage()}");
                }
            }
            $channel->push(true);
        });
        return true;
    }

    private function refreshRouteCache()
    {
        foreach ($this->subscriptionExpressions as $topic => $expression) {
            try {
                $this->queryAssignment($topic);
                $this->logger->debug("Route refreshed for topic={$topic}");
            } catch (\Throwable $e) {
                $this->logger->warning("Route refreshed failed, topic={$topic}, error={$e->getMessage()}");
            }
        }
    }

    private function onVerifyMessage($verifyCmd)
    {
        $message = null;
        if (method_exists($verifyCmd, 'getMessage')) {
            $message = $verifyCmd->getMessage();
        }
        if ($message === null) {
            $this->logger->warning("PushConsumer onVerifyMessage no message in verify command");
            return null;
        }
        try {
            $messageView = new MessageView($message, null, null, 1);
            if ($messageView->isCorrupted()) {
                $this->logger->error("PushConsumer onVerifyMessage message is corrupted");
                $status = new \Apache\Rocketmq\V2\Status();
                $status->setCode(50000);
                $status->setMessage("message is corrupted");
                $result = new \Apache\Rocketmq\V2\VerifyMessageResult();
                $result->setNonce($verifyCmd->getNonce());
                $resp = new \Apache\Rocketmq\V2\TelemetryCommand();
                $resp->setStatus($status);
                $resp->setVerifyMessageResult($result);
                return $resp;
            }
            $result = $this->consumeService->consumeMessage($messageView);
            $code = ($result === ConsumeResult::SUCCESS) ? 20000 : 40000;
            $status = new \Apache\Rocketmq\V2\Status();
            $status->setCode($code);
            $verifyResult = new \Apache\Rocketmq\V2\VerifyMessageResult();
            $verifyResult->setNonce($verifyCmd->getNonce());

            $resp = new \Apache\Rocketmq\V2\TelemetryCommand();
            $resp->setStatus($status);
            $resp->setVerifyMessageResult($verifyResult);
            return $resp;
        } catch (\Exception $e) {
            $this->logger->warning("PushConsumer onVerifyMessage failed: " . $e->getMessage());
            return null;
        }
    }
}
