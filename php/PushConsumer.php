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
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/ProcessQueue.php';
require_once __DIR__ . '/ConsumeService.php';
require_once __DIR__ . '/LiteFifoConsumeService.php';
require_once __DIR__ . '/Signature.php';

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
    private $client;
    private $endpoints;
    private $clientId;
    protected $consumerGroup;
    private $telemetrySession;
    private $subscriptionExpressions = [];
    private $cacheAssignments = [];
    private $processQueueTable = [];
    private $consumeService = null;
    private $isRunning = false;
    private $shutdownRequested = false;
    protected $logger;

    // Builder options
    private $messageListener = null;
    private $maxCacheMessageCount = 4096;
    private $maxCacheMessageSizeInBytes = 67108864; // 64MB
    private $awaitDuration = 30; // seconds
    private $scanIntervalSeconds = 5;
    private $fifo = false;
    private $receiveBatchSize = 32;
    private $enableFifoConsumeAccelerator = false;
    private $isLiteConsumer = false;
    private $credentials = null; // SessionCredentials for AK/SK auth
    private $namespace = '';
    private $lastHeartbeatTime = 0;

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

        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials);
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
        $this->checkNotRunning();
        $this->subscriptionExpressions[$topic] = $expression;
        return $this;
    }

    /**
     * Unsubscribe from a topic.
     */
    public function unsubscribe($topic)
    {
        $this->checkNotRunning();
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

                if ($this->shutdownRequested) {
                    break;
                }

                $now = time();
                if ($now - $lastScanTime >= $this->scanIntervalSeconds) {
                    $this->scanAssignments();
                    $lastScanTime = $now;
                }

                // Periodic heartbeat
                $this->onHeartbeatTick();

                // Fetch messages from each active ProcessQueue
                foreach ($this->processQueueTable as $key => $pq) {
                    if ($pq->isDropped()) {
                        continue;
                    }
                    if ($pq->expired()) {
                        $pq->drop();
                        unset($this->processQueueTable[$key]);
                        continue;
                    }
                    if (!$pq->isCacheFull()) {
                        $pq->fetchMessages();
                    }
                }

                // Consume cached messages
                if ($this->consumeService !== null) {
                    foreach ($this->processQueueTable as $pq) {
                        if (!$pq->isDropped() && !empty($pq->getCachedMessages())) {
                            $this->consumeService->consume($pq);
                        }
                    }
                }

                // Short sleep between iterations
                usleep(100000);

                // Periodic garbage collection
                gc_collect_cycles();
            }

            $this->shutdown();

        } catch (\Exception $e) {
            $this->logger->error("PushConsumer start failed: " . $e->getMessage());
            throw $e;
        }
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
     * Register SIGTERM/SIGINT signal handlers.
     */
    private function registerSignalHandlers()
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
    private function establishTelemetrySession()
    {
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');

        $subscriptionEntries = [];
        foreach ($this->subscriptionExpressions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);

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

        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);

        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);

        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $success = $this->telemetrySession->establishAndSyncSettings($command);
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

        // Drop ProcessQueues no longer in the latest assignments
        foreach ($this->processQueueTable as $key => $pq) {
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
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        $request = new QueryAssignmentRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->parseEndpoints($this->endpoints));

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        if (!empty($this->namespace)) {
            $groupResource->setResourceNamespace($this->namespace);
        }
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
        return $this->consumeService->ackMessage($messageView);
    }

    /**
     * Reject a message (change invisible duration for retry).
     *
     * @param MessageView $messageView
     * @param int $invisibleDuration Next invisible duration in seconds
     * @return bool
     */
    public function nackMessage(MessageView $messageView, int $invisibleDuration = 30): bool
    {
        if ($this->consumeService === null) {
            $this->logger->warning("PushConsumer nackMessage: consume service not initialized");
            return false;
        }
        return $this->consumeService->nackMessage($messageView, 1);
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
     * Parse endpoints string into protobuf Endpoints object.
     *
     * @param string $endpoints e.g. "127.0.0.1:8080" or "example.com:8080"
     * @return Endpoints
     */
    private function parseEndpoints($endpoints)
    {
        $cleaned = $endpoints;
        // Strip http/https prefix if present
        if (strpos($cleaned, 'http://') === 0) {
            $cleaned = substr($cleaned, 7);
        } elseif (strpos($cleaned, 'https://') === 0) {
            $cleaned = substr($cleaned, 8);
        }

        $lastColon = strrpos($cleaned, ':');
        if ($lastColon !== false) {
            $host = substr($cleaned, 0, $lastColon);
            $port = (int)substr($cleaned, $lastColon + 1);
        } else {
            $host = $cleaned;
            $port = 80;
        }

        // Determine address scheme
        $scheme = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) !== false
            ? AddressScheme::IPv4
            : AddressScheme::DOMAIN_NAME;

        $address = new Address();
        $address->setHost($host);
        $address->setPort($port);

        $endpointsObj = new Endpoints();
        $endpointsObj->setScheme($scheme);
        $endpointsObj->setAddresses([$address]);

        return $endpointsObj;
    }

    /**
     * Register settings change callback on the Telemetry session.
     */
    private function registerSettingsCallback()
    {
        $self = $this;
        $this->telemetrySession->setOnSettingsChange(function ($settings) use ($self) {
            $self->onServerSettings($settings);
        });
    }

    /**
     * Handle server-pushed Settings.
     */
    private function onServerSettings($settings)
    {
        $this->logger->info("Processing server settings");
        if (method_exists($settings, 'getBackoffPolicy') && $settings->hasBackoffPolicy()) {
            $this->logger->info("Received backoff policy from server");
        }
    }

    /**
     * Send heartbeat to all route endpoints.
     */
    private function doHeartbeat()
    {
        if (empty($this->processQueueTable)) {
            return;
        }

        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::PUSH_CONSUMER);

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
     * Heartbeat tick handler - called from main loop.
     */
    private function onHeartbeatTick()
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            $this->doHeartbeat();
            $this->lastHeartbeatTime = $now;
        }
    }

    /**
     * Build metadata for gRPC calls using Signature class.
     *
     * @return array
     */
    protected function buildMetadata()
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
