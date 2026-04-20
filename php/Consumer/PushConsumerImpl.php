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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\ClientStateException;
use Apache\Rocketmq\Exception\NetworkException;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryAssignmentRequest;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;

// Import Assignment management classes
use Apache\Rocketmq\Consumer\Assignment;
use Apache\Rocketmq\Consumer\Assignments;

/**
 * PushConsumer implementation referencing Java architecture
 * 
 * Key features:
 * - ProcessQueue management for each message queue
 * - Assignment scanning for load balancing
 * - Message caching with count and size limits
 * - Thread pool for concurrent consumption
 * - Graceful shutdown with inflight request handling
 */
class PushConsumerImpl implements PushConsumer
{
    private ClientConfiguration $config;
    private string $consumerGroup;
    private string $clientId;
    /** @var array<string, FilterExpression> */
    private array $subscriptionExpressions = [];
    /** @var array<string, Assignments|null> Cached assignments per topic */
    private array $cacheAssignments = [];
    /** @var callable */
    private $messageListener;
    private int $maxCacheMessageCount;
    private int $maxCacheMessageSizeInBytes;
    private bool $enableFifoConsumeAccelerator;
    /** @var array<string, ProcessQueue> */
    private array $processQueueTable = [];
    private int $consumptionThreadCount;
    private string $state = 'CREATED';
    private bool $running = false;
    /** @var int|null Swoole timer ID for periodic assignment scanning */
    private ?int $scanTimerId = null;
    /** @var int|null Swoole timer ID for periodic statistics logging */
    private ?int $statsTimerId = null;
    /** @var MessagingServiceClient|null Shared gRPC client with fixed clientId */
    private ?MessagingServiceClient $client = null;
    /** @var mixed Telemetry bidirectional stream call */
    private $telemetryStream = null;
    /** @var ClientMeterManager|null Metrics manager (optional) */
    private ?ClientMeterManager $meterManager = null;
    private int $receptionTimes = 0;
    private int $receivedMessagesQuantity = 0;
    private int $consumptionOkQuantity = 0;
    private int $consumptionErrorQuantity = 0;

    public function __construct(
        ClientConfiguration $config,
        string $consumerGroup,
        array $subscriptionExpressions,
        callable $messageListener,
        int $maxCacheMessageCount = 4096,
        int $maxCacheMessageSizeInBytes = 67108864, // 64MB
        int $consumptionThreadCount = 20,
        bool $enableFifoConsumeAccelerator = false
    ) {
        $this->config = $config;
        $this->consumerGroup = $consumerGroup;
        $this->subscriptionExpressions = $subscriptionExpressions;
        $this->messageListener = $messageListener;
        $this->maxCacheMessageCount = $maxCacheMessageCount;
        $this->maxCacheMessageSizeInBytes = $maxCacheMessageSizeInBytes;
        $this->consumptionThreadCount = $consumptionThreadCount;
        $this->enableFifoConsumeAccelerator = $enableFifoConsumeAccelerator;
        $this->clientId = Util::generateClientId();

        $routeCache = \Apache\Rocketmq\RouteCache::getInstance();
        $routeCache->setConfigFromClientConfiguration($this->config);
    }

    public function getConsumerGroup(): string
    {
        return $this->consumerGroup;
    }

    public function getSubscriptionExpressions(): array
    {
        return $this->subscriptionExpressions;
    }

    public function subscribe(string $topic, FilterExpression $filterExpression): PushConsumer
    {
        $this->checkRunning();

        $this->getRouteData($topic);
        $this->subscriptionExpressions[$topic] = $filterExpression;

        return $this;
    }

    public function unsubscribe(string $topic): PushConsumer
    {
        $this->checkRunning();

        unset($this->subscriptionExpressions[$topic]);
        unset($this->cacheAssignments[$topic]);

        $this->dropProcessQueuesByTopic($topic);

        return $this;
    }

    /**
     * Start the push consumer
     * 
     * @throws ClientException If startup fails
     */
    public function start(): void
    {
        if ($this->state !== 'CREATED') {
            throw new ClientStateException("Consumer is already {$this->state}");
        }

        Logger::info("Begin to start the rocketmq push consumer, clientId={$this->clientId}");

        $this->state = 'STARTING';

        try {
            // Create shared gRPC client with fixed clientId
            $this->initClient();

            // Establish Telemetry session to register with proxy
            $this->initTelemetrySession();

            // Initialize metrics system (aligned with Java)
            $this->initMetrics();

            $this->heartbeat();

            $this->state = 'RUNNING';
            $this->running = true;

            $this->startAssignmentScanning();

            Logger::info("The rocketmq push consumer starts successfully, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            $this->state = 'FAILED';
            Logger::error("Failed to start the rocketmq push consumer, clientId={$this->clientId}, error={$e->getMessage()}");
            throw new ClientException("Failed to start push consumer: " . $e->getMessage(), 0, $e);
        }
    }

    public function close(): void
    {
        if ($this->state === 'TERMINATED' || $this->state === 'STOPPING') {
            return;
        }

        Logger::info("Begin to shutdown the rocketmq push consumer, clientId={$this->clientId}");

        $this->state = 'STOPPING';
        $this->running = false;

        // Cancel periodic assignment scanning timer
        if ($this->scanTimerId !== null) {
            \Swoole\Timer::clear($this->scanTimerId);
            $this->scanTimerId = null;
        }

        // Cancel periodic statistics logging timer
        if ($this->statsTimerId !== null) {
            \Swoole\Timer::clear($this->statsTimerId);
            $this->statsTimerId = null;
        }

        $this->waitingReceiveRequestFinished();

        foreach ($this->processQueueTable as $mq => $pq) {
            $pq->drop();
        }
        $this->processQueueTable = [];

        // Close telemetry stream
        if ($this->telemetryStream !== null) {
            try {
                $this->telemetryStream->writesDone();
            } catch (\Throwable $e) {
                // Ignore
            }
            $this->telemetryStream = null;
        }

        // Shutdown metrics manager
        if ($this->meterManager !== null) {
            try {
                // MetricsCollector doesn't have shutdown, just clear reference
                $this->meterManager = null;
            } catch (\Throwable $e) {
                Logger::error("Failed to shutdown metrics manager, clientId={$this->clientId}, error={$e->getMessage()}");
            }
        }

        $this->client = null;

        $this->state = 'TERMINATED';
        Logger::info("Shutdown the rocketmq push consumer successfully, clientId={$this->clientId}");
    }

    /**
     * Alias for close()
     */
    public function shutdown(): void
    {
        $this->close();
    }

    private function checkRunning(): void
    {
        if ($this->state !== 'RUNNING') {
            throw new ClientStateException("Consumer is not running (current state: {$this->state})");
        }
    }

    /**
     * Send heartbeat to broker
     * 
     * @throws ClientException If heartbeat fails
     */
    private function heartbeat(): void
    {
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $request = new HeartbeatRequest();
        $request->setGroup($groupResource);
        $request->setClientType(ClientType::PUSH_CONSUMER);

        try {
            list($response, $status) = $this->client->Heartbeat($request)->wait();
            Logger::debug("Heartbeat sent successfully, consumerGroup={$this->consumerGroup}, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to send heartbeat, consumerGroup={$this->consumerGroup}, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    private function getRouteData(string $topic): void
    {
        // TODO: Implement route data fetching
    }

    private function startAssignmentScanning(): void
    {
        // Initial scan
        $this->scanAssignments();

        // Schedule periodic scanning every 5 seconds (matching Java's scheduleWithFixedDelay)
        $this->scanTimerId = \Swoole\Timer::tick(5000, function () {
            if (!$this->running) {
                return;
            }
            try {
                $this->scanAssignments();
            } catch (\Throwable $t) {
                Logger::error("Exception raised while scanning assignments, clientId={$this->clientId}, error={$t->getMessage()}");
            }
        });

        // Schedule periodic statistics logging every 60 seconds (aligned with Java)
        $this->statsTimerId = \Swoole\Timer::tick(60000, function () {
            if (!$this->running) {
                return;
            }
            try {
                $this->doStats();
            } catch (\Throwable $t) {
                Logger::error("Exception raised while logging stats, clientId={$this->clientId}, error={$t->getMessage()}");
            }
        });
    }

    /**
     * Scan assignments for all subscribed topics
     * 
     * References Java PushConsumerImpl.scanAssignments():
     * - Catches exceptions per-topic to prevent one topic failure from blocking others
     */
    public function scanAssignments(): void
    {
        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            try {
                $existed = $this->cacheAssignments[$topic] ?? null;
                $latest = $this->queryAssignment($topic);

                if ($latest === null || $latest->isEmpty()) {
                    if ($existed === null || $existed->isEmpty()) {
                        Logger::debug("Acquired empty assignments from remote, would scan later, topic={$topic}, clientId={$this->clientId}");
                        continue;
                    }
                    Logger::warn("Attention!!! acquired empty assignments from remote, but existed assignments is not empty, topic={$topic}, clientId={$this->clientId}");
                }

                // Check if assignments changed (aligned with Java)
                if (!$latest->equals($existed ?? new Assignments([]))) {
                    Logger::info("Assignments of topic={$topic} has changed, {$existed} => {$latest}, clientId={$this->clientId}");
                } else {
                    Logger::debug("Assignments of topic={$topic} remains the same, assignments={$latest}, clientId={$this->clientId}");
                }

                // Sync process queues anyway (process queue may be dropped)
                $this->syncProcessQueue($topic, $latest, $filterExpression);
                $this->cacheAssignments[$topic] = $latest;
            } catch (\Throwable $e) {
                Logger::error("Exception raised while scanning assignments for topic={$topic}, clientId={$this->clientId}, error={$e->getMessage()}");
            }
        }
    }

    /**
     * @return \Google\Protobuf\Internal\RepeatedField|null
     * @throws ClientException|NetworkException If query fails
     */
    private function queryAssignment(string $topic)
    {
        $request = new QueryAssignmentRequest();
        
        // Set topic with namespace (aligned with Java wrapQueryAssignmentRequest)
        $topicRes = new Resource();
        $topicRes->setResourceNamespace($this->config->getNamespace());
        $topicRes->setName($topic);
        $request->setTopic($topicRes);
        
        // Set consumer group with namespace (aligned with Java getProtobufGroup)
        $groupRes = new Resource();
        $groupRes->setResourceNamespace($this->config->getNamespace());
        $groupRes->setName($this->consumerGroup);
        $request->setGroup($groupRes);
        
        // Set endpoints
        $request->setEndpoints($this->config->getEndpointsAsProtobuf());

        try {
            $call = $this->client->QueryAssignment($request);
            list($response, $status) = $call->wait();

            if ($response instanceof QueryAssignmentResponse) {
                // Convert Protobuf assignments to Assignment objects (aligned with Java)
                $assignmentList = [];
                $protobufAssignments = $response->getAssignments();
                
                if ($protobufAssignments !== null) {
                    foreach ($protobufAssignments as $protobufAssignment) {
                        $messageQueue = new MessageQueue();
                        $messageQueue->__constructFromProtobuf($protobufAssignment->getMessageQueue());
                        $assignmentList[] = new Assignment($messageQueue);
                    }
                }
                
                return new Assignments($assignmentList);
            }
        } catch (\Throwable $e) {
            throw new NetworkException("Failed to query assignment for topic {$topic}: " . $e->getMessage(), 0, $e);
        }

        return null;
    }

    private function syncProcessQueue(string $topic, Assignments $assignments, FilterExpression $filterExpression): void
    {
        // Build latest message queue set (aligned with Java)
        $latest = [];
        foreach ($assignments->getAssignmentList() as $assignment) {
            $mq = $assignment->getMessageQueue();
            $latest[$this->getMessageQueueKey($mq)] = $mq;
        }

        $activeMqs = [];

        // Drop process queues that are no longer assigned or expired
        foreach ($this->processQueueTable as $key => $pq) {
            $mq = $pq->getMessageQueue();
            $mqTopic = $mq->getTopic()->getName();

            if ($topic !== $mqTopic) {
                continue;
            }

            if (!isset($latest[$key])) {
                Logger::info("Drop process queue according to the latest assignmentList, topic={$topic}, mq={$key}, clientId={$this->clientId}");
                $this->dropProcessQueue($key);
                continue;
            }

            if ($pq->expired()) {
                Logger::warn("Drop message queue because it is expired, topic={$topic}, mq={$key}, clientId={$this->clientId}");
                $this->dropProcessQueue($key);
                continue;
            }
            
            $activeMqs[$key] = true;
        }

        // Create new process queues for newly assigned message queues
        foreach ($latest as $key => $mq) {
            if (isset($activeMqs[$key])) {
                continue;
            }

            Logger::info("Start to fetch message from remote, topic={$topic}, mq={$key}, clientId={$this->clientId}");
            $processQueue = $this->createProcessQueue($mq, $filterExpression);
            if ($processQueue !== null) {
                $processQueue->fetchMessageImmediately();
            }
        }
    }

    private function createProcessQueue(MessageQueue $mq, FilterExpression $filterExpression): ?ProcessQueue
    {
        $key = $this->getMessageQueueKey($mq);

        if (isset($this->processQueueTable[$key])) {
            return null;
        }

        $processQueue = new ProcessQueue($this, $mq, $filterExpression);
        $this->processQueueTable[$key] = $processQueue;

        return $processQueue;
    }

    private function dropProcessQueue(string $key): void
    {
        if (isset($this->processQueueTable[$key])) {
            $pq = $this->processQueueTable[$key];
            $pq->drop();
            unset($this->processQueueTable[$key]);
        }
    }

    private function dropProcessQueuesByTopic(string $topic): void
    {
        foreach ($this->processQueueTable as $key => $pq) {
            $mq = $pq->getMessageQueue();
            if ($mq->getTopic()->getName() === $topic) {
                $this->dropProcessQueue($key);
            }
        }
    }

    private function getMessageQueueKey(MessageQueue $mq): string
    {
        $topic = $mq->getTopic()->getName();
        $broker = $mq->getBroker()->getName() ?? '';
        $id = $mq->getId() ?? 0;
        return "{$topic}@{$broker}:{$id}";
    }

    /**
     * Wait for inflight receive requests to finish
     * 
     * Checks real process queue state instead of blind wait.
     */
    private function waitingReceiveRequestFinished(): void
    {
        $maxWaitMs = 35000; // 35 seconds (requestTimeout + longPollingTimeout)
        $startTime = microtime(true) * 1000;

        while (true) {
            $hasInflight = false;
            foreach ($this->processQueueTable as $pq) {
                if (!$pq->isDropped()) {
                    $hasInflight = true;
                    break;
                }
            }

            if (!$hasInflight) {
                break;
            }

            $elapsed = microtime(true) * 1000 - $startTime;
            if ($elapsed > $maxWaitMs) {
                Logger::warn("Timed out waiting for inflight requests, elapsed={$elapsed}ms, clientId={$this->clientId}");
                break;
            }

            usleep(100000); // 100ms
        }
    }

    public function getQueueSize(): int
    {
        return count($this->processQueueTable);
    }

    public function cacheMessageCountThresholdPerQueue(): int
    {
        $size = $this->getQueueSize();
        if ($size <= 0) {
            return 0;
        }
        return max(1, intdiv($this->maxCacheMessageCount, $size));
    }

    public function cacheMessageSizeThresholdPerQueue(): int
    {
        $size = $this->getQueueSize();
        if ($size <= 0) {
            return 0;
        }
        return max(1, intdiv($this->maxCacheMessageSizeInBytes, $size));
    }

    public function getMessageListener(): callable
    {
        return $this->messageListener;
    }

    public function getConsumptionThreadCount(): int
    {
        return $this->consumptionThreadCount;
    }

    public function isEnableFifoConsumeAccelerator(): bool
    {
        return $this->enableFifoConsumeAccelerator;
    }

    public function incrementReceptionTimes(): void
    {
        $this->receptionTimes++;
    }

    public function incrementReceivedMessagesQuantity(int $count): void
    {
        $this->receivedMessagesQuantity += $count;
    }

    public function incrementConsumptionOkQuantity(int $count): void
    {
        $this->consumptionOkQuantity += $count;
    }

    public function incrementConsumptionErrorQuantity(int $count): void
    {
        $this->consumptionErrorQuantity += $count;
    }

    /**
     * Log statistics and reset counters
     * 
     * References Java PushConsumerImpl.doStats():
     * - Logs consumer-level stats and resets counters
     * - Calls doStats() on all process queues
     */
    public function doStats(): void
    {
        // Log consumer-level stats (aligned with Java)
        Logger::info(sprintf(
            "clientId=%s, consumerGroup=%s, receptionTimes=%d, receivedMessagesQuantity=%d, consumptionOkQuantity=%d, consumptionErrorQuantity=%d",
            $this->clientId,
            $this->consumerGroup,
            $this->receptionTimes,
            $this->receivedMessagesQuantity,
            $this->consumptionOkQuantity,
            $this->consumptionErrorQuantity
        ));

        // Reset counters
        $this->receptionTimes = 0;
        $this->receivedMessagesQuantity = 0;
        $this->consumptionOkQuantity = 0;
        $this->consumptionErrorQuantity = 0;

        // Call doStats() on all process queues (aligned with Java)
        foreach ($this->processQueueTable as $pq) {
            try {
                $pq->doStats();
            } catch (\Throwable $e) {
                Logger::error("Exception raised while logging process queue stats, clientId={$this->clientId}, error={$e->getMessage()}");
            }
        }
    }

    /**
     * Get client ID
     */
    public function getClientId(): string
    {
        return $this->clientId;
    }

    /**
     * Get current state
     */
    public function getState(): string
    {
        return $this->state;
    }

    /**
     * Check if consumer is running
     */
    public function isRunning(): bool
    {
        return $this->running;
    }

    /**
     * Get client configuration
     */
    public function getConfig(): ClientConfiguration
    {
        return $this->config;
    }

    /**
     * Get shared gRPC client (used by ProcessQueue)
     */
    public function getClient(): MessagingServiceClient
    {
        return $this->client;
    }

    /**
     * Initialize shared gRPC client with fixed clientId
     */
    private function initClient(): void
    {
        $pool = ConnectionPool::getInstance();
        $pool->setConfigFromClientConfiguration($this->config);
        $this->client = $pool->getConnectionWithClientId($this->config, $this->clientId);
        Logger::info("Shared gRPC client created with clientId={$this->clientId}");
    }

    /**
     * Initialize Telemetry bidirectional stream session.
     * 
     * RocketMQ proxy requires clients to register via Telemetry before
     * accepting ReceiveMessage calls (otherwise returns 40401 "disconnected").
     */
    private function initTelemetrySession(): void
    {
        try {
            $this->telemetryStream = $this->client->Telemetry();

            // Build Settings with PUSH_CONSUMER client type and subscription info
            $settings = new Settings();
            $settings->setClientType(ClientType::PUSH_CONSUMER);

            $subscription = new Subscription();
            $group = new Resource();
            $group->setName($this->consumerGroup);
            $subscription->setGroup($group);

            $longPollingDuration = new \Google\Protobuf\Duration();
            $longPollingDuration->setSeconds(15);
            $subscription->setLongPollingTimeout($longPollingDuration);

            // Add subscription entries for all topics
            $entries = [];
            foreach ($this->subscriptionExpressions as $topic => $filterExpr) {
                $entry = new SubscriptionEntry();
                $topicResource = new Resource();
                $topicResource->setName($topic);
                $entry->setTopic($topicResource);

                $v2Filter = new \Apache\Rocketmq\V2\FilterExpression();
                $v2Filter->setExpression($filterExpr->getExpression());
                $v2Filter->setType(
                    $filterExpr->getType() === FilterExpressionType::TAG
                        ? \Apache\Rocketmq\V2\FilterType::TAG
                        : \Apache\Rocketmq\V2\FilterType::SQL
                );
                $entry->setExpression($v2Filter);

                $entries[] = $entry;
            }
            $subscription->setSubscriptions($entries);
            $settings->setSubscription($subscription);

            // Send settings via telemetry stream
            $command = new TelemetryCommand();
            $command->setSettings($settings);
            $this->telemetryStream->write($command);

            // Wait briefly for the proxy to process the registration
            usleep(500000); // 500ms

            Logger::info("Telemetry session established, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to establish telemetry session, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Initialize metrics system with ProcessQueue gauge observer
     * 
     * References Java PushConsumerImpl.startUp():
     * - Creates MetricsCollector and ClientMeterManager
     * - Registers ProcessQueueGaugeObserver for real-time metrics
     * - Enables automatic metric collection
     */
    private function initMetrics(): void
    {
        try {
            // Create metrics collector
            $metricsCollector = new MetricsCollector();
            
            // Create meter manager
            $this->meterManager = new ClientMeterManager($this->clientId, $metricsCollector);
            
            // Register ProcessQueue gauge observer (aligned with Java)
            $gaugeObserver = new ProcessQueueGaugeObserver(
                $this->processQueueTable,
                $this->clientId,
                $this->consumerGroup
            );
            
            $this->meterManager->setGaugeObserver($gaugeObserver);
            
            // Enable metrics with default settings
            // In production, you can configure export endpoint from ClientConfiguration
            $this->meterManager->enable(null, 60); // Export every 60 seconds
            
            Logger::info("Metrics system initialized, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to initialize metrics system, clientId={$this->clientId}, error={$e->getMessage()}");
            // Don't throw - metrics is optional, consumer should still work
        }
    }
}
