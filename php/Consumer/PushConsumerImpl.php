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
use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\ClientStateException;
use Apache\Rocketmq\Exception\NetworkException;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\Assignment;
use Apache\Rocketmq\V2\Assignments;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\QueryAssignmentRequest;
use Apache\Rocketmq\V2\QueryAssignmentResponse;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;

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
    /** @var array<string, Assignments> Cached assignments per topic */
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

        $this->waitingReceiveRequestFinished();

        foreach ($this->processQueueTable as $mq => $pq) {
            $pq->drop();
        }
        $this->processQueueTable = [];

        $this->state = 'TERMINATED';
        Logger::info("Shutdown the rocketmq push consumer successfully, clientId={$this->clientId}");
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
        $request = new HeartbeatRequest();

        $settings = new Settings();
        $settings->setClientType(ClientType::PUSH_CONSUMER);

        $subscription = new Subscription();
        $subscription->setGroup(Resource::create()->setName($this->consumerGroup));

        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            $entry = new SubscriptionEntry();
            $entry->setTopic(Resource::create()->setName($topic));
            $entry->setFilterExpression($filterExpression);
            $subscription->addEntries($entry);
        }

        $request->setSettings($settings);
        $request->setSubscriptions([$subscription]);

        $pool = ConnectionPool::getInstance();
        $pool->setConfigFromClientConfiguration($this->config);
        $client = $pool->getConnection($this->config);

        try {
            $call = $client->Heartbeat($request);
            $call->wait();
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
        $this->scanAssignments();
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

                if ($latest === null || empty($latest->getAssignmentList())) {
                    if ($existed === null || empty($existed->getAssignmentList())) {
                        Logger::debug("No assignments found for topic, topic={$topic}, clientId={$this->clientId}");
                        continue;
                    }
                }

                if ($latest !== $existed) {
                    $this->syncProcessQueue($topic, $latest, $filterExpression);
                    $this->cacheAssignments[$topic] = $latest;
                } else {
                    $this->syncProcessQueue($topic, $latest, $filterExpression);
                }
            } catch (\Throwable $e) {
                Logger::error("Exception raised while scanning assignments, topic={$topic}, consumerGroup={$this->consumerGroup}, clientId={$this->clientId}, error={$e->getMessage()}");
            }
        }
    }

    /**
     * @throws ClientException|NetworkException If query fails
     */
    private function queryAssignment(string $topic): ?Assignments
    {
        $request = new QueryAssignmentRequest();
        $request->setTopic(Resource::create()->setName($topic));
        $request->setGroup(Resource::create()->setName($this->consumerGroup));

        $pool = ConnectionPool::getInstance();
        $client = $pool->getConnection($this->config);

        try {
            $call = $client->QueryAssignment($request);
            $response = $call->wait();

            if ($response instanceof QueryAssignmentResponse) {
                return $response->getAssignments();
            }
        } catch (\Throwable $e) {
            throw new NetworkException("Failed to query assignment for topic {$topic}: " . $e->getMessage(), 0, $e);
        }

        return null;
    }

    private function syncProcessQueue(string $topic, Assignments $assignments, FilterExpression $filterExpression): void
    {
        $latest = [];
        foreach ($assignments->getAssignmentList() as $assignment) {
            $mq = $assignment->getMessageQueue();
            $latest[$this->getMessageQueueKey($mq)] = $mq;
        }

        $activeMqs = [];

        foreach ($this->processQueueTable as $key => $pq) {
            $mq = $pq->getMessageQueue();
            $mqTopic = $mq->getTopic()->getName();

            if ($topic !== $mqTopic) {
                continue;
            }

            if (!isset($latest[$key])) {
                Logger::info("Drop process queue (no longer assigned), topic={$topic}, mq={$key}, clientId={$this->clientId}");
                $this->dropProcessQueue($key);
                continue;
            }

            if ($pq->expired()) {
                Logger::warn("Drop process queue (expired), topic={$topic}, mq={$key}, clientId={$this->clientId}");
                $this->dropProcessQueue($key);
                continue;
            }

            $activeMqs[$key] = true;
        }

        foreach ($latest as $key => $mq) {
            if (isset($activeMqs[$key])) {
                continue;
            }

            Logger::info("Create process queue for new assignment, topic={$topic}, mq={$key}, clientId={$this->clientId}");
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
     */
    public function doStats(): void
    {
        Logger::info(sprintf(
            "clientId=%s, consumerGroup=%s, receptionTimes=%d, receivedMessagesQuantity=%d, consumptionOkQuantity=%d, consumptionErrorQuantity=%d",
            $this->clientId,
            $this->consumerGroup,
            $this->receptionTimes,
            $this->receivedMessagesQuantity,
            $this->consumptionOkQuantity,
            $this->consumptionErrorQuantity
        ));

        $this->receptionTimes = 0;
        $this->receivedMessagesQuantity = 0;
        $this->consumptionOkQuantity = 0;
        $this->consumptionErrorQuantity = 0;
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
}
