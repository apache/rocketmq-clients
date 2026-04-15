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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\ClientStateException;
use Apache\Rocketmq\Exception\NetworkException;
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
    /**
     * @var ClientConfiguration Client configuration
     */
    private $config;
    
    /**
     * @var string Consumer group name
     */
    private $consumerGroup;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var array<string, FilterExpression> Subscription expressions
     */
    private $subscriptionExpressions = [];
    
    /**
     * @var array<string, Assignments> Cached assignments per topic
     */
    private $cacheAssignments = [];
    
    /**
     * @var callable Message listener
     */
    private $messageListener;
    
    /**
     * @var int Maximum cache message count
     */
    private $maxCacheMessageCount;
    
    /**
     * @var int Maximum cache message size in bytes
     */
    private $maxCacheMessageSizeInBytes;
    
    /**
     * @var bool Enable FIFO consume accelerator
     */
    private $enableFifoConsumeAccelerator;
    
    /**
     * @var array<string, ProcessQueue> Process queue table
     */
    private $processQueueTable = [];
    
    /**
     * @var int Consumption thread count
     */
    private $consumptionThreadCount;
    
    /**
     * @var string Client state: CREATED, STARTING, RUNNING, STOPPING, TERMINATED
     */
    private $state = 'CREATED';
    
    /**
     * @var bool Whether running
     */
    private $running = false;
    
    /**
     * @var int Reception times counter
     */
    private $receptionTimes = 0;
    
    /**
     * @var int Received messages quantity counter
     */
    private $receivedMessagesQuantity = 0;
    
    /**
     * @var int Consumption OK quantity counter
     */
    private $consumptionOkQuantity = 0;
    
    /**
     * @var int Consumption error quantity counter
     */
    private $consumptionErrorQuantity = 0;
    
    /**
     * Constructor
     * 
     * @param ClientConfiguration $config Client configuration
     * @param string $consumerGroup Consumer group name
     * @param array<string, FilterExpression> $subscriptionExpressions Subscription expressions
     * @param callable $messageListener Message listener
     * @param int $maxCacheMessageCount Maximum cache message count
     * @param int $maxCacheMessageSizeInBytes Maximum cache message size in bytes
     * @param int $consumptionThreadCount Consumption thread count
     * @param bool $enableFifoConsumeAccelerator Enable FIFO consume accelerator
     */
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
        
        // Set route cache configuration
        $routeCache = \Apache\Rocketmq\RouteCache::getInstance();
        $routeCache->setConfigFromClientConfiguration($this->config);
    }
    
    /**
     * {@inheritdoc}
     */
    public function getConsumerGroup(): string
    {
        return $this->consumerGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getSubscriptionExpressions(): array
    {
        return $this->subscriptionExpressions;
    }
    
    /**
     * {@inheritdoc}
     */
    public function subscribe(string $topic, FilterExpression $filterExpression): PushConsumer
    {
        $this->checkRunning();
        
        // Get route data for the topic
        $this->getRouteData($topic);
        
        $this->subscriptionExpressions[$topic] = $filterExpression;
        
        return $this;
    }
    
    /**
     * {@inheritdoc}
     */
    public function unsubscribe(string $topic): PushConsumer
    {
        $this->checkRunning();
        
        unset($this->subscriptionExpressions[$topic]);
        unset($this->cacheAssignments[$topic]);
        
        // Drop process queues for this topic
        $this->dropProcessQueuesByTopic($topic);
        
        return $this;
    }
    
    /**
     * Start the push consumer
     * 
     * @return void
     * @throws ClientException If startup fails
     */
    public function start(): void
    {
        if ($this->state !== 'CREATED') {
            throw new ClientStateException("Consumer is already {$this->state}");
        }
        
        $this->state = 'STARTING';
        
        try {
            // Send heartbeat to register with broker
            $this->heartbeat();
            
            $this->state = 'RUNNING';
            $this->running = true;
            
            // Start assignment scanning in background
            $this->startAssignmentScanning();
            
        } catch (\Exception $e) {
            $this->state = 'FAILED';
            throw new ClientException("Failed to start push consumer: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function close()
    {
        if ($this->state === 'TERMINATED' || $this->state === 'STOPPING') {
            return;
        }
        
        $this->state = 'STOPPING';
        $this->running = false;
        
        // Wait for inflight requests to complete
        $this->waitingReceiveRequestFinished();
        
        // Drop all process queues
        foreach ($this->processQueueTable as $mq => $pq) {
            $pq->drop();
        }
        $this->processQueueTable = [];
        
        $this->state = 'TERMINATED';
    }
    
    /**
     * Check if consumer is running
     * 
     * @return void
     * @throws ClientStateException If not running
     */
    private function checkRunning(): void
    {
        if ($this->state !== 'RUNNING') {
            throw new ClientStateException("Consumer is not running (current state: {$this->state})");
        }
    }
    
    /**
     * Send heartbeat to broker
     * 
     * @return void
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
        
        $call = $client->Heartbeat($request);
        $call->wait();
    }
    
    /**
     * Get route data for topic
     * 
     * @param string $topic Topic name
     * @return void
     */
    private function getRouteData(string $topic): void
    {
        // TODO: Implement route data fetching
        // This should query the name server for topic route information
    }
    
    /**
     * Start assignment scanning periodically
     * 
     * @return void
     */
    private function startAssignmentScanning(): void
    {
        // In PHP, we'll use a simple loop with pcntl_fork or run in background
        // For now, we'll use a simple approach
        $this->scanAssignments();
    }
    
    /**
     * Scan assignments for all subscribed topics
     * 
     * @return void
     */
    public function scanAssignments(): void
    {
        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            try {
                $existed = $this->cacheAssignments[$topic] ?? null;
                $latest = $this->queryAssignment($topic);
                
                if ($latest === null || empty($latest->getAssignmentList())) {
                    if ($existed === null || empty($existed->getAssignmentList())) {
                        continue;
                    }
                }
                
                if ($latest !== $existed) {
                    $this->syncProcessQueue($topic, $latest, $filterExpression);
                    $this->cacheAssignments[$topic] = $latest;
                } else {
                    // Process queue may be dropped, need to sync anyway
                    $this->syncProcessQueue($topic, $latest, $filterExpression);
                }
            } catch (\Exception $e) {
                error_log("Exception raised while scanning assignments for topic {$topic}: " . $e->getMessage());
            }
        }
    }
    
    /**
     * Query assignment from broker
     * 
     * @param string $topic Topic name
     * @return Assignments|null Assignments
     * @throws ClientException If query fails
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
        } catch (\Exception $e) {
            throw new NetworkException("Failed to query assignment: " . $e->getMessage(), 0, $e);
        }
        
        return null;
    }
    
    /**
     * Sync process queue with assignments
     * 
     * @param string $topic Topic name
     * @param Assignments $assignments Assignments
     * @param FilterExpression $filterExpression Filter expression
     * @return void
     */
    private function syncProcessQueue(string $topic, Assignments $assignments, FilterExpression $filterExpression): void
    {
        $latest = [];
        foreach ($assignments->getAssignmentList() as $assignment) {
            $mq = $assignment->getMessageQueue();
            $latest[$this->getMessageQueueKey($mq)] = $mq;
        }
        
        $activeMqs = [];
        
        // Remove process queues that are no longer assigned or expired
        foreach ($this->processQueueTable as $key => $pq) {
            $mq = $pq->getMessageQueue();
            $mqTopic = $mq->getTopic()->getName();
            
            if ($topic !== $mqTopic) {
                continue;
            }
            
            if (!isset($latest[$key])) {
                $this->dropProcessQueue($key);
                continue;
            }
            
            if ($pq->expired()) {
                $this->dropProcessQueue($key);
                continue;
            }
            
            $activeMqs[$key] = true;
        }
        
        // Create process queues for new assignments
        foreach ($latest as $key => $mq) {
            if (isset($activeMqs[$key])) {
                continue;
            }
            
            $processQueue = $this->createProcessQueue($mq, $filterExpression);
            if ($processQueue !== null) {
                $processQueue->fetchMessageImmediately();
            }
        }
    }
    
    /**
     * Create a new process queue
     * 
     * @param MessageQueue $mq Message queue
     * @param FilterExpression $filterExpression Filter expression
     * @return ProcessQueue|null Process queue
     */
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
    
    /**
     * Drop a process queue
     * 
     * @param string $key Process queue key
     * @return void
     */
    private function dropProcessQueue(string $key): void
    {
        if (isset($this->processQueueTable[$key])) {
            $pq = $this->processQueueTable[$key];
            $pq->drop();
            unset($this->processQueueTable[$key]);
        }
    }
    
    /**
     * Drop all process queues for a topic
     * 
     * @param string $topic Topic name
     * @return void
     */
    private function dropProcessQueuesByTopic(string $topic): void
    {
        foreach ($this->processQueueTable as $key => $pq) {
            $mq = $pq->getMessageQueue();
            if ($mq->getTopic()->getName() === $topic) {
                $this->dropProcessQueue($key);
            }
        }
    }
    
    /**
     * Get message queue unique key
     * 
     * @param MessageQueue $mq Message queue
     * @return string Unique key
     */
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
     * @return void
     */
    private function waitingReceiveRequestFinished(): void
    {
        $maxWaitingTime = 35000; // 35 seconds (request timeout + long polling timeout)
        $endTime = microtime(true) * 1000 + $maxWaitingTime;
        
        while (true) {
            // In PHP, we don't have direct access to inflight count
            // Just wait a short time
            usleep(100000); // 100ms
            
            if (microtime(true) * 1000 > $endTime) {
                break;
            }
        }
    }
    
    /**
     * Get process queue size
     * 
     * @return int Process queue count
     */
    public function getQueueSize(): int
    {
        return count($this->processQueueTable);
    }
    
    /**
     * Get cache message count threshold per queue
     * 
     * @return int Threshold
     */
    public function cacheMessageCountThresholdPerQueue(): int
    {
        $size = $this->getQueueSize();
        if ($size <= 0) {
            return 0;
        }
        return max(1, (int)($this->maxCacheMessageCount / $size));
    }
    
    /**
     * Get cache message size threshold per queue
     * 
     * @return int Threshold in bytes
     */
    public function cacheMessageSizeThresholdPerQueue(): int
    {
        $size = $this->getQueueSize();
        if ($size <= 0) {
            return 0;
        }
        return max(1, (int)($this->maxCacheMessageSizeInBytes / $size));
    }
    
    /**
     * Get message listener
     * 
     * @return callable Message listener
     */
    public function getMessageListener(): callable
    {
        return $this->messageListener;
    }
    
    /**
     * Get consumption thread count
     * 
     * @return int Thread count
     */
    public function getConsumptionThreadCount(): int
    {
        return $this->consumptionThreadCount;
    }
    
    /**
     * Check if FIFO consume accelerator is enabled
     * 
     * @return bool Whether enabled
     */
    public function isEnableFifoConsumeAccelerator(): bool
    {
        return $this->enableFifoConsumeAccelerator;
    }
    
    /**
     * Increment reception times
     * 
     * @return void
     */
    public function incrementReceptionTimes(): void
    {
        $this->receptionTimes++;
    }
    
    /**
     * Increment received messages quantity
     * 
     * @param int $count Message count
     * @return void
     */
    public function incrementReceivedMessagesQuantity(int $count): void
    {
        $this->receivedMessagesQuantity += $count;
    }
    
    /**
     * Increment consumption OK quantity
     * 
     * @param int $count Count
     * @return void
     */
    public function incrementConsumptionOkQuantity(int $count): void
    {
        $this->consumptionOkQuantity += $count;
    }
    
    /**
     * Increment consumption error quantity
     * 
     * @param int $count Count
     * @return void
     */
    public function incrementConsumptionErrorQuantity(int $count): void
    {
        $this->consumptionErrorQuantity += $count;
    }
    
    /**
     * Do stats logging
     * 
     * @return void
     */
    public function doStats(): void
    {
        error_log(sprintf(
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
}
