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
use Apache\Rocketmq\Exception\ServerException;
use Apache\Rocketmq\Message\MessageView;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\ChangeInvisibleDurationRequest;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;

/**
 * Lite simple consumer implementation
 * 
 * Implements LiteSimpleConsumer interface for lite topics
 */
class LiteSimpleConsumerImpl implements LiteSimpleConsumer {
    /**
     * @var MessagingServiceClient|null gRPC client instance
     */
    private $client = null;
    
    /**
     * @var ClientConfiguration Client configuration
     */
    private $config;
    
    /**
     * @var string Consumer group name
     */
    private $consumerGroup;
    
    /**
     * @var array Lite topics and their filters
     */
    private $liteTopics = [];
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var int Max message num per receive
     */
    private $maxMessageNum = 32;
    
    /**
     * @var int Invisible duration (seconds)
     */
    private $invisibleDuration = 30;
    
    /**
     * @var int Long polling wait duration (seconds)
     */
    private $awaitDuration = 30;
    
    /**
     * @var string Client state: CREATED, STARTING, RUNNING, STOPPING, TERMINATED
     */
    private $state = 'CREATED';
    
    /**
     * Constructor
     * 
     * @param ClientConfiguration $config Client configuration
     * @param string $consumerGroup Consumer group name
     * @param int $maxMessageNum Max message num
     * @param int $invisibleDuration Invisible duration
     * @param int $awaitDuration Await duration
     */
    public function __construct(ClientConfiguration $config, string $consumerGroup, int $maxMessageNum = 32, int $invisibleDuration = 30, int $awaitDuration = 30) {
        $this->config = $config;
        $this->consumerGroup = $consumerGroup;
        $this->maxMessageNum = $maxMessageNum;
        $this->invisibleDuration = $invisibleDuration;
        $this->awaitDuration = $awaitDuration;
        $this->clientId = Util::generateClientId();
        
        // Set route cache configuration from client configuration
        $routeCache = \Apache\Rocketmq\RouteCache::getInstance();
        $routeCache->setConfigFromClientConfiguration($this->config);
    }
    
    /**
     * {@inheritdoc}
     */
    public function getConsumerGroup(): string {
        return $this->consumerGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function subscribeLite(string $liteTopic, string $filterExpression = '', string $filterType = 'TAG'): void {
        $this->subscribeLiteWithOffset($liteTopic, 'LATEST', $filterExpression, $filterType);
    }
    
    /**
     * {@inheritdoc}
     */
    public function subscribeLiteWithOffset(string $liteTopic, string $offsetOption, string $filterExpression = '', string $filterType = 'TAG'): void {
        if (empty($liteTopic)) {
            throw new \InvalidArgumentException("Lite topic cannot be empty");
        }
        
        $this->liteTopics[$liteTopic] = [
            'filterExpression' => $filterExpression,
            'filterType' => $filterType,
            'offsetOption' => $offsetOption
        ];
        
        if ($this->state === 'RUNNING') {
            // Re-register with the broker
            $this->sendHeartbeat();
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function unsubscribeLite(string $liteTopic): void {
        if (empty($liteTopic)) {
            throw new \InvalidArgumentException("Lite topic cannot be empty");
        }
        
        if (isset($this->liteTopics[$liteTopic])) {
            unset($this->liteTopics[$liteTopic]);
            
            if ($this->state === 'RUNNING') {
                // Re-register with the broker
                $this->sendHeartbeat();
            }
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function getLiteTopicSet(): array {
        return array_keys($this->liteTopics);
    }
    
    /**
     * {@inheritdoc}
     */
    public function receive(int $maxMessageNum, int $invisibleDuration, int $awaitDuration = 30): array {
        if ($this->state !== 'RUNNING') {
            throw new ClientStateException("Lite simple consumer is not running");
        }
        
        if (empty($this->liteTopics)) {
            throw new ClientException("No lite topic subscribed");
        }
        
        // Receive messages from all subscribed lite topics
        $allMessages = [];
        foreach ($this->liteTopics as $liteTopic => $topicConfig) {
            $messages = $this->receiveFromTopic($liteTopic, $maxMessageNum, $invisibleDuration, $awaitDuration);
            $allMessages = array_merge($allMessages, $messages);
        }
        
        return $allMessages;
    }
    
    /**
     * {@inheritdoc}
     */
    public function ack(MessageView $messageView): void {
        if ($this->state !== 'RUNNING') {
            throw new ClientStateException("Lite simple consumer is not running");
        }
        
        if (!$this->client) {
            throw new ClientStateException("Lite simple consumer is not started");
        }
        
        $entry = new AckMessageEntry();
        $entry->setTopic(Resource::create()->setName($messageView->getTopic()));
        $entry->setMessageId($messageView->getMessageId());
        $entry->setReceiptHandle($messageView->getReceiptHandle());
        
        $request = new AckMessageRequest();
        $request->setGroup(Resource::create()->setName($this->consumerGroup));
        $request->addEntries($entry);
        
        try {
            $this->client->AckMessage($request);
        } catch (\Exception $e) {
            throw new NetworkException("Failed to ack message: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function changeInvisibleDuration(MessageView $messageView, int $invisibleDuration): void {
        if ($this->state !== 'RUNNING') {
            throw new ClientStateException("Lite simple consumer is not running");
        }
        
        if (!$this->client) {
            throw new ClientStateException("Lite simple consumer is not started");
        }
        
        $request = new ChangeInvisibleDurationRequest();
        $request->setGroup(Resource::create()->setName($this->consumerGroup));
        $request->setTopic(Resource::create()->setName($messageView->getTopic()));
        $request->setMessageId($messageView->getMessageId());
        $request->setReceiptHandle($messageView->getReceiptHandle());
        $request->setInvisibleDuration($invisibleDuration);
        
        try {
            $this->client->ChangeInvisibleDuration($request);
        } catch (\Exception $e) {
            throw new NetworkException("Failed to change invisible duration: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function start(): void {
        if ($this->state === 'RUNNING') {
            return;
        }
        
        if (empty($this->liteTopics)) {
            throw new ClientException("No lite topic subscribed");
        }
        
        $this->state = 'STARTING';
        
        try {
            // Initialize gRPC client using connection pool
            $connectionPool = ConnectionPool::getInstance();
            $connectionPool->setConfigFromClientConfiguration($this->config);
            $this->client = $connectionPool->getConnection($this->config->getEndpoints());
            
            // Send heartbeat to register with broker
            $this->sendHeartbeat();
            
            $this->state = 'RUNNING';
        } catch (\Exception $e) {
            $this->state = 'CREATED';
            throw new ClientException("Failed to start lite simple consumer: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown(): void {
        if ($this->state === 'TERMINATED') {
            return;
        }
        
        $this->state = 'STOPPING';
        
        try {
            // Close gRPC client
            if ($this->client) {
                $connectionPool = ConnectionPool::getInstance();
                $connectionPool->returnConnection($this->config->getEndpoints(), $this->client);
                $this->client = null;
            }
            
            $this->state = 'TERMINATED';
        } catch (\Exception $e) {
            throw new ClientException("Failed to shutdown lite simple consumer: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function isRunning(): bool {
        return $this->state === 'RUNNING';
    }
    
    /**
     * Receive messages from a specific lite topic
     * 
     * @param string $liteTopic Lite topic name
     * @param int $maxMessageNum Max message num
     * @param int $invisibleDuration Invisible duration
     * @param int $awaitDuration Await duration
     * @return array List of message views
     * @throws ClientException If an error occurs
     */
    private function receiveFromTopic(string $liteTopic, int $maxMessageNum, int $invisibleDuration, int $awaitDuration): array {
        if (!$this->client) {
            throw new ClientStateException("Lite simple consumer is not started");
        }
        
        $request = new ReceiveMessageRequest();
        $request->setGroup(Resource::create()->setName($this->consumerGroup));
        $request->setTopic(Resource::create()->setName($liteTopic));
        $request->setMaxMessageNum($maxMessageNum);
        $request->setInvisibleDuration($invisibleDuration);
        $request->setAwaitDuration($awaitDuration);
        $request->setMessageType(MessageType::LITE);
        
        try {
            $response = $this->client->ReceiveMessage($request);
            $messages = [];
            foreach ($response->getMessages() as $message) {
                $messages[] = new MessageView($message);
            }
            return $messages;
        } catch (\Exception $e) {
            throw new NetworkException("Failed to receive messages: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * Send heartbeat to the broker
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    private function sendHeartbeat(): void {
        if (!$this->client) {
            throw new ClientStateException("Lite simple consumer is not started");
        }
        
        $request = new HeartbeatRequest();
        $request->setClientType('PHP');
        $request->setClientVersion('2.0.0');
        $request->setClientId($this->clientId);
        
        $subscription = new Subscription();
        $subscription->setGroup(Resource::create()->setName($this->consumerGroup));
        
        foreach ($this->liteTopics as $liteTopic => $topicConfig) {
            $entry = new SubscriptionEntry();
            $entry->setTopic(Resource::create()->setName($liteTopic));
            
            if (!empty($topicConfig['filterExpression'])) {
                $filterExpression = new FilterExpression();
                $filterExpression->setExpression($topicConfig['filterExpression']);
                $filterExpression->setType($topicConfig['filterType'] === 'SQL92' ? FilterType::SQL92 : FilterType::TAG);
                $entry->setFilterExpression($filterExpression);
            }
            
            $subscription->addEntries($entry);
        }
        
        $request->addSubscriptions($subscription);
        
        try {
            $this->client->Heartbeat($request);
        } catch (\Exception $e) {
            throw new NetworkException("Failed to send heartbeat: " . $e->getMessage(), $e);
        }
    }
}
