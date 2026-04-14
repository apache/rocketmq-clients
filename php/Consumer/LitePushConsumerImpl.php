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
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\Util;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;

/**
 * Lite push consumer implementation
 * 
 * Implements LitePushConsumer interface for lite topics
 */
class LitePushConsumerImpl implements LitePushConsumer {
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
     * @var array Lite topics and their listeners
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
     * @var callable Default message listener
     */
    private $defaultMessageListener;
    
    /**
     * @var int Thread pool size
     */
    private $threadPoolSize = 1;
    
    /**
     * @var string Client state: CREATED, STARTING, RUNNING, STOPPING, TERMINATED
     */
    private $state = 'CREATED';
    
    /**
     * @var array Running threads
     */
    private $runningThreads = [];
    
    /**
     * Constructor
     * 
     * @param ClientConfiguration $config Client configuration
     * @param string $consumerGroup Consumer group name
     * @param callable|null $defaultMessageListener Default message listener
     * @param int $threadPoolSize Thread pool size
     * @param int $maxMessageNum Max message num
     * @param int $invisibleDuration Invisible duration
     * @param int $awaitDuration Await duration
     */
    public function __construct(ClientConfiguration $config, string $consumerGroup, callable $defaultMessageListener = null, int $threadPoolSize = 1, int $maxMessageNum = 32, int $invisibleDuration = 30, int $awaitDuration = 30) {
        $this->config = $config;
        $this->consumerGroup = $consumerGroup;
        $this->defaultMessageListener = $defaultMessageListener;
        $this->threadPoolSize = $threadPoolSize;
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
    public function subscribeLite(string $liteTopic, callable $messageListener, string $filterExpression = '', string $filterType = 'TAG'): void {
        $this->subscribeLiteWithOffset($liteTopic, $messageListener, 'LATEST', $filterExpression, $filterType);
    }
    
    /**
     * {@inheritdoc}
     */
    public function subscribeLiteWithOffset(string $liteTopic, callable $messageListener, string $offsetOption, string $filterExpression = '', string $filterType = 'TAG'): void {
        if (empty($liteTopic)) {
            throw new \InvalidArgumentException("Lite topic cannot be empty");
        }
        
        $this->liteTopics[$liteTopic] = [
            'listener' => $messageListener,
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
    public function getConsumerGroup(): string {
        return $this->consumerGroup;
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
        
        Logger::info("Begin to start the rocketmq lite push consumer, clientId={$this->clientId}");
        
        try {
            // Initialize gRPC client using connection pool
            $connectionPool = ConnectionPool::getInstance();
            $connectionPool->setConfigFromClientConfiguration($this->config);
            $this->client = $connectionPool->getConnection($this->config);
            
            // Send heartbeat to register with broker
            $this->sendHeartbeat();
            
            // Start message consumption threads
            for ($i = 0; $i < $this->threadPoolSize; $i++) {
                $thread = new \Thread(function () {
                    $this->consumeLoop();
                });
                $thread->start();
                $this->runningThreads[] = $thread;
            }
            
            $this->state = 'RUNNING';
            Logger::info("The rocketmq lite push consumer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            $this->state = 'CREATED';
            Logger::error("Failed to start the rocketmq lite push consumer, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            throw new ClientException("Failed to start lite push consumer: " . $e->getMessage(), $e);
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
        
        Logger::info("Begin to shutdown the rocketmq lite push consumer, clientId={}", [$this->clientId]);
        
        try {
            // Stop all running threads
            foreach ($this->runningThreads as $thread) {
                $thread->join();
            }
            $this->runningThreads = [];
            
            // Close gRPC client
            if ($this->client) {
                $connectionPool = ConnectionPool::getInstance();
                $connectionPool->returnConnection($this->config, $this->client);
                $this->client = null;
            }
            
            $this->state = 'TERMINATED';
            Logger::info("Shutdown the rocketmq lite push consumer successfully, clientId={}", [$this->clientId]);
        } catch (\Exception $e) {
            Logger::error("Failed to shutdown the rocketmq lite push consumer, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
            throw new ClientException("Failed to shutdown lite push consumer: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function isRunning(): bool {
        return $this->state === 'RUNNING';
    }
    
    /**
     * Message consumption loop
     * 
     * @return void
     */
    private function consumeLoop(): void {
        while ($this->state === 'RUNNING') {
            try {
                foreach ($this->liteTopics as $liteTopic => $topicConfig) {
                    $messages = $this->receiveMessages($liteTopic);
                    foreach ($messages as $message) {
                        $this->processMessage($message, $topicConfig['listener']);
                    }
                }
            } catch (\Exception $e) {
                // Log error and continue
                Logger::error("Lite push consumer error, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
                usleep(1000000); // 1 second
            }
        }
    }
    
    /**
     * Receive messages from the server
     * 
     * @param string $liteTopic Lite topic name
     * @return array List of message views
     * @throws ClientException If an error occurs
     */
    private function receiveMessages(string $liteTopic): array {
        if (!$this->client) {
            throw new ClientStateException("Lite push consumer is not started");
        }
        
        $request = new ReceiveMessageRequest();
        $request->setGroup(Resource::create()->setName($this->consumerGroup));
        $request->setTopic(Resource::create()->setName($liteTopic));
        $request->setMaxMessageNum($this->maxMessageNum);
        $request->setInvisibleDuration($this->invisibleDuration);
        $request->setAwaitDuration($this->awaitDuration);
        $request->setMessageType(MessageType::LITE);
        
        try {
            $response = $this->client->ReceiveMessage($request);
            $messages = [];
            foreach ($response->getMessages() as $message) {
                $messages[] = new \Apache\Rocketmq\Message\MessageView($message);
            }
            return $messages;
        } catch (\Exception $e) {
            throw new NetworkException("Failed to receive messages: " . $e->getMessage(), $e);
        }
    }
    
    /**
     * Process a message
     * 
     * @param \Apache\Rocketmq\Message\MessageView $message Message view
     * @param callable $listener Message listener
     * @return void
     */
    private function processMessage(\Apache\Rocketmq\Message\MessageView $message, callable $listener): void {
        try {
            $result = $listener($message);
            
            if ($result === ConsumeResult::SUCCESS) {
                $this->ackMessage($message);
            } else {
                // Message processing failed, no need to ack
            }
        } catch (\Exception $e) {
            // Message processing failed, no need to ack
            Logger::error(
                "Message listener raised an exception while consuming messages, messageId={}, topic={}, clientId={}",
                [$message->getMessageId(), $message->getTopic(), $this->clientId, 'error' => $e->getMessage()]
            );
        }
    }
    
    /**
     * Acknowledge a message
     * 
     * @param \Apache\Rocketmq\Message\MessageView $message Message view
     * @return void
     * @throws ClientException If an error occurs
     */
    private function ackMessage(\Apache\Rocketmq\Message\MessageView $message): void {
        if (!$this->client) {
            throw new ClientStateException("Lite push consumer is not started");
        }
        
        $entry = new AckMessageEntry();
        $entry->setTopic(Resource::create()->setName($message->getTopic()));
        $entry->setMessageId($message->getMessageId());
        $entry->setReceiptHandle($message->getReceiptHandle());
        
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
     * Send heartbeat to the broker
     * 
     * @return void
     * @throws ClientException If an error occurs
     */
    private function sendHeartbeat(): void {
        if (!$this->client) {
            throw new ClientStateException("Lite push consumer is not started");
        }
        
        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::PUSH_CONSUMER);
        
        // Set group
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $request->setGroup($group);
        
        try {
            $this->client->Heartbeat($request);
        } catch (\Exception $e) {
            throw new NetworkException("Failed to send heartbeat: " . $e->getMessage(), $e);
        }
    }
}
