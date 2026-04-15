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

namespace Apache\Rocketmq\Producer;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\LiteTopicQuotaExceededException;
use Apache\Rocketmq\Message\MessageInterface;
use Apache\Rocketmq\Logger;

/**
 * Lite producer implementation
 * 
 * A simplified producer optimized for lite topics.
 * Lite topics are sub-topics under a parent topic that share resources.
 */
class LiteProducerImpl implements LiteProducer {
    /**
     * @var Producer Underlying producer instance
     */
    private $producer;
    
    /**
     * @var string Parent topic name
     */
    private $parentTopic;
    
    /**
     * @var ClientState Current state
     */
    private $state = ClientState::CREATED;
    
    /**
     * Constructor
     * 
     * @param ClientConfiguration $config Client configuration
     * @param string $parentTopic Parent topic name
     */
    public function __construct(ClientConfiguration $config, string $parentTopic) {
        $this->parentTopic = $parentTopic;
        
        // Create underlying producer for the parent topic
        $this->producer = Producer::getInstance($config, $parentTopic);
        
        Logger::info("Lite producer created for parent topic: {$parentTopic}");
    }
    
    /**
     * {@inheritdoc}
     */
    public function start(): void {
        ClientState::checkStateTransition($this->state, ClientState::CREATED, ClientState::STARTING);
        
        try {
            $this->producer->start();
            $this->state = ClientState::RUNNING;
            Logger::info("Lite producer started successfully, clientId={}", [$this->getClientId()]);
        } catch (\Exception $e) {
            $this->state = ClientState::FAILED;
            Logger::error("Failed to start lite producer, error={}", [$e->getMessage()]);
            throw $e;
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown(): void {
        ClientState::checkStateTransition($this->state, ClientState::RUNNING, ClientState::SHUTDOWN);
        
        try {
            $this->producer->shutdown();
            $this->state = ClientState::SHUTDOWN;
            Logger::info("Lite producer shutdown successfully, clientId={}", [$this->getClientId()]);
        } catch (\Exception $e) {
            Logger::error("Failed to shutdown lite producer, error={}", [$e->getMessage()]);
            throw $e;
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function send(MessageInterface $message): SendReceipt {
        ClientState::checkState($this->state, [ClientState::RUNNING], 'send message');
        
        // Validate that message has lite topic set
        $liteTopic = $message->getLiteTopic();
        if (empty($liteTopic)) {
            throw new ClientException("Lite topic must be set for lite messages. Use MessageBuilder::setLiteTopic().");
        }
        
        Logger::debug("Sending lite message, parentTopic={}, liteTopic={}, messageId={}", [
            $this->parentTopic,
            $liteTopic,
            $message->getMessageId() ?? 'unknown'
        ]);
        
        try {
            // Send using underlying producer
            $receipt = $this->producer->send($message);
            
            Logger::info("Lite message sent successfully, parentTopic={}, liteTopic={}, messageId={}", [
                $this->parentTopic,
                $liteTopic,
                $receipt->getMessageId()
            ]);
            
            return $receipt;
            
        } catch (ClientException $e) {
            // Check if it's a quota exceeded error
            if (strpos($e->getMessage(), 'quota') !== false || 
                strpos($e->getMessage(), 'LiteTopicQuotaExceeded') !== false) {
                
                Logger::error("Lite topic quota exceeded, parentTopic={}, liteTopic={}, error={}", [
                    $this->parentTopic,
                    $liteTopic,
                    $e->getMessage()
                ]);
                
                throw new LiteTopicQuotaExceededException(
                    "Lite topic quota exceeded: " . $e->getMessage(),
                    $liteTopic,
                    $e->getCode(),
                    $e
                );
            }
            
            Logger::error("Failed to send lite message, parentTopic={}, liteTopic={}, error={}", [
                $this->parentTopic,
                $liteTopic,
                $e->getMessage()
            ]);
            
            throw $e;
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function sendAsync(MessageInterface $message, ?callable $callback = null) {
        ClientState::checkState($this->state, [ClientState::RUNNING], 'send async message');
        
        // Validate that message has lite topic set
        $liteTopic = $message->getLiteTopic();
        if (empty($liteTopic)) {
            throw new ClientException("Lite topic must be set for lite messages. Use MessageBuilder::setLiteTopic().");
        }
        
        Logger::debug("Sending lite message asynchronously, parentTopic={}, liteTopic={}", [
            $this->parentTopic,
            $liteTopic
        ]);
        
        // Wrap callback to handle quota errors
        $wrappedCallback = null;
        if ($callback !== null) {
            $wrappedCallback = function($receipt, $error) use ($callback, $liteTopic) {
                if ($error !== null && 
                    (strpos($error->getMessage(), 'quota') !== false || 
                     strpos($error->getMessage(), 'LiteTopicQuotaExceeded') !== false)) {
                    
                    $quotaError = new LiteTopicQuotaExceededException(
                        "Lite topic quota exceeded: " . $error->getMessage(),
                        $liteTopic,
                        $error->getCode(),
                        $error
                    );
                    
                    call_user_func($callback, null, $quotaError);
                } else {
                    call_user_func($callback, $receipt, $error);
                }
            };
        }
        
        return $this->producer->sendAsync($message, $wrappedCallback);
    }
    
    /**
     * {@inheritdoc}
     */
    public function getClientId(): string {
        return $this->producer->getClientId();
    }
    
    /**
     * {@inheritdoc}
     */
    public function isRunning(): bool {
        return $this->state === ClientState::RUNNING;
    }
    
    /**
     * Get the parent topic name
     * 
     * @return string Parent topic name
     */
    public function getParentTopic(): string {
        return $this->parentTopic;
    }
}
