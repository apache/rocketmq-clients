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

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/TelemetrySession.php';

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
    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $telemetrySession;
    private $subscriptions = [];
    private $isStarted = false;
    
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
        
        // Create gRPC client
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints);
    }
    
    /**
     * Subscribe to a topic
     * 
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->subscriptions[$topic] = $expression;
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
    }
    
    /**
     * Establish Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // Create UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // Create SubscriptionEntry list
        $subscriptionEntries = [];
        foreach ($this->subscriptions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);
            
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
        
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessages);
        
        $invisibleDurationObj = new Duration();
        $invisibleDurationObj->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);
        
        // Prepare metadata
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // Receive messages
        $call = $this->client->ReceiveMessage($request, $metadata);
        
        $messages = [];
        try {
            foreach ($call->responses() as $response) {
                if ($response->hasMessage()) {
                    $messages[] = $response->getMessage();
                }
                
                // Check if maximum count is reached
                if (count($messages) >= $maxMessages) {
                    break;
                }
            }
        } catch (\Exception $e) {
            // Ignore errors such as timeouts
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }
        
        return $messages;
    }
    
    /**
     * Get MessageQueue
     */
    private function getMessageQueue($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        try {
            list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
            
            if ($status->code !== 0) {
                return null;
            }
            
            $queues = $response->getMessageQueues();
            if (empty($queues)) {
                return null;
            }
            
            // Return the first available queue
            return $queues[0];
        } catch (\Exception $e) {
            return null;
        }
    }
    
    /**
     * Acknowledge messages
     *
     * @param array $receiptHandles Receipt handle list
     */
    public function ack($receiptHandles)
    {
        // TODO: Implement ACK logic
        // Need to use AckMessageRequest
    }
    
    /**
     * Shut down the consumer
     */
    public function shutdown()
    {
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isStarted = false;
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
     * Destructor
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
