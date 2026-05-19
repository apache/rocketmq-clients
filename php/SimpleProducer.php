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
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;

/**
 * SimpleProducer - Message producer
 *
 * Integrates TelemetrySession implementation:
 * 1. Singleton Stream management
 * 2. Sequential writes
 * 3. Backpressure handling
 */
class SimpleProducer
{
    private $client;
    private $endpoints;
    private $clientId;
    private $telemetrySession;
    private $topicPublishInfo = [];
    private $isStarted = false;
    
    /**
     * Constructor
     *
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        
        // Create gRPC client
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints);
    }
    
    /**
     * Start the Producer
     */
    public function start()
    {
        if ($this->isStarted) {
            return;
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
        
        // Create Publishing configuration
        $publishing = new Publishing();
        // Topics can be added here
        
        // Create Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);
        $settings->setPublishing($publishing);
        
        // Create TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Synchronously send Settings
        $success = $this->telemetrySession->syncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        
        // Wait for server processing
        usleep(500000); // 500ms
    }
    
    /**
     * Send message
     *
     * @param Message $message Message object
     * @return array Send result ['messageId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Producer not started");
        }
        
        // Query route
        $topic = $message->getTopic()->getName();
        $this->ensureTopicRoute($topic);
        
        // Create send request
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        // Prepare metadata
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // Send message
        list($response, $status) = $this->client->SendMessage($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Send message failed: " . $status->details);
        }
        
        // Parse response
        $entries = $response->getEntries();
        if (count($entries) > 0) {
            $entry = $entries[0];
            $resultStatus = $entry->getStatus();
            
            return [
                'messageId' => $entry->getMessageId(),
                'code' => $resultStatus->getCode(),
                'message' => $resultStatus->getMessage(),
            ];
        }
        
        throw new \RuntimeException("No response entries");
    }
    
    /**
     * Ensure Topic route is cached
     */
    private function ensureTopicRoute($topic)
    {
        if (isset($this->topicPublishInfo[$topic])) {
            return;
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }
        
        $this->topicPublishInfo[$topic] = [
            'queues' => $response->getMessageQueues(),
            'timestamp' => time(),
        ];
    }
    
    /**
     * Shutdown the Producer
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
     * Destructor
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
