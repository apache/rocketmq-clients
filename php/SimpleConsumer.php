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
require_once __DIR__ . '/Signature.php';

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
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
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
    private $logger;
    private $credentials = null;
    private $namespace = '';
    
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
        $this->logger = Logger::getInstance('SimpleConsumer');
    }
    
    /**
     * Subscribe to a topic
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->subscriptions[$topic] = $expression;
        return $this;
    }

    /**
     * Unsubscribe from a topic
     *
     * @param string $topic Topic name
     * @return $this
     */
    public function unsubscribe($topic)
    {
        unset($this->subscriptions[$topic]);
        return $this;
    }

    /**
     * Get all subscription expressions
     *
     * @return array
     */
    public function getSubscriptionExpressions()
    {
        return $this->subscriptions;
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
     * @param array $messages List of message objects to acknowledge
     */
    public function ack($messages)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        if (empty($messages)) {
            return;
        }

        // Group messages by topic for batch ack
        $messagesByTopic = [];
        foreach ($messages as $message) {
            $topic = $this->extractTopic($message);
            if (!$topic) {
                continue;
            }
            if (!isset($messagesByTopic[$topic])) {
                $messagesByTopic[$topic] = [];
            }
            $messagesByTopic[$topic][] = $message;
        }

        foreach ($messagesByTopic as $topic => $topicMessages) {
            $this->ackMessagesForTopic($topic, $topicMessages);
        }
    }

    /**
     * Acknowledge messages for a specific topic
     */
    private function ackMessagesForTopic($topic, $messages)
    {
        $entries = [];
        foreach ($messages as $message) {
            $receiptHandle = $this->extractReceiptHandle($message);
            $messageId = $this->extractMessageId($message);

            if (!$receiptHandle) {
                $this->logger->warning("Skip ack: no receipt handle for message");
                continue;
            }

            $entry = new AckMessageEntry();
            if ($messageId) {
                $entry->setMessageId($messageId);
            }
            $entry->setReceiptHandle($receiptHandle);
            $entries[] = $entry;
        }

        if (empty($entries)) {
            return;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries($entries);

        $metadata = $this->buildMetadata();

        $maxRetries = 3;
        $attempt = 0;

        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("AckMessage attempt {$attempt}: status error: " . $status->details);
                } else {
                    $responseEntries = $response->getEntries();
                    if (!empty($responseEntries)) {
                        foreach ($responseEntries as $resultEntry) {
                            if ($resultEntry->hasStatus()) {
                                $entryCode = $resultEntry->getStatus()->getCode();
                                if ($entryCode !== 20000) {
                                    if ($entryCode == 40003) {
                                        $this->logger->warning("AckMessage invalid receipt handle, giving up");
                                    } else {
                                        $this->logger->warning("AckMessage entry failed with code: " . $entryCode);
                                    }
                                }
                            }
                        }
                    }
                }
                return;
            } catch (\Exception $e) {
                $this->logger->warning("AckMessage attempt {$attempt} failed: " . $e->getMessage());
                $attempt++;
                if ($attempt < $maxRetries) {
                    usleep(pow(2, $attempt) * 100000);
                }
            }
        }

        $this->logger->error("AckMessage failed after {$maxRetries} retries for topic={$topic}");
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
     * Extract receipt handle from a message
     */
    private function extractReceiptHandle($message)
    {
        if (method_exists($message, 'getSystemProperties')) {
            $sysProps = $message->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
        }
        return null;
    }

    /**
     * Extract message ID from a message
     */
    private function extractMessageId($message)
    {
        if (method_exists($message, 'getSystemProperties')) {
            $sysProps = $message->getSystemProperties();
            if (method_exists($sysProps, 'getMessageId')) {
                return $sysProps->getMessageId();
            }
        }
        return null;
    }

    /**
     * Extract topic from a message
     */
    private function extractTopic($message)
    {
        if (method_exists($message, 'getTopic')) {
            $topic = $message->getTopic();
            if (method_exists($topic, 'getName')) {
                return $topic->getName();
            }
        }
        return null;
    }

    /**
     * Build metadata for gRPC calls
     */
    private function buildMetadata()
    {
        return Signature::sign(
            null,
            $this->clientId,
            'PHP',
            '5.0.0',
            '',
            'v2'
        );
    }
    
    /**
     * Shut down the consumer
     */
    public function shutdown()
    {
        if ($this->telemetrySession) {
            $this->notifyClientTermination();
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
