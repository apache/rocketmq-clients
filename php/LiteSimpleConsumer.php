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
require_once __DIR__ . '/RpcClientManager.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/SwooleCompat.php';
require_once __DIR__ . '/OffsetOption.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\ChangeInvisibleDurationRequest;
use Apache\Rocketmq\V2\SyncLiteSubscriptionRequest;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Apache\Rocketmq\V2\LiteSubscriptionAction;
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
 * LiteSimpleConsumer - Simple (pull) consumer for lite topics.
 *
 * Extends the simple consumer with lite topic subscription management.
 * Uses a parent topic and dynamically subscribes/unsubscribes lite topics.
 *
 * Usage:
 *   $consumer = new LiteSimpleConsumer($endpoints, $consumerGroup, $parentTopic);
 *   $consumer->subscribeLite('lite-topic-1');
 *   $consumer->subscribeLite('lite-topic-2');
 *   $consumer->start();
 *   $messages = $consumer->receive(10, 30);
 */
class LiteSimpleConsumer
{
    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $parentTopic;
    private $liteTopics = [];
    private $telemetrySession;
    private $isRunning = false;
    private $awaitDuration = 30;
    private $receiveBatchSize = 32;
    private $liteSubscriptionQuota = 0;
    private $maxLiteTopicSize = 64;
    private $tlsCredentials = null;
    private $logger;

    /**
     * Constructor.
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param string $parentTopic Parent topic
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $consumerGroup, $parentTopic, $options = [])
    {
        $this->endpoints = $endpoints;
        if (empty($consumerGroup)) {
            throw new \InvalidArgumentException("LiteSimpleConsumer consumerGroup cannot be empty");
        }
        $this->consumerGroup = $consumerGroup;
        if (empty(trim($parentTopic))) {
            throw new \InvalidArgumentException("LiteSimpleConsumer parentTopic cannot be empty");
        }
        $this->parentTopic = $parentTopic;
        $this->clientId = $options['clientId'] ?? ('php-lite-simple-consumer-' . getmypid() . '-' . time());
        $this->awaitDuration = $options['awaitDuration'] ?? 30;
        $this->receiveBatchSize = $options['receiveBatchSize'] ?? 32;
        $this->liteSubscriptionQuota = $options['liteSubscriptionQuota'] ?? 0;
        $this->maxLiteTopicSize = $options['maxLiteTopicSize'] ?? 64;

        $this->logger = Logger::getInstance('LiteSimpleConsumer');

        // Use RpcClientManager for connection pooling
        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);

        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId);
    }

    /**
     * Subscribe to a lite topic.
     *
     * @param string $liteTopic Lite topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribeLite($liteTopic, $expression = '*')
    {
        $this->checkNotRunning();

        if (empty(trim($liteTopic))) {
            throw new \InvalidArgumentException("LiteSimpleConsumer liteTopic cannot be empty");
        }
        if (strlen($liteTopic) > $this->maxLiteTopicSize) {
            throw new \RuntimeException("Lite topic name exceeds max length of {$this->maxLiteTopicSize}");
        }

        if ($this->liteSubscriptionQuota > 0 && count($this->liteTopics) >= $this->liteSubscriptionQuota) {
            throw new \RuntimeException("Lite subscription quota exceeded: {$this->liteSubscriptionQuota}");
        }

        $this->liteTopics[$liteTopic] = $expression;

        return $this;
    }

    /**
     * Unsubscribe from a lite topic.
     *
     * @param string $liteTopic
     * @return $this
     */
    public function unsubscribeLite($liteTopic)
    {
        $this->checkNotRunning();
        unset($this->liteTopics[$liteTopic]);
        return $this;
    }

    /**
     * Get subscribed lite topics.
     *
     * @return array
     */
    public function getLiteTopics()
    {
        return array_keys($this->liteTopics);
    }

    /**
     * Start the consumer.
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }

        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LiteSimpleConsumer has no lite topics subscribed");
        }

        $this->logger->info("LiteSimpleConsumer starting, clientId={$this->clientId}");

        $this->establishTelemetrySession();

        $this->syncLiteSubscriptions();

        $this->isRunning = true;

        $this->logger->info("LiteSimpleConsumer started successfully, clientId={$this->clientId}");
    }

    /**
     * Shutdown the consumer.
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }

        $this->isRunning = false;

        // Notify server of client termination
        $this->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->logger->info("LiteSimpleConsumer shutdown complete, clientId={$this->clientId}");
    }

    /**
     * Receive messages from subscribed lite topics.
     *
     * @param int $maxMessageCount Maximum messages to receive
     * @param int $invisibleDurationSeconds Invisible duration in seconds
     * @param OffsetOption|null $offsetOption Optional offset policy
     * @return array Array of Message objects
     */
    public function receive($maxMessageCount = 1, $invisibleDurationSeconds = 30, $offsetOption = null)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("LiteSimpleConsumer is not running");
        }

        if (empty($this->liteTopics)) {
            return [];
        }

        $allMessages = [];

        foreach ($this->liteTopics as $liteTopic => $expression) {
            $messages = $this->receiveMessagesFromLiteTopic($liteTopic, $expression, $maxMessageCount, $invisibleDurationSeconds, $offsetOption);
            $allMessages = array_merge($allMessages, $messages);

            if (count($allMessages) >= $maxMessageCount) {
                break;
            }
        }

        return $allMessages;
    }

    /**
     * Receive messages with an explicit offset option.
     *
     * @param OffsetOption $offsetOption Offset policy
     * @param int $maxMessageCount Maximum messages to receive
     * @param int $invisibleDurationSeconds Invisible duration in seconds
     * @return array Array of Message objects
     */
    public function receiveWithOffset(OffsetOption $offsetOption, $maxMessageCount = 1, $invisibleDurationSeconds = 30)
    {
        return $this->receive($maxMessageCount, $invisibleDurationSeconds, $offsetOption);
    }

    /**
     * Asynchronously receive messages via Swoole coroutine.
     *
     * @param int $maxMessageCount Maximum messages to receive
     * @param int $invisibleDurationSeconds Invisible duration in seconds
     * @param OffsetOption|null $offsetOption Optional offset policy
     * @return \Generator|array
     */
    public function receiveAsync($maxMessageCount = 1, $invisibleDurationSeconds = 30, $offsetOption = null)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $maxMessageCount, $invisibleDurationSeconds, $offsetOption, $channel) {
                try {
                    $messages = $self->receive($maxMessageCount, $invisibleDurationSeconds, $offsetOption);
                    $channel->push(['result' => $messages]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'];
        }
        yield $this->receive($maxMessageCount, $invisibleDurationSeconds, $offsetOption);
    }

    /**
     * Asynchronously acknowledge a message via Swoole coroutine.
     *
     * @param object $message Message object
     * @return \Generator
     */
    public function ackAsync($message)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $message, $channel) {
                try {
                    $success = $self->ack($message);
                    $channel->push(['success' => $success]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        yield $this->ack($message);
    }

    /**
     * Asynchronously change invisible duration via Swoole coroutine.
     *
     * @param object $message Message object
     * @param int $invisibleDurationSeconds New invisible duration
     * @return \Generator
     */
    public function changeInvisibleDurationAsync($message, $invisibleDurationSeconds)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $message, $invisibleDurationSeconds, $channel) {
                try {
                    $success = $self->changeInvisibleDuration($message, $invisibleDurationSeconds);
                    $channel->push(['success' => $success]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            $data = $channel->pop();
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        yield $this->changeInvisibleDuration($message, $invisibleDurationSeconds);
    }

    /**
     * ACK a message (lite-aware).
     *
     * @param object $message Message object
     * @return bool
     */
    public function ack($message)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("LiteSimpleConsumer is not running");
        }

        $receiptHandle = $this->extractReceiptHandle($message);
        $messageId = $this->extractMessageId($message);
        $topic = $this->extractTopic($message);

        if (!$receiptHandle) {
            $this->logger->warning("LiteSimpleConsumer ack: no receipt handle, skipping");
            return false;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $entry = new AckMessageEntry();
        if ($messageId) {
            $entry->setMessageId($messageId);
        }
        $entry->setReceiptHandle($receiptHandle);

        // Lite consumers include liteTopic in ack request
        $liteTopic = $this->extractLiteTopic($message);
        if ($liteTopic) {
            $entry->setLiteTopic($liteTopic);
        }

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries([$entry]);

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();
            if ($status->code !== 0) {
                $this->logger->warning("LiteSimpleConsumer ack failed: " . $status->details);
                return false;
            }
            $this->logger->debug("LiteSimpleConsumer ack success for messageId={$messageId}");
            return true;
        } catch (\Exception $e) {
            $this->logger->error("LiteSimpleConsumer ack exception: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Change invisible duration (lite-aware).
     *
     * @param object $message Message object
     * @param int $invisibleDurationSeconds New invisible duration
     * @return bool
     */
    public function changeInvisibleDuration($message, $invisibleDurationSeconds)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("LiteSimpleConsumer is not running");
        }

        $receiptHandle = $this->extractReceiptHandle($message);
        $messageId = $this->extractMessageId($message);
        $topic = $this->extractTopic($message);

        if (!$receiptHandle) {
            $this->logger->warning("LiteSimpleConsumer changeInvisibleDuration: no receipt handle, skipping");
            return false;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $duration = new Duration();
        $duration->setSeconds($invisibleDurationSeconds);
        $duration->setNanos(0);

        $request = new ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);
        if ($messageId) {
            $request->setMessageId($messageId);
        }

        // Lite consumers include liteTopic and set suspend flag
        $liteTopic = $this->extractLiteTopic($message);
        if ($liteTopic) {
            $request->setLiteTopic($liteTopic);
            $request->setSuspend(true);
        }

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->ChangeInvisibleDuration($request, $metadata)->wait();
            if ($status->code !== 0) {
                $this->logger->warning("LiteSimpleConsumer changeInvisibleDuration failed: " . $status->details);
                return false;
            }
            $this->logger->debug("LiteSimpleConsumer changeInvisibleDuration success for messageId={$messageId}");
            return true;
        } catch (\Exception $e) {
            $this->logger->error("LiteSimpleConsumer changeInvisibleDuration exception: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Sync lite subscriptions to server.
     */
    private function syncLiteSubscriptions()
    {
        if (empty($this->liteTopics)) {
            return;
        }

        $topicResource = new Resource();
        $topicResource->setName($this->parentTopic);

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $request = new SyncLiteSubscriptionRequest();
        $request->setAction(LiteSubscriptionAction::PARTIAL_ADD);
        $request->setTopic($topicResource);
        $request->setGroup($groupResource);
        $request->setLiteTopicSet(array_keys($this->liteTopics));

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->SyncLiteSubscription($request, $metadata)->wait();
            if ($status->code !== 0) {
                $this->logger->error("SyncLiteSubscription failed: " . $status->details);
            } else {
                $this->logger->info("SyncLiteSubscription success for " . count($this->liteTopics) . " lite topics");
            }
        } catch (\Exception $e) {
            $this->logger->warning("SyncLiteSubscription exception: " . $e->getMessage());
        }
    }

    /**
     * Receive messages from a specific lite topic.
     */
    private function receiveMessagesFromLiteTopic($liteTopic, $expression, $maxCount, $invisibleDuration, $offsetOption = null)
    {
        $topicResource = new Resource();
        $topicResource->setName($this->parentTopic);

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);

        $subscriptionEntry = new SubscriptionEntry();
        $subscriptionEntry->setTopic($topicResource);
        $subscriptionEntry->setExpression($filterExpression);

        $request = new ReceiveMessageRequest();
        $request->setSubscriptions([$subscriptionEntry]);

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request->setGroup($groupResource);

        $request->setAutoRenew(false);
        $request->setBatchSize($this->receiveBatchSize);
        $request->setInvisibleDuration($invisibleDuration * 1000);

        // Wire OffsetOption into the request
        if ($offsetOption instanceof OffsetOption) {
            if ($offsetOption->isPolicy()) {
                $policy = $offsetOption->getValue();
                if ($policy === OffsetOption::POLICY_MIN_VALUE) {
                    $request->setReceiveSystemProperties(\Apache\Rocketmq\V2\ReceiveSystemProperties::MIN_OFFSET);
                } elseif ($policy === OffsetOption::POLICY_MAX_VALUE) {
                    $request->setReceiveSystemProperties(\Apache\Rocketmq\V2\ReceiveSystemProperties::MAX_OFFSET);
                } elseif ($policy === OffsetOption::POLICY_LAST_VALUE) {
                    $request->setReceiveSystemProperties(\Apache\Rocketmq\V2\ReceiveSystemProperties::LAST_OFFSET);
                }
            } elseif ($offsetOption->isTimestamp()) {
                $timestamp = new \Google\Protobuf\Timestamp();
                $timestamp->setSeconds($offsetOption->getValue());
                $request->setReceiveTimestamp($timestamp);
            }
            // TYPE_OFFSET and TYPE_TAIL_N are handled server-side via system properties
        }

        $metadata = $this->buildMetadata();

        // Set call timeout for PHP gRPC (microseconds)
        $totalTimeoutMs = $invisibleDuration * 1000;
        $callOptions = ['timeout' => $totalTimeoutMs * 1000];

        try {
            $call = $this->client->ReceiveMessage($request, $metadata, $callOptions);
            $messages = [];

            while ($receiveResponse = $call->recv()) {
                $case = $receiveResponse->getCase();
                if ($case === 4) { // MESSAGE
                    $messageList = $receiveResponse->getMessage();
                    foreach ($messageList->getMessages() as $msg) {
                        $messages[] = $msg;
                    }
                }
            }

            $this->logger->debug("LiteSimpleConsumer received " . count($messages) . " messages for liteTopic={$liteTopic}");
            return $messages;
        } catch (\Exception $e) {
            $this->logger->warning("LiteSimpleConsumer receive exception for liteTopic={$liteTopic}: " . $e->getMessage());
            return [];
        }
    }

    /**
     * Establish Telemetry Session.
     */
    private function establishTelemetrySession()
    {
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

        $subscriptionEntries = [];
        foreach ($this->liteTopics as $liteTopic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);

            $topicResource = new Resource();
            $topicResource->setName($this->parentTopic);

            $subscriptionEntry = new SubscriptionEntry();
            $subscriptionEntry->setTopic($topicResource);
            $subscriptionEntry->setExpression($filterExpression);

            $subscriptionEntries[] = $subscriptionEntry;
        }

        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);

        $settings = new Settings();
        $settings->setClientType(ClientType::LITE_SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);

        $command = new TelemetryCommand();
        $command->setSettings($settings);

        $success = $this->telemetrySession->createStreamAndSync($command);
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
    }

    /**
     * Extract receipt handle from a message.
     */
    private function extractReceiptHandle($messageView)
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
        }
        return null;
    }

    /**
     * Extract message ID from a message.
     */
    private function extractMessageId($messageView)
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getMessageId') && $sysProps->getMessageId() !== null && $sysProps->getMessageId() !== '') {
                return $sysProps->getMessageId();
            }
        }
        return null;
    }

    /**
     * Extract topic from a message.
     */
    private function extractTopic($messageView)
    {
        if (method_exists($messageView, 'getTopic')) {
            $topic = $messageView->getTopic();
            if (method_exists($topic, 'getName')) {
                return $topic->getName();
            }
        }
        return null;
    }

    /**
     * Extract liteTopic from a message.
     */
    private function extractLiteTopic($messageView)
    {
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic()) {
                return $sysProps->getLiteTopic();
            }
        }
        return null;
    }

    /**
     * Build metadata for gRPC calls.
     */
    private function buildMetadata()
    {
        return Signature::sign(
            null,
            $this->clientId,
            ClientConstants::LANGUAGE,
            ClientConstants::CLIENT_VERSION,
            '',
            'v2'
        );
    }

    /**
     * @return string
     */
    public function getClientId()
    {
        return $this->clientId;
    }

    /**
     * @return bool
     */
    public function isRunning()
    {
        return $this->isRunning;
    }

    /**
     * @return MessagingServiceClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Check that the consumer is not yet running.
     */
    private function checkNotRunning()
    {
        if ($this->isRunning) {
            throw new \RuntimeException("LiteSimpleConsumer is already running");
        }
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
}
