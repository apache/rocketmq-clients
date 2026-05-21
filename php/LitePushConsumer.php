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
require_once __DIR__ . '/PushConsumer.php';
require_once __DIR__ . '/ConsumeResultSuspend.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\SyncLiteSubscriptionRequest;
use Apache\Rocketmq\V2\LiteSubscriptionAction;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\ClientType;
use Grpc\ChannelCredentials;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Broker;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\MessageType;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;

/**
 * LitePushConsumer - Push consumer for lite topics.
 *
 * Extends PushConsumer with dynamic lite topic subscription management.
 * Instead of creating many physical topics, Lite consumers use a parent topic
 * with logical lite topic sub-classifiers.
 *
 * Usage:
 *   $consumer = new LitePushConsumer($endpoints, $consumerGroup, $parentTopic);
 *   $consumer->subscribeLite('lite-topic-1', $callback);
 *   $consumer->subscribeLite('lite-topic-2', $callback);
 *   $consumer->start();
 */
class LitePushConsumer extends PushConsumer
{
    private $parentTopic = '';
    private $liteTopics = [];
    private $liteSubscriptionQuota = 0;
    private $maxLiteTopicSize = 64;
    private $syncLiteSubscriptionInterval = 30;
    private $liteMessageListener = null;
    private $lastSyncTime = 0;
    private $virtualProcessQueue = null;

    /**
     * Constructor.
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param string $parentTopic Parent (bound) topic
     * @param array $options Configuration options
     */
    public function __construct($endpoints, $consumerGroup, $parentTopic, $options = [])
    {
        if (empty(trim($parentTopic))) {
            throw new \InvalidArgumentException("LitePushConsumer parentTopic cannot be empty");
        }
        $this->parentTopic = $parentTopic;
        $this->liteMessageListener = $options['messageListener'] ?? null;

        $liteOptions = array_merge($options, [
            'subscriptionExpressions' => [$parentTopic => '*'],
            'fifo' => true,
            'isLiteConsumer' => true,
            'enableFifoConsumeAccelerator' => $options['enableFifoConsumeAccelerator'] ?? true,
            'messageListener' => $this->liteMessageListener,
        ]);

        parent::__construct($endpoints, $consumerGroup, $liteOptions);

        $this->liteSubscriptionQuota = $options['liteSubscriptionQuota'] ?? 0;
        $this->maxLiteTopicSize = $options['maxLiteTopicSize'] ?? 64;
    }

    /**
     * Subscribe to a lite topic.
     *
     * @param string $liteTopic Lite topic name
     * @param callable|null $listener Optional per-lite-topic callback
     * @return $this
     */
    public function subscribeLite($liteTopic, $listener = null)
    {
        $this->checkNotRunning();

        if (strlen($liteTopic) > $this->maxLiteTopicSize) {
            throw new \RuntimeException("Lite topic name exceeds max length of {$this->maxLiteTopicSize}");
        }

        if ($this->liteSubscriptionQuota > 0 && count($this->liteTopics) >= $this->liteSubscriptionQuota) {
            throw new \RuntimeException("Lite subscription quota exceeded: {$this->liteSubscriptionQuota}");
        }

        $this->liteTopics[$liteTopic] = $listener;

        return $this;
    }

    protected function getClientType(): int
    {
        return \Apache\Rocketmq\V2\ClientType::LITE_PUSH_CONSUMER;
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
     * Set the global lite message listener (used when no per-lite-topic listener is set).
     *
     * @param callable $listener
     * @return $this
     */
    public function setLiteMessageListener(callable $listener)
    {
        $this->liteMessageListener = $listener;
        return $this;
    }

    /**
     * Start the LitePushConsumer.
     *
     * Overrides parent start() to sync lite subscriptions, register handlers, and use lite-aware consume service.
     */
    public function start()
    {
        if ($this->isLitePushRunning()) {
            return;
        }

        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LitePushConsumer has no lite topics subscribed");
        }

        if ($this->liteMessageListener === null) {
            throw new \RuntimeException("LitePushConsumer has no lite message listener");
        }

        $this->logger->info("LitePushConsumer starting, clientId={$this->getClientId()}, parentTopic={$this->parentTopic}");

        // Register onNotifyUnsubscribeLite handler for server-initiated unsubscribe commands
        $this->registerUnsubscribeLiteHandler();

        // Sync lite subscriptions to server
        $this->syncLiteSubscriptions();

        $this->lastSyncTime = time();

        $this->startLiteConsumer();

        // Start the parent push consumer loop
        parent::start();
    }

    /**
     * Register the onNotifyUnsubscribeLite handler.
     */
    private function registerUnsubscribeLiteHandler()
    {
        $self = $this;
        $this->telemetrySession->setOnNotifyUnsubscribeLite(function ($notifyCmd) use ($self) {
            $liteTopic = $notifyCmd->getLiteTopic();
            $self->logger->info("Received NotifyUnsubscribeLite for liteTopic={$liteTopic}");
            $self->handleUnsubscribeLite($liteTopic);
        });
    }

    /**
     * Handle server-initiated lite topic unsubscription.
     *
     * @param string $liteTopic
     */
    public function handleUnsubscribeLite($liteTopic)
    {
        if (isset($this->liteTopics[$liteTopic])) {
            unset($this->liteTopics[$liteTopic]);
            $this->logger->info("Unsubscribed from lite topic: {$liteTopic}");
        }
    }

    /**
     * Hook called after each scan cycle to perform periodic lite sync.
     */
    protected function onScanCycleComplete()
    {
        $now = time();
        if (!empty($this->liteTopics) && ($now - $this->lastSyncTime) >= $this->syncLiteSubscriptionInterval) {
            $this->syncLiteSubscriptions();
            $this->lastSyncTime = $now;
        }
    }

    /**
     * Sync lite subscriptions to server via SyncLiteSubscription gRPC.
     */
    public function syncLiteSubscriptions()
    {
        if (empty($this->liteTopics)) {
            return;
        }

        $topicResource = new Resource();
        $topicResource->setName($this->parentTopic);

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $request = new SyncLiteSubscriptionRequest();
        $request->setAction(LiteSubscriptionAction::COMPLETE_ADD);
        $request->setTopic($topicResource);
        $request->setGroup($groupResource);
        $request->setLiteTopicSet(array_keys($this->liteTopics));

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->getClient()->SyncLiteSubscription($request, $metadata)->wait();
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
     * Get the lite message listener for a specific lite topic.
     *
     * @param string $liteTopic
     * @return callable|null
     */
    public function getLiteMessageListener($liteTopic)
    {
        if (isset($this->liteTopics[$liteTopic])) {
            $listener = $this->liteTopics[$liteTopic];
            if (is_callable($listener)) {
                return $listener;
            }
        }
        return $this->liteMessageListener;
    }

    /**
     * Check if consumer is in FIFO mode (lite consumers always use FIFO consume service).
     *
     * @return bool
     */
    public function isLiteConsumer()
    {
        return true;
    }

    /**
     * @return string
     */
    public function getParentTopic()
    {
        return $this->parentTopic;
    }

    /**
     * @return int
     */
    public function getMaxLiteTopicSize()
    {
        return $this->maxLiteTopicSize;
    }

    /**
     * @return bool
     */
    private function isLitePushRunning()
    {
        return $this->isRunning();
    }

    private function startLiteConsumer()
    {
        try {
            $this->establishTelemetrySession();
            $this->registerSettingsCallback();
            $this->consumeService = new LiteFifoConsumeService($this->logger, $this->messageListener, $this, $this->enableFifoConsumeAccelerator);
            $this->registerUnsubscribeLiteHandler();
            $this->logger->info("LitePushConsumer started successfully, clientId={$this->clientId}");
            $this->createVirtualProcessQueueForLite();
            $lastScanTime = time();
            while ($this->isRunning() && $this->shutdownRequested) {
                if (function_exists('pcntl_signal_dispatch')) {
                    pcntl_signal_dispatch();
                }
                if ($this->telemetrySession) {
                    $this->telemetrySession->pollTelemetry();
                }
                if ($this->shutdownRequested) {
                    break;
                }
                $now = time();
                if (!empty($this->liteTopics) && ($now - $this->lastSyncTime) >= $this->syncLiteSubscriptionInterval) {
                    $this->syncLiteSubscriptions();
                    $this->lastSyncTime = $now;
                }
                $this->onHeartbeatTick();
                if ($this->virtualProcessQueue !== null && $this->virtualProcessQueue->isDropped()) {
                    if ($this->virtualProcessQueue->isCacheFull()) {
                        $this->virtualProcessQueue->fetchMessages();
                    }
                    if ($this->consumeService !== null && !empty($this->virtualProcessQueue->getCachedMessages())) {
                        $this->consumeService->consume($this->virtualProcessQueue);
                    }
                }
                usleep(100000);
                gc_collect_cycles();
            }
            $this->drainInFlightMessagesLite();
            $this->shutdown();
        } catch (\Exception $e) {
            $this->logger->error("LitePushConsumer start failed: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Create a virtual ProcessQueue for lite consumer.
     * Unlike regular PushConsumer which creates one ProcessQueue per MessageQueue,
     * LitePushConsumer uses a single virtual ProcessQueue bound to the parent topic
     * to receive messages for all lite topics.
     */
    private function createVirtualProcessQueueForLite()
    {
        $this->logger->info("Creating virtual ProcessQueue for LitePushConsumer, parentTopic={$this->parentTopic}");

        $broker = new Broker();
        $broker->setName('virtual-broker');

        $address = new Address();
        $address->setHost('127.0.0.1');
        $address->setPort(8080);

        $endpoints = new Endpoints();
        $endpoints->setScheme(AddressScheme::IPv4);
        $endpoints->setAddresses([$address]);
        $broker->setEndpoints($endpoints);

        $topic = new Resource();
        $topic->setName($this->parentTopic);

        $mq = new MessageQueue();
        $mq->setTopic($topic);
        $mq->setBroker($broker);
        $mq->setPermission(Permission::READ_WRITE);
        $mq->setAcceptMessageTypes([MessageType::FIFO, MessageType::NORMAL]);

        $this->virtualProcessQueue = new ProcessQueue($this, $mq, '*');
        $this->virtualProcessQueue->fetchMessageImmediately();
    }

    /**
     * Drain in-flight messages before shutdown.
     * Marks the virtual ProcessQueue as dropped, then waits up to 30 seconds
     * for cached messages to be consumed, preventing message loss on abrupt termination.
     */
    private function drainInFlightMessagesLite()
    {
        $drainStart = microtime(true);
        $drainTimeout = 30; // seconds
        $drainIterations = 0;

        $this->logger->info("LitePushConsumer drain phase: waiting for in-flight messages to be consumed");

        // Stop fetching new messages
        if ($this->virtualProcessQueue !== null) {
            $this->virtualProcessQueue->drop();
        }

        // Drain cached messages
        while (microtime(true) - $drainStart < $drainTimeout) {
            if ($this->virtualProcessQueue === null) {
                break;
            }

            $messages = $this->virtualProcessQueue->getCachedMessages();
            $remainingCount = count($messages);

            if ($remainingCount === 0) {
                $this->logger->info("LitePushConsumer drain phase completed after {$drainIterations} iterations");
                return;
            }

            // Consume remaining messages
            if ($this->consumeService !== null) {
                $this->consumeService->consume($this->virtualProcessQueue);
            }

            usleep(100000);
            $drainIterations++;
        }

        $remainingCount = $this->virtualProcessQueue !== null ? count($this->virtualProcessQueue->getCachedMessages()) : 0;
        $this->logger->warning("LitePushConsumer drain phase timed out after {$drainTimeout}s, {$remainingCount} messages remaining");
    }
}
