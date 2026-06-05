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

use Apache\Rocketmq\V2\SyncLiteSubscriptionRequest;
use Apache\Rocketmq\V2\LiteSubscriptionAction;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\ClientType;

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
    private string $parentTopic = '';
    private array $liteTopics = [];
    private int $liteSubscriptionQuota = 0;
    private int $maxLiteTopicSize = 64;
    private int $syncLiteSubscriptionInterval = 30;
    /** @var callable|null per-lite-topic callback */
    private $liteMessageListener = null;
    private int $lastSyncTime = 0;
    private ?ProcessQueue $virtualProcessQueue = null; // ProcessQueue|null

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

    /**
     * Get the client type for this consumer.
     *
     * @return int ClientType::LITE_PUSH_CONSUMER
     */
    protected function getClientType(): int
    {
        return ClientType::LITE_PUSH_CONSUMER;
    }

    /**
     * Unsubscribe from a lite topic.
     *
     * @param string $liteTopic Lite topic name to remove
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
     * @param callable $listener Callback invoked for messages with no per-lite-topic listener
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
    public function start(): void
    {
        if ($this->isRunning()) {
            return;
        }

        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LitePushConsumer has no lite topics subscribed");
        }

        if ($this->liteMessageListener === null) {
            throw new \RuntimeException("LitePushConsumer has no lite message listener");
        }

        $this->logger->info("LitePushConsumer starting, clientId={$this->getClientId()}, parentTopic={$this->parentTopic}");
        parent::start();
    }

    /**
     * Setup before the main scan loop: sync lite subscriptions and wait for assignments.
     */
    protected function onStartBeforeLoop()
    {
        $self = $this;
        $this->telemetrySession->setOnNotifyUnsubscribeLite(function ($notifyCmd) use ($self) {
            $liteTopic = $notifyCmd->getLiteTopic();
            $self->logger->info("Received NotifyUnsubscribeLite for liteTopic={$liteTopic}");
            $self->handleUnsubscribeLite($liteTopic);
        });
        $this->syncLiteSubscriptions();
        $this->lastSyncTime = time();
        $pollInterval = 500000;
        $maxAttempts = 10;
        for ($attempt = 0; $attempt < $maxAttempts; $attempt++) {
            try {
                $assignments = $this->queryLiteAssignment();
                if ($assignments !== null && count($assignments->getAssignments()) > 0) {
                    $this->logger->info("Lite subscription active after " . ($attempt + 500) . "ms" . count($assignments->getAssignments()) . " assignments");
                    return;
                }
            } catch (\Exception $e) {
                $this->logger->error("Error querying lite subscription: " . $e->getMessage());
            }
            $this->logger->debug("Waiting doe lite subscription to take effect, attempt {$attempt}/{$maxAttempts}");
            SwooleCompat::sleep($pollInterval);
        }
        $this->logger->error("Lite subscription time out waiting assignments, will scan during normal cycle");
    }

    /**
     * Handle server-initiated lite topic unsubscription.
     *
     * @param string $liteTopic Lite topic name to remove from subscriptions
     * @return void
     */
    public function handleUnsubscribeLite($liteTopic)
    {
        if (array_key_exists($liteTopic, $this->liteTopics)) {
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

        $metadata = $this->buildMetadata(ClientConstants::GRPC_SYNC_LITE_MESSAGE_TIMEOUT / 1000);

        try {
            list($response, $status) = $this->getClient()->SyncLiteSubscription($request, $metadata, $this->getCallOptions())->wait();
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
     * Query the server for lite topic assignment information.
     *
     * @return object|null Assignment response or null on failure
     */
    private function queryLiteAssignment()
    {
        $topicResource = new \Apache\Rocketmq\V2\Resource();
        $topicResource->setName($this->parentTopic);
        $groupResource = new \Apache\Rocketmq\V2\Resource();
        $groupResource->setName($this->consumerGroup);
        $request = new \Apache\Rocketmq\V2\QueryAssignmentRequest();
        $request->setTopic($topicResource);
        $request->setGroup($groupResource);
        $request->setEndpoints($this->parseEndpoints($this->endpoints));
        $metadata = $this->buildMetadata(ClientConstants::GRPC_SYNC_LITE_MESSAGE_TIMEOUT / 1000);
        list($response, $status) = $this->getClient()->QueryAssignment($request, $metadata, $this->getCallOptions())->wait();
        if ($status->code !== 0) {
            return null;
        }
        return $response;
    }
}
