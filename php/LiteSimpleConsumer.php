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
use Apache\Rocketmq\V2\QueryAssignmentRequest;

/**
 * LiteSimpleConsumer - Pull-based (explicit receive/ack) consumer for lite topics.
 *
 * Extends SimpleConsumer with dynamic lite topic subscription management. Instead
 * of creating many physical topics, Lite consumers bind to a single parent (bind)
 * topic and dynamically (un)subscribe logical lite topics via SyncLiteSubscription.
 * Messages are pulled explicitly with receive() and acknowledged with ack() /
 * changeInvisibleDuration(), exactly like a normal SimpleConsumer -- the only
 * difference is the lite subscription lifecycle and the lite_topic carried on the
 * wire for ack / change-invisible-duration requests.
 *
 * Usage:
 *   $consumer = new LiteSimpleConsumer($endpoints, $consumerGroup, $parentTopic);
 *   $consumer->subscribeLite('lite-topic-1');
 *   $consumer->subscribeLite('lite-topic-2');
 *   $consumer->start();
 *   $messages = $consumer->receive(32, 30);
 *   // ... process ...
 *   $consumer->ack($messages);
 */
class LiteSimpleConsumer extends SimpleConsumer
{
    private readonly string $parentTopic;
    private array $liteTopics = [];
    private readonly int $liteSubscriptionQuota;
    private readonly int $maxLiteTopicSize;
    private int $syncLiteSubscriptionInterval = 30;
    private int $lastSyncTime = 0;

    /**
     * Constructor.
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param string $parentTopic Parent (bind) topic that hosts the lite topics
     * @param array $options Configuration options
     *  - clientId: string, custom client identifier (default: 'php-consumer-{pid}-{time}')
     *  - namespace: string, resource namespace prefix (default: '')
     *  - requestTimeout: int, gRPC request timeout in ms (default: 3000)
     *  - awaitDuration: int, long polling timeout in seconds (default: 30)
     *  - credentials: SessionCredentials|null, AK/SK authentication credentials
     *  - tlsCredentials: TlsCredentials|null, TLS/SSL configuration
     *  - sslEnabled: bool, enable SSL for gRPC channel (default: true)
     *  - liteSubscriptionQuota: int, max number of lite topic subscriptions (default: 0 = unlimited)
     *  - maxLiteTopicSize: int, max length of lite topic name (default: 64)
     */
    public function __construct(string $endpoints, string $consumerGroup, string $parentTopic, array $options = [])
    {
        if (empty(trim($parentTopic))) {
            throw new \InvalidArgumentException("LiteSimpleConsumer parentTopic cannot be empty");
        }
        $this->parentTopic = $parentTopic;

        // Bind to the parent topic; lite topics are synced separately via SyncLiteSubscription.
        $liteOptions = array_merge($options, [
            'subscriptionExpressions' => [$parentTopic => '*'],
        ]);

        parent::__construct($endpoints, $consumerGroup, $liteOptions);

        $this->liteSubscriptionQuota = $options['liteSubscriptionQuota'] ?? 0;
        $this->maxLiteTopicSize = $options['maxLiteTopicSize'] ?? 64;
    }

    /**
     * Subscribe to a lite topic.
     *
     * Must be called before start().
     *
     * @param string $liteTopic Lite topic name
     * @param callable|null $listener Optional per-lite-topic callback (reserved for symmetry)
     * @return $this
     */
    public function subscribeLite(string $liteTopic, ?callable $listener = null): self
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
     * Unsubscribe from a lite topic.
     *
     * @param string $liteTopic Lite topic name to remove
     * @return $this
     */
    public function unsubscribeLite(string $liteTopic): self
    {
        $this->checkNotRunning();
        unset($this->liteTopics[$liteTopic]);
        return $this;
    }

    /**
     * Get the client type reported to the broker.
     *
     * @return int ClientType::LITE_SIMPLE_CONSUMER
     */
    protected function getClientType(): int
    {
        return ClientType::LITE_SIMPLE_CONSUMER;
    }

    /**
     * Address ack / change-invisible-duration requests at the bound parent topic.
     *
     * LMQ messages are physically stored under the parent topic; the logical lite
     * topic travels in the lite_topic field, not in the request's topic.
     *
     * @param object $message MessageView to acknowledge
     * @return string|null
     */
    protected function getAckRequestTopic($message): ?string
    {
        return $this->parentTopic;
    }

    /**
     * Get the subscribed lite topics.
     *
     * @return array List of lite topic names
     */
    public function getLiteTopics(): array
    {
        return array_keys($this->liteTopics);
    }

    /**
     * Start the LiteSimpleConsumer.
     *
     * Overrides parent start() to validate lite subscriptions and sync them with
     * the broker before the first receive().
     */
    public function start(): void
    {
        if ($this->isStarted) {
            return;
        }

        if (empty($this->liteTopics)) {
            throw new \RuntimeException("LiteSimpleConsumer has no lite topics subscribed");
        }

        $this->logger->info("LiteSimpleConsumer starting, clientId={$this->getClientId()}, parentTopic={$this->parentTopic}");
        parent::start();

        $this->onStartBeforeLoop();
    }

    /**
     * Receive messages from the bound parent topic.
     *
     * Overrides parent to periodically re-sync lite subscriptions (the broker may
     * drop the lite subscription if the client goes silent), then delegates to the
     * standard SimpleConsumer receive().
     *
     * @param int $maxMessages Maximum number of messages to receive in total
     * @param int $invisibleDuration Invisible duration in seconds for received messages
     * @return array List of received MessageView objects
     */
    public function receive(int $maxMessages = 10, int $invisibleDuration = 30): array
    {
        $now = time();
        if (!empty($this->liteTopics) && ($now - $this->lastSyncTime) >= $this->syncLiteSubscriptionInterval) {
            try {
                $this->syncLiteSubscriptions();
                $this->lastSyncTime = $now;
            } catch (\Exception $e) {
                $this->logger->warning("Periodic SyncLiteSubscription failed: " . $e->getMessage());
            }
        }
        return parent::receive($maxMessages, $invisibleDuration);
    }

    /**
     * Setup before the first receive(): register the unsubscribe handler, sync
     * lite subscriptions, and wait for the assignment to take effect.
     */
    protected function onStartBeforeLoop(): void
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
                $assignmentList = $assignments ? ProtobufUtil::repeatedFieldToArray($assignments->getAssignments()) : [];
                if (!empty($assignmentList)) {
                    $this->logger->info("Lite subscription active after " . (($attempt + 1) * 500) . "ms, " . count($assignmentList) . " assignments");
                    return;
                }
            } catch (\Exception $e) {
                $this->logger->error("Error querying lite subscription: " . $e->getMessage());
            }
            $this->logger->debug("Waiting for lite subscription to take effect, attempt {$attempt}/{$maxAttempts}");
            SwooleCompat::sleep($pollInterval);
        }
        $this->logger->error("Lite subscription timed out waiting for assignments, will retry during normal cycle");
    }

    /**
     * Handle server-initiated lite topic unsubscription.
     *
     * @param string $liteTopic Lite topic name to remove from subscriptions
     * @return void
     */
    public function handleUnsubscribeLite(string $liteTopic): void
    {
        if (array_key_exists($liteTopic, $this->liteTopics)) {
            unset($this->liteTopics[$liteTopic]);
            $this->logger->info("Unsubscribed from lite topic: {$liteTopic}");
        }
    }

    /**
     * Sync lite subscriptions to server via SyncLiteSubscription gRPC.
     *
     * @return void
     * @throws \RuntimeException If the sync fails
     */
    public function syncLiteSubscriptions(): void
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
                throw new \RuntimeException("SyncLiteSubscription failed: " . $status->details);
            }
            $this->logger->info("SyncLiteSubscription success for " . count($this->liteTopics) . " lite topics");
        } catch (\RuntimeException $e) {
            throw $e;
        } catch (\Exception $e) {
            throw new \RuntimeException("SyncLiteSubscription exception: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Query the server for lite topic assignment information.
     *
     * @return object|null Assignment response or null on failure
     */
    private function queryLiteAssignment(): ?object
    {
        $topicResource = new Resource();
        $topicResource->setName($this->parentTopic);
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request = new QueryAssignmentRequest();
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
