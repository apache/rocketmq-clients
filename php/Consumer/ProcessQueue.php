<?php

declare(strict_types=1);

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

use Apache\Rocketmq\Logger;
use Apache\Rocketmq\Message\MessageViewImpl;
use Apache\Rocketmq\V2\FilterExpression as V2FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;

/**
 * ProcessQueue manages message receiving and caching for a specific message queue.
 *
 * References Java ProcessQueueImpl:
 * - Backpressure via cache count + size thresholds (dual-layer)
 * - Expiry detection based on activity time + requestTimeout + longPollingTimeout
 * - Delayed retry on cache full or fetch failure
 * - Uses Logger for unified logging, catches Throwable for robustness
 */
class ProcessQueue
{
    /** @var int Backoff delay when cache is full (microseconds) */
    private const RECEIVING_BACKOFF_WHEN_CACHE_FULL_US = 1000000; // 1s

    /** @var int Backoff delay on failure (microseconds) */
    private const RECEIVING_FAILURE_BACKOFF_US = 1000000; // 1s

    /** @var int Backoff delay on flow control (microseconds) */
    private const RECEIVING_FLOW_CONTROL_BACKOFF_US = 20000; // 20ms

    /** @var PushConsumerImpl */
    private PushConsumerImpl $pushConsumer;

    /** @var MessageQueue */
    private MessageQueue $messageQueue;

    /** @var FilterExpression Consumer-side filter expression */
    private FilterExpression $filterExpression;

    /** @var bool */
    private bool $dropped = false;

    /** @var int Cached message count */
    private int $cachedMessageCount = 0;

    /** @var int Cached message size in bytes */
    private int $cachedMessageSizeInBytes = 0;

    /** @var array Cached messages */
    private array $cachedMessages = [];

    /** @var float Last activity timestamp (seconds with microseconds) */
    private float $activityTime;

    /** @var float Timestamp when cache became full (PHP_FLOAT_MIN = never, aligned with Java Long.MIN_VALUE) */
    private float $cacheFullTime;

    /** @var int Receive request count */
    private int $receptionTimes = 0;

    /** @var int Total received messages */
    private int $receivedMessagesQuantity = 0;

    public function __construct(
        PushConsumerImpl $pushConsumer,
        MessageQueue $messageQueue,
        FilterExpression $filterExpression
    ) {
        $this->pushConsumer = $pushConsumer;
        $this->messageQueue = $messageQueue;
        $this->filterExpression = $filterExpression;
        $this->activityTime = microtime(true);
    }

    public function getMessageQueue(): MessageQueue
    {
        return $this->messageQueue;
    }

    public function isDropped(): bool
    {
        return $this->dropped;
    }

    public function drop(): void
    {
        $this->dropped = true;
        Logger::info("Process queue has been dropped, mq=" . $this->getQueueDescription());
    }

    /**
     * Check if this process queue is expired.
     *
     * References Java ProcessQueueImpl.expired():
     * A queue is expired only if BOTH activity time and cache-full time
     * exceed the max idle duration (3x of requestTimeout + longPollingTimeout).
     */
    public function expired(): bool
    {
        $requestTimeoutSec = 3; // default 3 seconds
        $longPollingTimeoutSec = 30; // default 30 seconds
        $maxIdleSec = ($requestTimeoutSec + $longPollingTimeoutSec) * 3;

        $now = microtime(true);
        $idleDuration = $now - $this->activityTime;

        if ($idleDuration < $maxIdleSec) {
            return false;
        }

        // Also check cache full time to prevent false positives
        // Aligned with Java: if cacheFullTime is PHP_FLOAT_MIN (never full), 
        // afterCacheFullDuration will be huge, so this check won't block expiration
        $afterCacheFullDuration = $now - $this->cacheFullTime;
        if ($afterCacheFullDuration < $maxIdleSec) {
            return false;
        }

        Logger::warn(sprintf(
            "Process queue is idle, idleDuration=%.1fs, maxIdleDuration=%ds, afterCacheFullDuration=%.1fs, mq=%s",
            $idleDuration,
            $maxIdleSec,
            $afterCacheFullDuration,
            $this->getQueueDescription()
        ));
        return true;
    }

    /**
     * Check if the cache is full (backpressure trigger).
     *
     * References Java ProcessQueueImpl.isCacheFull():
     * Dual-layer check: message count OR message size.
     */
    public function isCacheFull(): bool
    {
        $countThreshold = $this->pushConsumer->cacheMessageCountThresholdPerQueue();
        if ($this->cachedMessageCount >= $countThreshold) {
            Logger::warn(sprintf(
                "Process queue total cached messages quantity exceeds the threshold, threshold=%d, actual=%d, mq=%s",
                $countThreshold,
                $this->cachedMessageCount,
                $this->getQueueDescription()
            ));
            $this->cacheFullTime = microtime(true);
            return true;
        }

        $sizeThreshold = $this->pushConsumer->cacheMessageSizeThresholdPerQueue();
        if ($this->cachedMessageSizeInBytes >= $sizeThreshold) {
            Logger::warn(sprintf(
                "Process queue total cached messages memory exceeds the threshold, threshold=%d bytes, actual=%d bytes, mq=%s",
                $sizeThreshold,
                $this->cachedMessageSizeInBytes,
                $this->getQueueDescription()
            ));
            $this->cacheFullTime = microtime(true);
            return true;
        }

        return false;
    }

    /** @var int gRPC ReceiveMessage timeout in seconds (short to avoid blocking event loop) */
    private const RECEIVE_TIMEOUT_SEC = 5;

    /**
     * Start continuous message fetching via Swoole Timer self-scheduling.
     *
     * References Java ProcessQueueImpl self-scheduling pattern:
     * receiveMessage() → gRPC fetch → onReceiveMessageResult() → receiveMessage() (loop)
     * On failure → receiveMessageLater(delay) → receiveMessage() (retry with backoff)
     *
     * Uses Timer::after() instead of Coroutine::create() because PHP gRPC extension
     * performs C-level blocking I/O that doesn't yield to Swoole's coroutine scheduler.
     */
    public function fetchMessageImmediately(): void
    {
        // Defer to next event loop tick so start() returns immediately
        \Swoole\Timer::after(1, function () {
            $this->scheduleFetchCycle();
        });
    }

    /**
     * Single fetch cycle: check conditions → fetch → schedule next cycle.
     */
    private function scheduleFetchCycle(): void
    {
        if ($this->dropped || !$this->pushConsumer->isRunning()) {
            Logger::info("ProcessQueue fetch loop exited, dropped=" . ($this->dropped ? 'true' : 'false') . ", mq=" . $this->getQueueDescription());
            return;
        }

        if ($this->isCacheFull()) {
            Logger::warn("Process queue cache is full, would receive message later, mq=" . $this->getQueueDescription());
            $this->cacheFullTime = microtime(true);
            \Swoole\Timer::after((int)(self::RECEIVING_BACKOFF_WHEN_CACHE_FULL_US / 1000), function () {
                $this->scheduleFetchCycle();
            });
            return;
        }

        try {
            $this->fetchMessage();
            // Success: schedule next fetch immediately (1ms to yield event loop)
            \Swoole\Timer::after(1, function () {
                $this->scheduleFetchCycle();
            });
        } catch (\Throwable $e) {
            Logger::error("Failed to fetch message: " . $e->getMessage() . ", mq=" . $this->getQueueDescription());
            // Failure: backoff then retry
            \Swoole\Timer::after((int)(self::RECEIVING_FAILURE_BACKOFF_US / 1000), function () {
                $this->scheduleFetchCycle();
            });
        }
    }

    /**
     * Calculate the reception batch size based on cache capacity.
     *
     * References Java ProcessQueueImpl.getReceptionBatchSize():
     * Dynamically adjusts batch size to avoid exceeding cache thresholds.
     *
     * @return int Batch size (1 to maxBatchSize)
     */
    private function getReceptionBatchSize(): int
    {
        // Calculate remaining cache capacity by count
        $countThreshold = $this->pushConsumer->cacheMessageCountThresholdPerQueue();
        $remainingCount = $countThreshold - $this->cachedMessageCount;
        $remainingCount = max($remainingCount, 1); // At least 1

        // Get configured max batch size (default 32)
        $maxBatchSize = 32; // TODO: Get from settings

        // Return the smaller of remaining capacity and max batch size
        return min($remainingCount, $maxBatchSize);
    }

    private function fetchMessage(): void
    {
        if ($this->dropped) {
            return;
        }

        $this->activityTime = microtime(true);

        $request = new ReceiveMessageRequest();

        // Set consumer group (required field)
        $groupResource = new Resource();
        $groupResource->setName($this->pushConsumer->getConsumerGroup());
        $request->setGroup($groupResource);

        $request->setMessageQueue($this->messageQueue);

        // Convert Consumer\FilterExpression to V2\FilterExpression for gRPC
        $v2Filter = new V2FilterExpression();
        $v2Filter->setExpression($this->filterExpression->getExpression());
        $filterType = $this->filterExpression->getType() === FilterExpressionType::TAG
            ? FilterType::TAG
            : FilterType::SQL;
        $v2Filter->setType($filterType);
        $request->setFilterExpression($v2Filter);

        $request->setBatchSize(32);
        $request->setAutoRenew(true);

        // Set long polling timeout
        $longPollingTimeout = new \Google\Protobuf\Duration();
        $longPollingTimeout->setSeconds(self::RECEIVE_TIMEOUT_SEC);
        $request->setLongPollingTimeout($longPollingTimeout);

        $client = $this->pushConsumer->getClient();

        // Set gRPC deadline: longPolling + extra margin for network
        $deadlineUs = (self::RECEIVE_TIMEOUT_SEC + 3) * 1000 * 1000;
        $call = $client->ReceiveMessage($request, [], ['timeout' => $deadlineUs]);

        $messages = [];
        foreach ($call->responses() as $response) {
            if ($response->hasMessage()) {
                $message = $response->getMessage();
                $messages[] = $message;

                $this->cachedMessageCount++;
                $body = $message->getBody();
                $messageSize = is_string($body) ? strlen($body) : 0;
                $this->cachedMessageSizeInBytes += $messageSize;
            }
        }

        $this->receptionTimes++;
        $this->receivedMessagesQuantity += count($messages);

        if (method_exists($this->pushConsumer, 'incrementReceptionTimes')) {
            $this->pushConsumer->incrementReceptionTimes();
            $this->pushConsumer->incrementReceivedMessagesQuantity(count($messages));
        }

        if (!empty($messages)) {
            $this->processMessages($messages);
        }
    }

    private function processMessages(array $messages): void
    {
        $listener = $this->pushConsumer->getMessageListener();

        foreach ($messages as $message) {
            if ($this->dropped) {
                break;
            }

            try {
                // Convert protobuf Message to MessageViewImpl for the listener
                // This ensures RepeatedField/MapField are converted to native PHP arrays
                $messageView = MessageViewImpl::fromProtobuf($message);
                $result = $listener($messageView);

                if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                    $this->ackMessage($message);
                    if (method_exists($this->pushConsumer, 'incrementConsumptionOkQuantity')) {
                        $this->pushConsumer->incrementConsumptionOkQuantity(1);
                    }
                } else {
                    if (method_exists($this->pushConsumer, 'incrementConsumptionErrorQuantity')) {
                        $this->pushConsumer->incrementConsumptionErrorQuantity(1);
                    }
                }
            } catch (\Throwable $e) {
                Logger::error("Failed to process message: " . $e->getMessage());
                if (method_exists($this->pushConsumer, 'incrementConsumptionErrorQuantity')) {
                    $this->pushConsumer->incrementConsumptionErrorQuantity(1);
                }
            } finally {
                $this->evictCache($message);
            }
        }
    }

    /**
     * Remove a message from cache and update counters.
     */
    private function evictCache($message): void
    {
        $this->cachedMessageCount = max(0, $this->cachedMessageCount - 1);
        $body = $message->getBody();
        $messageSize = is_string($body) ? strlen($body) : 0;
        $this->cachedMessageSizeInBytes = max(0, $this->cachedMessageSizeInBytes - $messageSize);
    }

    private function ackMessage($message): void
    {
        try {
            $receiptHandle = $message->getSystemProperties()->getReceiptHandle();

            if ($receiptHandle === null || $receiptHandle === '') {
                return;
            }

            $entry = new \Apache\Rocketmq\V2\AckMessageEntry();
            $entry->setReceiptHandle($receiptHandle);
            $entry->setMessageId($message->getSystemProperties()->getMessageId());

            $groupResource = new Resource();
            $groupResource->setName($this->pushConsumer->getConsumerGroup());

            $topicResource = new Resource();
            $topicResource->setName($this->messageQueue->getTopic()->getName());

            $request = new \Apache\Rocketmq\V2\AckMessageRequest();
            $request->setGroup($groupResource);
            $request->setTopic($topicResource);
            $request->setEntries([$entry]);

            $client = $this->pushConsumer->getClient();

            [$response, $status] = $client->AckMessage($request)->wait();

            if ($status->code !== \Grpc\STATUS_OK) {
                Logger::error("Failed to ack message, gRPC status: {$status->details}");
            }
        } catch (\Throwable $e) {
            Logger::error("Failed to ack message: " . $e->getMessage());
        }
    }

    public function getCachedMessageCount(): int
    {
        return $this->cachedMessageCount;
    }

    public function getCachedMessageSizeInBytes(): int
    {
        return $this->cachedMessageSizeInBytes;
    }

    public function getReceptionTimes(): int
    {
        return $this->receptionTimes;
    }

    public function getReceivedMessagesQuantity(): int
    {
        return $this->receivedMessagesQuantity;
    }

    /**
     * Log statistics and reset counters
     * 
     * References Java ProcessQueueImpl.doStats():
     * - Logs process queue stats with clientId
     * - Resets receptionTimes and receivedMessagesQuantity counters
     */
    public function doStats(): void
    {
        // Get current values before resetting
        $receptionTimes = $this->receptionTimes;
        $receivedMessagesQuantity = $this->receivedMessagesQuantity;
        
        // Reset counters (aligned with Java getAndSet(0))
        $this->receptionTimes = 0;
        $this->receivedMessagesQuantity = 0;

        Logger::info(sprintf(
            "Process queue stats: clientId=%s, mq=%s, receptionTimes=%d, receivedMessageQuantity=%d, cachedMessageCount=%d, cachedMessageBytes=%d",
            $this->pushConsumer->getClientId(),
            $this->getQueueDescription(),
            $receptionTimes,
            $receivedMessagesQuantity,
            $this->cachedMessageCount,
            $this->cachedMessageSizeInBytes
        ));
    }

    private function getQueueDescription(): string
    {
        try {
            $topic = $this->messageQueue->getTopic()->getName();
            $broker = $this->messageQueue->getBroker()->getName();
            $id = $this->messageQueue->getId();
            return "topic={$topic}, broker={$broker}, id={$id}";
        } catch (\Throwable $e) {
            return 'unknown';
        }
    }
}
