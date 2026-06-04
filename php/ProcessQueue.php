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

use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\Resource;
use Google\Protobuf\Duration;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Grpc\ChannelCredentials;

/**
 * ProcessQueue - Per-MessageQueue message cache and fetcher.
 *
 * Each ProcessQueue is mapped to exactly
 * one MessageQueue. It caches messages fetched from the server, manages the
 * receive loop, and handles ack/nack/evict after consumption.
 */
class ProcessQueue
{
    private ConsumerInterface $consumer;
    private MessageQueue $messageQueue;
    private string $filterExpression;
    private bool $dropped = false;
    private array $cachedMessages = [];
    private int $cachedMessagesBytes = 0;

    // O(1) eviction: index by receipt handle + swap-with-last removal
    private array $cachedMessagesByReceiptHandle = [];
    private array $evictedMessageIds = [];

    private int $receptionTimes = 0;
    private int $receivedMessagesQuantity = 0;
    private int $activityNanoTime;
    private int $cacheFullNanoTime;
    private string $attemptId;
    private bool $fetchImmediately = false;
    private Logger $logger;

    /**
     * Constructor.
     *
     * @param ConsumerInterface $consumer The parent consumer instance
     * @param MessageQueue $messageQueue The message queue to consume from
     * @param string $filterExpression Filter expression (default '*')
     */
    public function __construct($consumer, MessageQueue $messageQueue, $filterExpression = '*')
    {
        $this->consumer = $consumer;
        $this->messageQueue = $messageQueue;
        $this->filterExpression = $filterExpression;
        $this->activityNanoTime = hrtime(true);
        $this->cacheFullNanoTime = 0;
        $this->attemptId = uniqid('php-pq-', true);
        $this->logger = Logger::getInstance('ProcessQueue');
    }

    /**
     * Signal that messages should be fetched immediately on next loop iteration.
     *
     * @return void
     */
    public function fetchMessageImmediately(): void
    {
        $this->fetchImmediately = true;
    }

    /**
     * Pull messages from broker. Called by the main loop.
     *
     * @return int Number of messages fetched
     */
    public function fetchMessages(): int
    {
        if ($this->dropped) {
            return 0;
        }

        if ($this->isCacheFull()) {
            $this->cacheFullNanoTime = hrtime(true);
            return 0;
        }

        $this->activityNanoTime = hrtime(true);
        $batchSize = $this->getBatchSize();

        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($this->filterExpression);
        $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);
        $awaitDuration = $this->consumer->getAwaitDuration();

        $request = new ReceiveMessageRequest();
        $request->setGroup($this->consumer->getGroupResourceWithNamespace());
        $request->setMessageQueue($this->messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($batchSize);
        $request->setAutoRenew(true); // PushConsumer uses server-side auto-renew
        $request->setAttemptId($this->attemptId);

        $longPollingTimeout = $this->createDuration($awaitDuration);
        $request->setLongPollingTimeout($longPollingTimeout);

        $requestTimeoutMs = 3000;
        $awaitDurationMs = $awaitDuration * 1000;
        $totalTimeoutMs = $requestTimeoutMs + $awaitDurationMs;
        $metadata = $this->buildMetadata($totalTimeoutMs);

        $this->logger->debug("ProcessQueue fetching messages from queue, batchSize={$batchSize}, attemptId={$this->attemptId}");

        $count = 0;
        try {
            $client = $this->getReceiveClient();
            $call = $client->ReceiveMessage($request, $metadata);

            foreach ($call->responses() as $response) {
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $code = $status->getCode();
                    if ($code !== 20000 && $code !== 40404 && $code !== 40401) {
                        $this->logger->warning("ProcessQueue non-OK status: code={$code}, msg=" . $status->getMessage());
                    }
                }

                if ($response->hasMessage()) {
                    $message = $response->getMessage();
                    $this->cacheMessages([$message]);
                    $this->receivedMessagesQuantity++;
                    $count++;
                    $this->consumeStreamedMessage($message);
                }
            }

            $this->receptionTimes++;
            $this->attemptId = uniqid('php-pq-', true);
            $this->activityNanoTime = hrtime(true);

        } catch (\Exception $e) {
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                $this->logger->error("ProcessQueue fetchMessages error: " . $e->getMessage());
            }
        }

        $this->logger->debug("ProcessQueue fetched {$count} messages, cached=" . count($this->cachedMessages));
        return $count;
    }

    /**
     * Get the delivery attempt count from the consumer retry policy.
     * @return int
     */
    private function getMaxAttempts(): int
    {
        $retryPolicy = $this->consumer->getRetryPolicy();
        if ($retryPolicy !== null && method_exists($retryPolicy, 'getMaxAttempts')) {
            return $retryPolicy->getMaxAttempts();
        }
        // Default max attempts
        return 5;
    }

    /**
     * Consume a streamed message and handle the result (ack/nack/evict).
     *
     * @param object $message Protobuf message received from stream
     * @return void
     */
    private function consumeStreamedMessage($message)
    {
        if ($this->consumer->getConsumeService() === null) {
            return;
        }
        if ($this->dropped) {
            return;
        }
        $endpoints = $this->getBrokerEndpoint();
        $messageView = new MessageView($message, null, $endpoints);
        $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
        
        if ($messageView->isCorrupted()) {
            $this->logger->error("ProcessQueue consumerStreamedMessage: message corrupted, discarding messageId={$messageId}");
            $this->consumer->nackMessage($messageView);
            $this->evictMessage($messageView);
            return;
        }
        
        // Consume FIRST, then evict based on result
        $result = $this->consumer->getConsumeService()->consumeMessage($messageView);
        
        if ($result instanceof \Apache\Rocketmq\ConsumeResultSuspend) {
            $suspendSec = (int)ceil($result->getSuspendTimeMs() / 1000);
            $this->logger->debug("ProcessQueue consumeStreamedMessage SUSPEND messageId={$messageId}, suspendSec={$suspendSec}");
            $this->consumer->nackMessage($messageView, 1, $suspendSec);
            $this->evictMessage($messageView);
        } elseif ($result === \Apache\Rocketmq\ConsumeResult::FAILURE) {
            $deliveryAttempt = method_exists($messageView, 'getDeliveryAttempt') ? $messageView->getDeliveryAttempt() : 1;
            $maxAttempts = $this->getMaxAttempts();
            if ($deliveryAttempt >= $maxAttempts) {
                $this->logger->warning("ProcessQueue consumeStreamedMessage FAILURE messageId={$messageId}, deliveryAttempt={$deliveryAttempt}/{$maxAttempts}, forwarding to DLQ");
                $this->consumer->getConsumeService()->forwardToDeadLetterQueue($messageView);
                $this->evictMessage($messageView);
            } else {
                $this->logger->debug("ProcessQueue consumeStreamedMessage FAILURE messageId={$messageId}");
                $this->consumer->nackMessage($messageView, $deliveryAttempt);
                $messageView->incrementDeliveryAttempt();
            }
        } else {
            $this->logger->debug("ProcessQueue consumeStreamedMessage SUCCESS messageId={$messageId}, ACKing immediately");
            $this->consumer->ackMessage($messageView);
            $this->evictMessage($messageView);
        }
    }

    /**
     * Get the broker endpoint from the message queue.
     *
     * @return object|null Broker endpoints object or null
     */
    private function getBrokerEndpoint()
    {
        $broker = $this->messageQueue->getBroker();
        if ($broker && $broker->hasEndpoints()) {
            return$broker->getEndpoints();
        }
        return null;
    }

    /**
     * Cache received messages and track byte size.
     * Also indexes by receipt handle for O(1) eviction.
     *
     * @param array $messages Protobuf messages to cache
     * @return void
     */
    private function cacheMessages($messages)
    {
        $endpoint = $this->getBrokerEndpoint();
        foreach ($messages as $msg) {
            $messageView = new MessageView($msg, null, $endpoint);
            $idx = count($this->cachedMessages);
            $this->cachedMessages[] = $messageView;
            $body = $messageView->getBody() ?? '';
            $this->cachedMessagesBytes += strlen($body);

            // O(1) eviction index
            $receiptHandle = $this->getReceiptHandle($msg);
            if ($receiptHandle !== null) {
                $this->cachedMessagesByReceiptHandle[$receiptHandle] = $idx;
            }
        }
    }

    /**
     * Extract receipt handle from a protobuf message.
     *
     * @param object $msg Protobuf message
     * @return string|null Receipt handle or null if not available
     */
    private function getReceiptHandle($msg): ?string
    {
        $sysProps = $msg->getSystemProperties();
        return $sysProps?->getReceiptHandle();
    }

    /**
     * Test wrapper for cacheMessages (used by unit tests).
     *
     * @param array $messages
     * @return void
     */
    public function testCacheMessages($messages)
    {
        $this->cacheMessages($messages);
    }

    /**
     * Get all cached messages.
     *
     * @return array
     */
    public function getCachedMessages()
    {
        return $this->cachedMessages;
    }

    /**
     * Evict a message from cache after consumption.
     * Uses O(1) swap-with-last eviction with incremental index update.
     *
     * @param object $messageView Message to evict
     * @return void
     */
    public function evictMessage($messageView)
    {
        $objId = spl_object_id($messageView);
        if (isset($this->evictedMessageIds[$objId])) {
            return;
        }
        $this->evictedMessageIds[$objId] = true;
        $idx = null;
        // Try O(1) lookup by receipt handle first
        $sysProps = $messageView->getSystemProperties();
        $receiptHandle = $sysProps->getReceiptHandle();
        if ($receiptHandle !== null && isset($this->cachedMessagesByReceiptHandle[$receiptHandle])) {
            $idx = $this->cachedMessagesByReceiptHandle[$receiptHandle];
            unset($this->cachedMessagesByReceiptHandle[$receiptHandle]);
        }

        if ($idx === null) {
            // Fallback: linear scan
            foreach ($this->cachedMessages as $i => $msg) {
                if ($msg === $messageView) {
                    $idx = $i;
                    break;
                }
            }
        }
        if ($idx === null) {
            return;
        }
        $body = $messageView->getBody() ?? '';
        $this->cachedMessagesBytes -= strlen($body);
        if ($this->cachedMessagesBytes < 0) {
            $this->cachedMessagesBytes = 0;
        }
        $lastIdx = count($this->cachedMessages) - 1;
        if ($idx !== $lastIdx) {
            $swappedMsg = $this->cachedMessages[$idx];
            $this->cachedMessages[$idx] = $this->cachedMessages[$lastIdx];
            $this->cachedMessages[$lastIdx] = $swappedMsg;

            $swappedReceipt = $this->getReceiptHandle($this->cachedMessages[$idx]);
            if ($swappedReceipt !== null) {
                $this->cachedMessagesByReceiptHandle[$swappedReceipt] = $idx;
            }
        }
        array_pop($this->cachedMessages);
    }

    /**
     * Post-consume handling: ack or nack based on result, then evict.
     *
     * @param object $messageView Message to erase
     * @param int $consumeResult ConsumeResult::SUCCESS, FAILURE, or ConsumeResultSuspend::SUSPEND
     * @param int|null $suspendSeconds Optional suspend time in seconds (for SUSPEND result)
     * @return void
     */
    public function eraseMessage($messageView, $consumeResult, ?int $suspendSeconds = null)
    {
        if ($consumeResult === \Apache\Rocketmq\ConsumeResult::SUCCESS) {
            $this->consumer->ackMessage($messageView);
        } else {
            // SUSPEND uses the provided suspendSeconds; FAILURE uses default
            if ($suspendSeconds !== null) {
                $this->consumer->nackMessage($messageView, 1, $suspendSeconds);
            } else {
                $this->consumer->nackMessage($messageView);
            }
        }
        $this->evictMessage($messageView);
    }

    /**
     * Discard a message immediately (nack and evict).
     *
     * @param object $messageView
     * @return void
     */
    public function discardMessage($messageView) {
        $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
        $this->logger->debug("ProcessQueue Discarding message $messageId");
        $this->consumer->nackMessage($messageView);
        $this->evictMessage($messageView);
    }

    /**
     * Discard a FIFO message by forwarding to dead letter queue and evicting.
     *
     * @param object $messageView
     * @return void
     */
    public function discardFifoMessage($messageView)
    {
        $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
        $this->logger->debug("ProcessQueue discardFifoMessage message $messageId");
        if ($this->consumer->getConsumeService() !== null) {
            $this->consumer->getConsumeService()->forwardToDeadLetterQueue($messageView);
        }
        $this->evictMessage($messageView);
    }

    /**
     * Check if cache is full based on per-queue thresholds.
     *
     * @return bool
     */
    public function isCacheFull()
    {
        $countThreshold = $this->consumer->getCacheMessageCountThresholdPerQueue();
        $bytesThreshold = $this->consumer->getCacheMessageBytesThresholdPerQueue();

        if ($countThreshold > 0 && count($this->cachedMessages) >= $countThreshold) {
            return true;
        }
        if ($bytesThreshold > 0 && $this->cachedMessagesBytes >= $bytesThreshold) {
            return true;
        }
        return false;
    }

    /**
     * Mark this ProcessQueue as dropped. Stops fetching.
     *
     * @return void
     */
    public function drop()
    {
        $this->dropped = true;
        $this->evictedMessageIds = [];
        $this->logger->info("ProcessQueue dropped for topic=" . $this->messageQueue->getTopic()->getName());
    }

    /**
     * Check if this ProcessQueue has been dropped.
     *
     * @return bool
     */
    public function isDropped()
    {
        return $this->dropped;
    }

    /**
     * Check if this ProcessQueue is expired (idle for too long while cache-full).
     *
     * @return bool
     */
    public function expired()
    {
        $now = hrtime(true);
        $longPollingTimeoutNs = $this->consumer->getAwaitDuration() * 1000000000;
        $requestTimeoutNs = 3000000000; // 3s
        $expiryThresholdNs = 3 * ($longPollingTimeoutNs + $requestTimeoutNs);

        if (($now - $this->activityNanoTime) > $expiryThresholdNs &&
            ($now - $this->cacheFullNanoTime) > $expiryThresholdNs &&
            $this->cacheFullNanoTime > 0) {
            return true;
        }
        return false;
    }

    /**
     * Get the number of cached messages.
     *
     * @return int
     */
    public function cachedMessagesCount()
    {
        return count($this->cachedMessages);
    }

    /**
     * Get the total bytes of cached messages.
     *
     * @return int
     */
    public function cachedMessageBytes()
    {
        return $this->cachedMessagesBytes;
    }

    /**
     * Get the associated message queue.
     *
     * @return MessageQueue
     */
    public function getMessageQueue()
    {
        return $this->messageQueue;
    }

    /**
     * Get batch size from consumer settings.
     *
     * @return int
     */
    private function getBatchSize()
    {
        return $this->consumer->getReceiveBatchSize();
    }

    /**
     * Create a Google Protobuf Duration from seconds.
     *
     * @param int|float $seconds
     * @return Duration
     */
    private function createDuration($seconds)
    {
        $duration = new Duration();
        $secs = intval($seconds);
        $nanos = intval(($seconds - $secs) * 1000000000);
        $duration->setSeconds($secs);
        $duration->setNanos($nanos);
        return $duration;
    }

    /**
     * Build metadata for gRPC calls using Signature.
     *
     * @param int|null $timeoutMs Optional timeout in milliseconds
     * @return array
     */
    private function buildMetadata(?int $timeoutMs = null)
    {
        $credentials = null;
        $namespace = '';
        $credentials = $this->consumer->getSessionCredentials();
        $namespace = $this->consumer->getNamespace();
        $metadata = Signature::sign(
            $credentials,
            $this->consumer->getClientId(),
            ClientConstants::LANGUAGE,
            ClientConstants::CLIENT_VERSION,
            $namespace,
            'v2'
        );
        if ($timeoutMs !== null) {
            $timeoutUs = $timeoutMs * 1000;
            $metadata['grpc-timeout'] = [$timeoutUs . 'u'];
        }
        return $metadata;
    }

    /**
     * Get the gRPC messaging service client for receiving messages.
     *
     * @return MessagingServiceClient
     */
    private function getReceiveClient(): MessagingServiceClient
    {
        return $this->consumer->getClient();
    }

    /**
     * Erase a FIFO message: ack on SUCCESS, nack with delay on SUSPEND, or forward to DLQ otherwise.
     *
     * @param object $messageView
     * @param mixed $consumeResult ConsumeResult::SUCCESS, ConsumeResultSuspend, or ConsumeResult::FAILURE
     * @return void
     */
    public function eraseFifoMessage($messageView, $consumeResult)
    {
        if ($consumeResult === ConsumeResult::SUCCESS) {
            $this->consumer->ackMessage($messageView);
        } elseif ($consumeResult instanceof ConsumeResultSuspend) {
            $suspendSec = (int)ceil($consumeResult->getSuspendTimeMs() / 1000);
            $this->consumer->nackMessage($messageView, 1, $suspendSec);
        } else {
            $this->consumer->getConsumeService()->forwardToDeadLetterQueue($messageView);
        }
        $this->evictMessage($messageView);
    }

    /**
     * Log stats for this ProcessQueue and reset counters.
     *
     * @return void
     */
    public function doStats()
    {
        $reception = $this->receptionTimes;
        $received = $this->receivedMessagesQuantity;
        $cachedCount = count($this->cachedMessages);
        $cachedBytes = $this->cachedMessagesBytes;

        $this->receptionTimes = 0;
        $this->receivedMessagesQuantity = 0;
        $this->logger->info("ProcessQueue: stats: topic=" . $this->messageQueue->getTopic()->getName() . " Received $received messages in $reception seconds. Cached $cachedCount messages ($cachedBytes bytes)");
    }
}
