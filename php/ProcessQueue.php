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
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/MessageView.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/RpcClientManager.php';

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
    private $consumer;
    private $messageQueue;
    private $filterExpression;
    private $dropped = false;
    private $cachedMessages = [];
    private $cachedMessagesBytes = 0;

    // O(1) eviction: index by receipt handle
    private $cachedMessagesByReceiptHandle = [];

    private $receptionTimes = 0;
    private $receivedMessagesQuantity = 0;
    private $activityNanoTime;
    private $cacheFullNanoTime;
    private $attemptId;
    private $fetchImmediately = false;
    private $logger;

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

    public function fetchMessageImmediately()
    {
        $this->fetchImmediately = true;
    }

    /**
     * Pull messages from broker. Called by the main loop.
     *
     * @return int Number of messages fetched
     */
    public function fetchMessages()
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

        $request = new ReceiveMessageRequest();
        $request->setGroup($this->consumer->getGroupResource());
        $request->setMessageQueue($this->messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($batchSize);
        $request->setAutoRenew(true); // PushConsumer uses server-side auto-renew
        $request->setAttemptId($this->attemptId);

        $awaitDuration = $this->consumer->getAwaitDuration();
        $longPollingTimeout = $this->createDuration($awaitDuration);
        $request->setLongPollingTimeout($longPollingTimeout);

        $metadata = $this->buildMetadata();

        $requestTimeoutMs = 3000;
        $awaitDurationMs = $awaitDuration * 1000;
        $totalTimeoutMs = $requestTimeoutMs + $awaitDurationMs;
        $metadata['grpc-timeout'] = ["{$totalTimeoutMs}m"];

        $this->logger->debug("ProcessQueue fetching messages from queue, batchSize={$batchSize}, attemptId={$this->attemptId}");

        $count = 0;
        try {
            $client = $this->getReceiveClient();
            $call = $client->ReceiveMessage($request, $metadata);

            foreach ($call->responses() as $response) {
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $code = $status->getCode();
                    if ($code !== 20000 && $code !== 40404) {
                        $this->logger->warning("ProcessQueue non-OK status: code={$code}, msg=" . $status->getMessage());
                    }
                }

                if ($response->hasMessage()) {
                    $message = $response->getMessage();
                    $this->cacheMessages([$message]);
                    $this->receivedMessagesQuantity++;
                    $count++;
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
     * Cache received messages and track byte size.
     * Also indexes by receipt handle for O(1) eviction.
     */
    private function cacheMessages($messages)
    {
        foreach ($messages as $msg) {
            $messageView = new MessageView($msg);
            $idx = count($this->cachedMessages);
            $this->cachedMessages[] = $messageView;
            $body = $msg->getBody() ?? '';
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
     */
    private function getReceiptHandle($msg): ?string
    {
        if (method_exists($msg, 'getSystemProperties')) {
            $sysProps = $msg->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
        }
        return null;
    }

    /**
     * Test wrapper for cacheMessages (used by unit tests).
     *
     * @param array $messages
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
     * Uses O(1) lookup via receipt handle index.
     */
    public function evictMessage($messageView)
    {
        $body = $messageView->body ?? ($messageView->getBody() ?? '');
        $this->cachedMessagesBytes -= strlen($body);
        if ($this->cachedMessagesBytes < 0) {
            $this->cachedMessagesBytes = 0;
        }

        // Try O(1) lookup by receipt handle first
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                $receiptHandle = $sysProps->getReceiptHandle();
                if ($receiptHandle !== null && isset($this->cachedMessagesByReceiptHandle[$receiptHandle])) {
                    $idx = $this->cachedMessagesByReceiptHandle[$receiptHandle];
                    unset($this->cachedMessages[$idx]);
                    unset($this->cachedMessagesByReceiptHandle[$receiptHandle]);
                    $this->cachedMessages = array_values($this->cachedMessages);
                    // Rebuild index after reindexing
                    $this->rebuildReceiptHandleIndex();
                    return;
                }
            }
        }

        // Fallback: linear scan
        foreach ($this->cachedMessages as $i => $msg) {
            if ($msg === $messageView) {
                unset($this->cachedMessages[$i]);
                $this->cachedMessages = array_values($this->cachedMessages);
                $this->rebuildReceiptHandleIndex();
                break;
            }
        }
    }

    /**
     * Rebuild the receipt handle index after array reindexing.
     */
    private function rebuildReceiptHandleIndex()
    {
        $this->cachedMessagesByReceiptHandle = [];
        foreach ($this->cachedMessages as $idx => $msg) {
            $receiptHandle = null;
            if (method_exists($msg, 'getSystemProperties')) {
                $sysProps = $msg->getSystemProperties();
                if (method_exists($sysProps, 'getReceiptHandle')) {
                    $receiptHandle = $sysProps->getReceiptHandle();
                }
            }
            if ($receiptHandle !== null) {
                $this->cachedMessagesByReceiptHandle[$receiptHandle] = $idx;
            }
        }
    }

    /**
     * Post-consume handling: ack or nack based on result, then evict.
     *
     * @param object $messageView Message to erase
     * @param int $consumeResult ConsumeResult::SUCCESS, FAILURE, or ConsumeResultSuspend::SUSPEND
     * @param int|null $suspendSeconds Optional suspend time in seconds (for SUSPEND result)
     */
    public function eraseMessage($messageView, $consumeResult, ?int $suspendSeconds = null)
    {
        if ($consumeResult === \Apache\Rocketmq\ConsumeResult::SUCCESS) {
            $this->consumer->ackMessage($messageView);
        } else {
            // SUSPEND uses the provided suspendSeconds; FAILURE uses default
            if ($suspendSeconds !== null) {
                $this->consumer->nackMessage($messageView, 1);
            } else {
                $this->consumer->nackMessage($messageView);
            }
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
     */
    public function drop()
    {
        $this->dropped = true;
        $this->logger->info("ProcessQueue dropped for topic=" . $this->messageQueue->getTopic()->getName());
    }

    /**
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
     * @return int
     */
    public function cachedMessagesCount()
    {
        return count($this->cachedMessages);
    }

    /**
     * @return int
     */
    public function cachedMessageBytes()
    {
        return $this->cachedMessagesBytes;
    }

    /**
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
     * @return array
     */
    private function buildMetadata()
    {
        $credentials = null;
        $namespace = '';
        if (method_exists($this->consumer, 'getSessionCredentials')) {
            $credentials = $this->consumer->getSessionCredentials();
        }
        if (method_exists($this->consumer, 'getNamespace')) {
            $namespace = $this->consumer->getNamespace();
        }
        return Signature::sign(
            $credentials,
            $this->consumer->getClientId(),
            ClientConstants::LANGUAGE,
            ClientConstants::CLIENT_VERSION,
            $namespace,
            'v2'
        );
    }

    private function getReceiveClient(): MessagingServiceClient
    {
        $broker = $this->messageQueue->getBroker();
        if ($broker && $broker->hasEndpoints()) {
            $endpointsProto = $broker->getEndpoints();
            $addresses = $endpointsProto->getAddresses();
            $addressesArray = ProtobufUtil::repeatedFieldToArray($addresses);
            if (!empty($addressesArray) && $addressesArray[0] !== null) {
                $address = $addressesArray[0];
                $brokerKey = $address->getHost() . ':' . $address->getPort();
                return RpcClientManager::getInstance()->getClient($brokerKey, [
                   'credentials' => ChannelCredentials::createInsecure(),
                ]);
            }
        }
        return $this->consumer->getClient();
    }
}
