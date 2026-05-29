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

use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\ChangeInvisibleDurationRequest;
use Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Google\Protobuf\Duration;
use Grpc\ChannelCredentials;

/**
 * ConsumeService - Abstract base class for message consumption strategies.
 *
 * Subclasses define how messages are dispatched
 * to the user listener (StandardConsumeService for parallel/sequential,
 * FifoConsumeService for strict FIFO ordering).
 */
abstract class ConsumeService
{
    use ClientTrait;

    protected Logger $logger;
    protected $messageListener;
    protected $consumer;
    protected int $maxAttempts = 5;

    /**
     * @param Logger $logger Logger instance
     * @param callable $messageListener User callback: function($messageView): int
     * @param object $consumer Reference to PushConsumer
     */
    public function __construct($logger, $messageListener, $consumer)
    {
        $this->logger = $logger;
        $this->messageListener = $messageListener;
        $this->consumer = $consumer;
    }

    /**
     * Consume messages from the given ProcessQueue.
     *
     * @param ProcessQueue $pq
     * @return void
     */
    abstract public function consume(ProcessQueue $pq): void;

    /**
     * Dispatch a single message to the user listener.
     *
     * @param object $messageView
     * @return mixed ConsumeResult::SUCCESS, ConsumeResult::FAILURE, ConsumeResultSuspend::SUSPEND, or the ConsumeResultSuspend instance
     */
    public function consumeMessage($messageView)
    {
        try {
            $result = call_user_func($this->messageListener, $messageView);

            // Handle ConsumeResultSuspend
            if ($result instanceof ConsumeResultSuspend) {
                if (method_exists($this->consumer, 'executeInterceptors')) {
                    $this->consumer->executeInterceptors(MessageHookPoints::CONSUME, [
                        'success' => false,
                        'messageId' => $this->extractMessageId($messageView),
                        'topic' => $this->extractTopic($messageView),
                    ]);
                }
                // Return the ConsumeResultSuspend instance with suspend time info
                return $result;
            }

            $success = $result !== ConsumeResult::FAILURE;

            if (method_exists($this->consumer, 'executeInterceptors')) {
                $this->consumer->executeInterceptors(MessageHookPoints::CONSUME, [
                    'success' => $success,
                    'messageId' => $this->extractMessageId($messageView),
                    'topic' => $this->extractTopic($messageView),
                ]);
            }

            return $result === ConsumeResult::FAILURE ? ConsumeResult::FAILURE : ConsumeResult::SUCCESS;
        } catch (\Throwable $e) {
            $this->logger->warning("ConsumeService listener threw exception: " . $e->getMessage());

            if (method_exists($this->consumer, 'executeInterceptors')) {
                $this->consumer->executeInterceptors(MessageHookPoints::CONSUME, [
                    'success' => false,
                    'messageId' => $this->extractMessageId($messageView),
                    'topic' => $this->extractTopic($messageView),
                ]);
            }

            return ConsumeResult::FAILURE;
        }
    }

    /**
     * ACK a message via gRPC.
     *
     * @param object $messageView
     * @return bool
     */
    public function ackMessage($messageView): bool
    {
        $receiptHandle = $this->extractReceiptHandle($messageView);
        $messageId = $this->extractMessageId($messageView);
        $topic = $this->extractTopic($messageView);

        if (!$receiptHandle) {
            $this->logger->warning("ConsumeService ackMessage: no receipt handle, skipping messageId={$messageId}");
            return false;
        }

        $namespace = $this->consumer->getNamespace();
        $groupResource = $this->consumer->getGroupResourceWithNamespace();
        $topicResource = $this->consumer->getTopicResource($topic);

        $entry = new AckMessageEntry();
        if ($messageId) {
            $entry->setMessageId($messageId);
        }
        $entry->setReceiptHandle($receiptHandle);

        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if ($sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic()) {
                $entry->setLiteTopic($sysProps->getLiteTopic());
            }
        }

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries([$entry]);

        $metadata = $this->buildMetadata(ClientConstants::GRPC_DEFAULT_TIMEOUT / 1000);

        $brokerClient = $this->getBrokerClient($messageView);
        $maxRetries = 3;
        $attempt = 0;
        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $brokerClient->AckMessage($request, $metadata, $this->getCallOptions())->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("ConsumeService ackMessage attempt {$attempt}: status error: " . $status->details);
                } elseif ($response->hasStatus()) {
                    $statusCode = $response->getStatus()->getCode();
                    if ($statusCode === 20000) {
                        $this->logger->debug("ConsumeService ackMessage success for messageId={$messageId}");
                        $this->executeAckInterceptor(true, $messageId, $topic);
                        return true;
                    }
                    if ($statusCode == 40003) {
                        $this->logger->warning("ConsumeService ackMessage invalid receipt handle, giving up");
                        $this->executeAckInterceptor(false, $messageId, $topic);
                        return false;
                    }
                    $this->logger->warning("ConsumeService ackMessage attempt {$attempt}: error code={$statusCode}, retrying");
                } else {
                    $this->logger->warning("ConsumeService ackMessage attempt {$attempt} response missing status, retrying");
                }
            } catch (\Exception $e) {
                $this->logger->warning("ConsumeService ackMessage attempt {$attempt} failed: " . $e->getMessage());
            }

            $attempt++;
            if ($attempt < $maxRetries) {
                SwooleCompat::sleep(1000000 * $attempt); // linear backoff: 1s, 2s
            }
        }
        $this->logger->warning("ConsumeService ackMessage exhausted {$maxRetries} retries for messageId={$messageId}");
        return false;
    }

    /**
     * NACK a message (change invisible duration for retry).
     *
     * @param object $messageView
     * @param int $deliveryAttempt Current delivery attempt number
     * @param int|null $invisibleDuration Override invisible duration in seconds (for ConsumeResultSuspend)
     * @return bool
     */
    public function nackMessage($messageView, $deliveryAttempt = 1, ?int $invisibleDuration = null): bool
    {
        $receiptHandle = $this->extractReceiptHandle($messageView);
        $messageId = $this->extractMessageId($messageView);
        $topic = $this->extractTopic($messageView);

        if (!$receiptHandle) {
            $this->logger->warning("ConsumeService nackMessage: no receipt handle, skipping");
            return false;
        }

        // Calculate retry delay: exponential backoff with cap at 30s, or use provided suspend time
        if ($invisibleDuration !== null) {
            $delaySeconds = $invisibleDuration;
        } else {
            $retryPolicy = null;
            if (method_exists($this->consumer, 'getRetryPolicy')) {
                $retryPolicy = $this->consumer->getRetryPolicy();
            }
            if ($retryPolicy !== null && method_exists($retryPolicy, 'getNextAttemptDelayMs')) {
                $delayMs = $retryPolicy->getNextAttemptDelayMs($deliveryAttempt);
                $delaySeconds = max(1, (int)ceil($delayMs / 1000));
            } else {
                $delaySeconds = min(pow(2, $deliveryAttempt - 1) * 10, 30);
            }
        }
        if ($delaySeconds < 1) {
            $delaySeconds = 1;
        }

        $groupResource = $this->consumer->getGroupResourceWithNamespace();
        $topicResource = $this->consumer->getTopicResource($topic);

        $duration = new Duration();
        $duration->setSeconds($delaySeconds);
        $duration->setNanos(0);

        $request = new ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);
        if ($messageId) {
            $request->setMessageId($messageId);
        }

        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if ($sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic()) {
                $request->setLiteTopic($sysProps->getLiteTopic());
                $request->setSuspend(true);
            }
        }

        $metadata = $this->buildMetadata(ClientConstants::GRPC_DEFAULT_TIMEOUT / 1000);

        $brokerClient = $this->getBrokerClient($messageView);
        $maxRetries = 3;
        $attempt = 0;
        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $brokerClient->ChangeInvisibleDuration($request, $metadata, $this->getCallOptions())->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("ConsumeService nackMessage attempt {$attempt}: status error: " . $status->details);
                } elseif ($response->hasStatus()) {
                    $statusCode = $response->getStatus()->getCode();
                    if ($statusCode === 20000) {
                        $this->logger->debug("ConsumeService nackMessage success for messageId={$messageId}");
                        $this->executeNackInterceptor(true, $messageId, $topic, $deliveryAttempt, $delaySeconds);
                        return true;
                    }
                    if ($statusCode == 40003) {
                        $this->logger->warning("ConsumeService nackMessage invalid receipt handle, giving up");
                        $this->executeNackInterceptor(false, $messageId, $topic, $deliveryAttempt, $delaySeconds);
                        return false;
                    }
                    $this->logger->warning("ConsumeService nackMessage attempt {$attempt}: error code={$statusCode}, retrying");
                } else {
                    $this->logger->warning("ConsumeService nackMessage attempt {$attempt} response missing status, retrying");
                }
            } catch (\Exception $e) {
                $this->logger->warning("ConsumeService nackMessage attempt {$attempt} failed: " . $e->getMessage());
            }
            $attempt++;
            if ($attempt < $maxRetries) {
                SwooleCompat::sleep(1000000 * $attempt);
            }
        }
        $this->logger->warning("ConsumeService nackMessage exhausted {$maxRetries} retries for messageId={$messageId}");
        return false;
    }

    /**
     * Forward a message to the dead letter queue.
     *
     * @param object $messageView
     * @return bool
     */
    public function forwardToDeadLetterQueue($messageView, $deliveryAttempt = null): bool
    {
        $receiptHandle = $this->extractReceiptHandle($messageView);
        $messageId = $this->extractMessageId($messageView);
        $topic = $this->extractTopic($messageView);
        $liteTopic = $this->extractLiteTopic($messageView);

        if (!$receiptHandle) {
            $this->logger->warning("ConsumeService forwardToDeadLetterQueue: no receipt handle, skipping");
            return false;
        }

        $groupResource = $this->consumer->getGroupResourceWithNamespace();
        $topicResource = $this->consumer->getTopicResource($topic);

        $request = new ForwardMessageToDeadLetterQueueRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        if ($messageId) {
            $request->setMessageId($messageId);
        }
        if ($liteTopic !== null) {
            $request->setLiteTopic($liteTopic);
        }
        $actualAttempt = $deliveryAttempt ?? $this->maxAttempts;
        $request->setDeliveryAttempt($actualAttempt);
        $request->setMaxDeliveryAttempts($this->maxAttempts);

        $metadata = $this->buildMetadata(ClientConstants::GRPC_DEFAULT_TIMEOUT / 1000);
        $brokerClient = $this->getBrokerClient($messageView);
        $maxRetries = 3;
        $attempt = 0;
        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $brokerClient->ForwardMessageToDeadLetterQueue($request, $metadata, $this->getCallOptions())->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("ConsumeService forwardToDeadLetterQueue attempt {$attempt}: " . $status->details);
                } else {
                    $this->logger->info("ConsumeService forwardToDeadLetterQueue success for messageId={$messageId}");
                    $this->executeDLQInterceptor(true, $messageId, $topic);
                    return true;
                }
            } catch (\Exception $e) {
                $this->logger->error("ConsumeService forwardToDeadLetterQueue exception: " . $e->getMessage());
            }
            $attempt++;
            if ($attempt < $maxRetries) {
                SwooleCompat::sleep(1000000 * $attempt);
            }
        }
        $this->logger->error("ConsumeService forwardToDeadLetterQueue exhausted {$maxRetries} retries for messageId={$messageId}");
        return false;
    }

    /**
     * Execute NACK interceptor hook.Overridden by FifoConsumeService.
     */
    protected function executeNackInterceptor($success, $messageId, $topic, $deliveryAttempt, $delaySeconds):  void
    {

    }

    /**
     * Execute DLQ interceptor hook.Overridden by FifoConsumeService.
     */
    protected function executeDLQInterceptor($success, $messageId, $topic, $deliveryAttempt, $delaySeconds):  void
    {

    }

    private function getBrokerClient($messageView)
    {
        $endpoints = $messageView->getEndpoints();
        if ($endpoints && method_exists($endpoints, 'getAddresses')) {
            $addresses = $endpoints->getAddresses();
            $addressesArray = ProtobufUtil::repeatedFieldToArray($addresses);
            if (!empty($addressesArray) && $addressesArray[0] !== null) {
                $address = $addressesArray[0];

                $brokerKey = $address->getHost() . ':' . $address->getPort();
                $this->logger->debug("ConsumerService getBrokerClient : routing to broker {$brokerKey}");
                
                // Use the same credentials as the consumer (inherited from ClientConfiguration)
                // Don't hardcode insecure credentials - let RpcClientManager handle defaults
                return RpcClientManager::getInstance()->getClient($brokerKey);
            }
        }
        return $this->consumer->getClient();
    }

    /**
     * Execute ACK interceptor on consumer.
     */
    private function executeAckInterceptor($success, $messageId, $topic)
    {
        if (method_exists($this->consumer, 'executeInterceptors')) {
            $this->consumer->executeInterceptors(MessageHookPoints::ACK, [
                'success' => $success,
                'messageId' => $messageId,
                'topic' => $topic,
            ]);
        }
    }

    // ClientTrait required methods
    protected function getCredentials(): ?SessionCredentials {
        return method_exists($this->consumer, 'getSessionCredentials') ? $this->consumer->getSessionCredentials() : null;
    }
    protected function getClientIdValue(): string { return method_exists($this->consumer, 'getClientId') ? $this->consumer->getClientId() : ''; }
    protected function getNamespaceValue(): string { return method_exists($this->consumer, 'getNamespace') ? $this->consumer->getNamespace() : ''; }
}

/**
 * StandardConsumeService - Sequential message consumption (Standard mode).
 *
 * Iterates through cached messages, invokes the listener for each sequentially,
 * then acks or nacks based on the result.
 */
class StandardConsumeService extends ConsumeService
{
    public function consume(ProcessQueue $pq): void
    {
        $messages = $pq->getCachedMessages();
        if (empty($messages)) {
            return;
        }

        // Copy messages list to avoid modification during iteration
        $toConsume = array_values($messages);
        $this->logger->debug("StandardConsumeService consuming " . count($toConsume) . " messages from queue");

        foreach ($toConsume as $messageView) {
            // Check if message was already evicted by a previous iteration
            if ($pq->isDropped()) {
                break;
            }

            if ($messageView->isCorrupted()) {
                $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
                $this->logger->error("StandardConsumeService: Message $messageId is corrupted");
                $pq->discardMessage($messageView);
                continue;
            }
            $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';

            $result = $this->consumeMessage($messageView);

            if ($result instanceof ConsumeResultSuspend) {
                $suspendSec = (int)ceil($result->getSuspendTimeMs() / 1000);
                $this->logger->debug('StandardConsumerService suspend for %d seconds, messageId: %s', $suspendSec, $messageId);
                $this->nackMessage($messageView, 1, $suspendSec);
            } elseif ($result === \Apache\Rocketmq\ConsumeResult::SUCCESS) {
                $this->logger->debug('StandardConsumerService consume success, messageId: %s', $messageId);
                $this->ackMessage($messageView);
            } else {
                $this->logger->debug('StandardConsumerService consume failed, messageId: %s', $messageId);
                $this->nackMessage($messageView);
            }
            $pq->evictMessage($messageView);
        }
    }
}

/**
 * FifoConsumeService - Strict FIFO order consumption.
 *
 * When enableFifoConsumeAccelerator is false: all messages consumed sequentially.
 * When enableFifoConsumeAccelerator is true: messages grouped by messageGroup key,
 * each group processed sequentially, but different groups in parallel.
 */
class FifoConsumeService extends ConsumeService
{
    private bool $enableFifoConsumeAccelerator = false;

    public function __construct($logger, $messageListener, $consumer, $enableFifoConsumeAccelerator = false)
    {
        parent::__construct($logger, $messageListener, $consumer);
        $this->enableFifoConsumeAccelerator = $enableFifoConsumeAccelerator;
    }

    /**
     * Get the group key for a message. Override in subclasses (e.g., liteTopic).
     */
    protected function getMessageGroupKey($messageView)
    {
        $sysProps = $messageView->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup()) {
            return $sysProps->getMessageGroup();
        }
        return 'default';
    }

    public function consume(ProcessQueue $pq): void
    {
        $messages = $pq->getCachedMessages();
        if (empty($messages)) {
            return;
        }

        if ($pq->isDropped()) {
            return;
        }

        if ($this->enableFifoConsumeAccelerator && count($messages) > 1) {
            $this->consumeWithAccelerator($pq, $messages);
        } else {
            $this->consumeSequentially($pq, $messages);
        }
    }

    /**
     * Sequential consumption (original behavior, accelerator disabled).
     */
    private function consumeSequentially(ProcessQueue $pq, $messages)
    {
        // Only consume the first message (head of queue) to preserve FIFO order
        $messageView = reset($messages);

        if ($pq->isDropped()) {
            return;
        }

        if ($messageView->isCorrupted()) {
            $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
            $this->logger->error("FifoConsumeService: Message $messageId is corrupted");
            $pq->discardFifoMessage($messageView);
            $next = next($messages);
            if ($next !== false) {
                $this->consumeSequentially($pq, $messages);
            }
            return;
        }
        $this->consumeFifoIteratively($pq, $messageView, 1);
    }

    /**
     * Accelerated FIFO consumption: group by messageGroup, process groups in parallel.
     * Each group is still processed one-at-a-time internally, but groups run concurrently.
     */
    private function consumeWithAccelerator(ProcessQueue $pq, $messages)
    {
        // Group messages by their group key
        $groupedMessages = [];
        foreach ($messages as $msg) {
            $groupKey = $this->getMessageGroupKey($msg);
            $groupedMessages[$groupKey][] = $msg;
        }

        $deliveryAttempts = [];
        foreach ($groupedMessages as $groupKey => $groupMsgs) {
            $deliveryAttempts[$groupKey] = 1;
        }
        $this->logger->debug("FifoConsumeService accelerator: " . count($groupedMessages) . " groups, " . count($messages) . " total messages");

        // Process each group's head message concurrently
        // In PHP's single-threaded model, we iterate through groups and consume
        // the head of each group one by one, rather than all from one group first.
        // This gives interleaved FIFO consumption across groups.
        $groupKeys = array_keys($groupedMessages);
        $hasMore = true;

        while ($hasMore && !$pq->isDropped()) {
            $hasMore = false;
            foreach ($groupKeys as $groupKey) {
                if (empty($groupedMessages[$groupKey])) {
                    continue;
                }

                $hasMore = true;
                $messageView = reset($groupedMessages[$groupKey]);

                if ($pq->isDropped()) {
                    return;
                }

                // Consume this message and remove it from the group
                array_shift($groupedMessages[$groupKey]);

                if ($messageView->isCorrupted()) {
                    $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
                    $this->logger->error("FifoConsumeService accelerator: Message $messageId is corrupted");
                    $pq->discardFifoMessage($messageView);
                    continue;
                }

                $result = $this->consumeMessage($messageView);

                if ($result === ConsumeResult::SUCCESS) {
                    $this->ackMessage($messageView);
                    $pq->evictMessage($messageView);
                } elseif ($result instanceof ConsumeResultSuspend) {
                    $this->handleSuspend($pq, $messageView, $result);
                } else {
                    $attempt = $deliveryAttempts[$groupKey];
                    // On failure, retry with delay, then continue with other groups
                    $this->handleFailure($pq, $messageView, $attempt);
                }
            }
        }
    }

    /**
     * Handle consumption failure with retry.
     */
    private function handleFailure(ProcessQueue $pq, $messageView, $deliveryAttempt)
    {
        if ($pq->isDropped()) {
            return;
        }

        if ($deliveryAttempt < $this->maxAttempts) {
            $this->logger->warning("FifoConsumeService message consume failed, attempt {$deliveryAttempt}/{$this->maxAttempts}");
            $this->nackMessage($messageView, $deliveryAttempt);
            $pq->evictMessage($messageView);
            $retryPolicy = null;
            if (method_exists($this->consumer, 'getRetryPolicy')) {
                $retryPolicy = $this->consumer->getRetryPolicy();
            }
            if ($retryPolicy !== null && method_exists($retryPolicy, 'getNextAttemptDelayMs')) {
                $delayMs = $retryPolicy->getNextAttemptDelayMs($deliveryAttempt);
            } else {
                $delayMs = min(pow(2, $deliveryAttempt - 1) * 10000, 30000);
            }
            SwooleCompat::sleep($delayMs * 1000);
            $this->consumeFifoIteratively($pq, $messageView, $deliveryAttempt + 1);
        } else {
            $this->logger->error("FifoConsumeService max attempts reached for message, forwarding to DLQ");
            $this->forwardToDeadLetterQueue($messageView, $deliveryAttempt);
            $pq->evictMessage($messageView);
        }
    }

    protected function executeNackInterceptor($success, $messageId, $topic, $deliveryAttempt, $delaySeconds):  void
    {
        if (method_exists($this->consumer, 'executeInterceptors')) {
            $this->consumer->executeInterceptors(MessageHookPoints::CHANGE_INVISIBLE_DURATION, [
                'success' => $success,
                'messageId' => $messageId,
                'topic' => $topic,
                'deliveryAttempt' => $deliveryAttempt,
                'delaySeconds' => $delaySeconds
            ]);
        }
    }

    protected function executeDLQInterceptor($success, $messageId, $topic, $deliveryAttempt, $delaySeconds):  void
    {
        if (method_exists($this->consumer, 'executeInterceptors')) {
            $this->consumer->executeInterceptors(MessageHookPoints::FORWARD_TO_DLQ, [
               'success' => $success,
               'messageId' => $messageId,
               'topic' => $topic,
            ]);
        }
    }

    /**
     * Handle suspension result in FIFO consumption.
     */
    protected function handleSuspend(ProcessQueue $pq, $messageView, ConsumeResultSuspend $suspendResult)
    {
        if ($pq->isDropped()) {
            return;
        }
        $suspendSec = (int)ceil($suspendResult->getSuspendTimeMs() / 1000);
        $this->nackMessage($messageView, 1, $suspendSec);
        $pq->evictMessage($messageView);
        SwooleCompat::sleep($suspendResult->getSuspendTimeMs() * 1000);
    }

    /**
     * Consume a message and handle retry/DLQ/SUSPEND logic recursively.
     */
    private function consumeFifoIteratively(ProcessQueue $pq, $messageView, $deliveryAttempt)
    {
        if ($pq->isDropped()) {
            return;
        }

        if ($messageView->isCorrupted()) {
            $messageId = method_exists($messageView, 'getMessageId') ? $messageView->getMessageId() : 'unknown';
            $this->logger->error("FifoConsumeService: discarding Message $messageId is corrupted");
            $pq->discardFifoMessage($messageView);
            return;
        }

        $result = $this->consumeMessage($messageView);

        if ($result === ConsumeResult::SUCCESS) {
            $this->ackMessage($messageView);
            $pq->evictMessage($messageView);
        } elseif ($result instanceof ConsumeResultSuspend) {
            $this->handleSuspend($pq, $messageView, $result);
        } else {
            $this->handleFailure($pq, $messageView, $deliveryAttempt);
        }
    }
}
