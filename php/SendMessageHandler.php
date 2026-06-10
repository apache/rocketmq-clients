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

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Encoding;
use Google\Protobuf\Timestamp;

/**
 * SendMessageHandler — Handles message sending, batching, and retry logic.
 *
 * Extracted from Producer to separate send concerns:
 * - send() / sendAsync(): single message sending
 * - sendBatch() / sendBatchAsync(): batch message sending
 * - Convenience builders: priority, delayed, FIFO messages
 * - Retry with deadline, queue rotation, and backoff
 * - Protobuf message enrichment (toProtobufMessage)
 *
 * Dependencies are injected; the handler has no lifecycle state of its own.
 * The Producer is responsible for running-state checks before delegating here.
 */
class SendMessageHandler
{
    private readonly Logger $logger;

    /**
     * @param MessagingServiceClient $client gRPC client for send calls
     * @param ProducerSettings $settings Producer configuration (retry, timeouts, etc.)
     * @param MessageValidator $validator Message validation and type detection
     * @param PublishingRouteManager $routeManager Route lookup and broker isolation
     * @param \Closure $interceptorExecutor fn(string $hookPoint, array $context): void
     * @param \Closure $metadataBuilder fn(?int $timeoutMs): array
     * @param \Closure $callOptionsResolver fn(?int $overrideTimeout): array
     * @param \Closure $operationTimeoutFn fn(string $operation): int (microseconds)
     */
    public function __construct(
        private readonly MessagingServiceClient $client,
        private readonly ProducerSettings $settings,
        private readonly MessageValidator $validator,
        private readonly PublishingRouteManager $routeManager,
        private readonly \Closure $interceptorExecutor,
        private readonly \Closure $metadataBuilder,
        private readonly \Closure $callOptionsResolver,
        private readonly \Closure $operationTimeoutFn,
    ) {
        $this->logger = Logger::getInstance('Producer');
    }

    // ==================== Send ====================

    /**
     * Send a single message with retry.
     *
     * @param Message $message The message to send
     * @return array{messageId: string, transactionId: string, recallHandle: string, code: int, message: string}
     * @throws \RuntimeException If no queue is available or all retries fail
     */
    public function send(Message $message): array
    {
        $this->validator->validateMessage($message);

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->routeManager->getPublishingLoadBalancer($topic);

        $sysProps = $message->getSystemProperties();
        $hasMessageGroup = $sysProps !== null && $sysProps->hasMessageGroup();
        if ($hasMessageGroup) {
            $messageQueue = $loadBalancer->takeMessageQueueByMessageGroup($sysProps->getMessageGroup());
            if (!$messageQueue) {
                throw new \RuntimeException(
                    "No available message queue for message group: {$sysProps->getMessageGroup()}"
                );
            }
            $candidates = [$messageQueue];
        } else {
            $candidates = $loadBalancer->takeMessageQueue(
                $this->routeManager->getIsolatedBrokerNames(),
                $this->settings->getMaxAttempts()
            );
            if (empty($candidates)) {
                throw new \RuntimeException("No available message queue for topic: {$topic}");
            }
        }

        if ($this->validator->isValidateMessageType()) {
            $msgType = $this->validator->detectMessageType($message, false);
            $loadBalancer->validateMessageTypeAgainstQueue($candidates[0], $msgType, $topic);
        }

        $request = $this->wrapSendMessageRequest([$message], $candidates[0]);
        return $this->sendMessageWithRetry($request, $message, $candidates, $this->settings->getMaxAttempts());
    }

    /**
     * Send a message asynchronously (Swoole coroutine or Generator fallback).
     *
     * @param Message $message The message to send
     * @return array|\Generator
     */
    public function sendAsync(Message $message): array|\Generator
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($message, $channel) {
                try {
                    $result = $this->send($message);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'exception' => $e]);
                }
            });
            $data = $channel->pop($this->settings->getRequestTimeout() / 1000.0);
            if ($data === false) {
                throw new \RuntimeException(
                    "Send async Request timeout {$this->settings->getRequestTimeout()}ms"
                );
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->sendSyncFallback($message);
    }

    // ==================== Batch Send ====================

    /**
     * Send a batch of messages.
     *
     * All messages must share the same topic. If any message has a messageGroup (FIFO),
     * all must belong to the same group. Message types must be uniform.
     *
     * @param array<Message> $messages Messages to send
     * @return array<array{messageId: string, transactionId: string, recallHandle: string, code: int, message: string}>
     * @throws \InvalidArgumentException If batch is empty, topics differ, or types/groups conflict
     * @throws \RuntimeException If no queue is available or all retries fail
     */
    public function sendBatch(array $messages): array
    {
        if (empty($messages)) {
            throw new \InvalidArgumentException("Batch messages cannot be empty");
        }

        $topic = $messages[0]->getTopic()->getName();
        $messageTypes = [];
        $messageGroups = [];
        $hasFifoMessage = false;
        foreach ($messages as $msg) {
            if ($msg->getTopic()->getName() !== $topic) {
                throw new \InvalidArgumentException("All messages in a batch must have the same topic");
            }
            $this->validator->validateMessage($msg);
            if ($this->validator->isValidateMessageType()) {
                $messageTypes[] = $this->validator->detectMessageType($msg, false);
            }
            $sysProps = $msg->getSystemProperties();
            if ($sysProps !== null && $sysProps->hasMessageGroup()) {
                $hasFifoMessage = true;
                $messageGroups[] = $sysProps->getMessageGroup();
            }
        }
        if ($this->validator->isValidateMessageType() && count(array_unique($messageTypes)) > 1) {
            throw new \InvalidArgumentException('Messages to send different message types , please check');
        }
        if ($hasFifoMessage && count(array_unique($messageGroups)) > 1) {
            throw new \InvalidArgumentException("FIFO messages to send have different message groups, please check");
        }

        $loadBalancer = $this->routeManager->getPublishingLoadBalancer($topic);
        $isolatedBroker = $this->routeManager->getIsolatedBrokerNames();

        if ($hasFifoMessage) {
            $messageGroup = $messageGroups[0];
            $mq = $loadBalancer->takeMessageQueueByMessageGroup($messageGroup);
            $messageQueue = $mq !== null ? [$mq] : [];
        } else {
            $messageQueue = $loadBalancer->takeMessageQueue($isolatedBroker, $this->settings->getMaxAttempts());
        }
        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }

        $request = $this->wrapSendMessageRequest($messages, $messageQueue[0]);
        return $this->sendBatchWithRetry($request, $messages, $messageQueue, $this->settings->getMaxAttempts());
    }

    /**
     * Send a batch of messages asynchronously (Swoole coroutine or Generator fallback).
     *
     * @param array<Message> $messages Messages to send
     * @return array|\Generator
     */
    public function sendBatchAsync(array $messages): array|\Generator
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($messages, $channel) {
                try {
                    $result = $this->sendBatch($messages);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'exception' => $e]);
                }
            });
            $data = $channel->pop($this->settings->getRequestTimeout() / 1000.0);
            if ($data === false) {
                throw new \RuntimeException(
                    "Send batch async Request timeout {$this->settings->getRequestTimeout()}ms"
                );
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->sendBatchSyncFallback($messages);
    }

    // ==================== Convenience Builders ====================

    /**
     * Build a message with custom system properties (used by convenience send methods).
     *
     * @param string $topic Topic name
     * @param string $body Message body
     * @param string $tag Optional message tag
     * @param callable $configurator fn(SystemProperties): void to set priority/group/delay
     * @return Message
     */
    public function buildConvenienceMessage(string $topic, string $body, string $tag, callable $configurator): Message
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }
        $configurator($sysProps);

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $message;
    }

    // ==================== Message Building ====================

    /**
     * Detect message type via MessageValidator.
     *
     * @param Message $msg
     * @param bool $txEnabled Whether to consider TRANSACTION type
     * @return int MessageType constant
     */
    public function detectMessageType(Message $msg, bool $txEnabled = false): int
    {
        return $this->validator->detectMessageType($msg, $txEnabled);
    }

    private function createTimestamp(): Timestamp
    {
        $now = microtime(true);
        $timestamp = new Timestamp();
        $timestamp->setSeconds((int)$now);
        $timestamp->setNanos((int)(($now - (int)$now) * 1000000000));
        return $timestamp;
    }

    /**
     * Convert a user-facing Message into a fully enriched protobuf Message for sending.
     *
     * Assigns messageId, bornTimestamp, bornHost, encoding, queueId, messageType,
     * and copies over optional fields (tag, keys, messageGroup, deliveryTimestamp,
     * liteTopic, priority, traceContext) from the input message.
     */
    private function toProtobufMessage(Message $msg, object $messageQueue, bool $txEnabled = false): Message
    {
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');

        // Preserve encoding from input message; default to IDENTITY
        $inputSysProps = $msg->getSystemProperties();
        $encoding = Encoding::IDENTITY;
        if ($inputSysProps !== null) {
            $inputEncoding = $inputSysProps->getBodyEncoding();
            if ($inputEncoding !== Encoding::ENCODING_UNSPECIFIED) {
                $encoding = $inputEncoding;
            }
        }
        $systemProperties->setBodyEncoding($encoding);
        $queueId = $messageQueue->getId();
        if ($queueId !== null) {
            $systemProperties->setQueueId($queueId);
        }
        $systemProperties->setMessageType($this->detectMessageType($msg, $txEnabled));

        if ($inputSysProps) {
            if ($inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (!ProtobufUtil::isRepeatedFieldEmpty($inputSysProps->getKeys())) {
                $systemProperties->setKeys($inputSysProps->getKeys());
            }
            if ($inputSysProps->hasMessageGroup()) {
                $systemProperties->setMessageGroup($inputSysProps->getMessageGroup());
            }
            if ($inputSysProps->hasDeliveryTimestamp()) {
                $systemProperties->setDeliveryTimestamp($inputSysProps->getDeliveryTimestamp());
            }
            if ($inputSysProps->hasLiteTopic()) {
                $systemProperties->setLiteTopic($inputSysProps->getLiteTopic());
            }
            if ($inputSysProps->hasPriority()) {
                $systemProperties->setPriority($inputSysProps->getPriority());
            }
            if ($inputSysProps->hasTraceContext()) {
                $systemProperties->setTraceContext($inputSysProps->getTraceContext());
            }
        }

        $topicResource = new Resource();
        $topicResource->setName($msg->getTopic()->getName());

        $protoMsg = new Message();
        $protoMsg->setTopic($topicResource);
        $protoMsg->setBody($msg->getBody());
        $protoMsg->setSystemProperties($systemProperties);

        $userProps = $msg->getUserProperties();
        if (!ProtobufUtil::isMapFieldEmpty($userProps)) {
            foreach ($userProps as $key => $value) {
                $protoMsg->getUserProperties()[$key] = $value;
            }
        }

        return $protoMsg;
    }

    /**
     * Wrap messages into a SendMessageRequest (non-transaction).
     */
    public function wrapSendMessageRequest(array $messages, object $messageQueue): SendMessageRequest
    {
        $enriched = [];
        foreach ($messages as $msg) {
            $enriched[] = $this->toProtobufMessage($msg, $messageQueue);
        }
        $request = new SendMessageRequest();
        $request->setMessages($enriched);
        return $request;
    }

    /**
     * Wrap messages into a SendMessageRequest with transaction message type.
     *
     * Used by TransactionTrait for half-message sending.
     */
    public function wrapTransactionMessageRequest(array $messages, object $messageQueue): SendMessageRequest
    {
        $enriched = [];
        foreach ($messages as $msg) {
            $enriched[] = $this->toProtobufMessage($msg, $messageQueue, true);
        }
        $request = new SendMessageRequest();
        $request->setMessages($enriched);
        return $request;
    }

    // ==================== Retry Logic ====================

    /**
     * Send a single message with retry, deadline, and queue rotation.
     *
     * On each failed attempt, the failed broker endpoint is isolated and the
     * next candidate queue is tried. Retry delay follows the configured
     * ExponentialBackoffRetryPolicy with jitter.
     *
     * @param SendMessageRequest $request The gRPC request
     * @param Message $message The original user message (for interceptor context)
     * @param array $candidates Candidate message queues for rotation
     * @param int $maxAttempts Maximum number of attempts
     * @return array{messageId: string, transactionId: string, recallHandle: string, code: int, message: string}
     * @throws \RuntimeException If deadline exceeded or all attempts fail
     */
    public function sendMessageWithRetry(
        SendMessageRequest $request,
        Message $message,
        array $candidates,
        int $maxAttempts
    ): array {
        $lastException = null;
        $startTime = microtime(true);
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        $operationTimeout = ($this->operationTimeoutFn)('SEND_MESSAGE');
        $deadlineMicroseconds = $startTime + ($operationTimeout / 1000000);

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            $now = microtime(true);
            if ($now >= $deadlineMicroseconds) {
                throw new \RuntimeException(
                    "Send message deadline exceeded after " .
                    round(($now - $startTime) * 1000, 2) . "ms"
                );
            }

            if ($attempt > 1 && $candidateCount > 1) {
                $queueIndex = IntMath::mod($attempt, $candidateCount);
                $currentMessageQueue = $candidates[$queueIndex];
                $request = $this->wrapSendMessageRequest([$message], $currentMessageQueue);
            }
            try {
                $remainingTimeUs = max(1000000, ($deadlineMicroseconds - microtime(true)) * 1000000);
                $remainingTimeMs = (int)($remainingTimeUs / 1000);
                $metadata = ($this->metadataBuilder)($remainingTimeMs);
                $callOptions = ['timeout' => min($remainingTimeUs, ClientConstants::GRPC_SEND_MESSAGE_TIMEOUT)];

                list($response, $status) = $this->client->SendMessage(
                    $request, $metadata, $callOptions
                )->wait();

                if ($status->code !== 0) {
                    throw new \RuntimeException("Send message failed: " . $status->details);
                }

                $entries = $response->getEntries()
                    ? ProtobufUtil::repeatedFieldToArray($response->getEntries())
                    : [];

                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException(
                            "SendMessage failed with code: " . $respStatus->getCode() .
                            ", message: " . $respStatus->getMessage()
                        );
                    }
                }

                if (count($entries) > 0) {
                    $entry = $entries[0];
                    $resultStatus = $entry->getStatus();

                    if ($resultStatus->getCode() !== 20000) {
                        throw new \RuntimeException(
                            "Send message failed with code: " . $resultStatus->getCode()
                        );
                    }

                    $latencyMs = (microtime(true) - $startTime) * 1000;
                    ($this->interceptorExecutor)(MessageHookPoints::SEND, [
                        'success' => true,
                        'latencyMs' => $latencyMs,
                        'topic' => $message->getTopic()->getName(),
                        'messageType' => $this->detectMessageType($message, false),
                        'sendReceipts' => [
                            'messageId' => $entry->getMessageId(),
                            'transactionId' => $entry->getTransactionId(),
                        ]
                    ]);

                    return [
                        'messageId' => $entry->getMessageId(),
                        'transactionId' => $entry->getTransactionId(),
                        'recallHandle' => $entry->getRecallHandle() ?? '',
                        'code' => $resultStatus->getCode(),
                        'message' => $resultStatus->getMessage(),
                    ];
                }

                throw new \RuntimeException("No response entries");

            } catch (\Exception $e) {
                $lastException = $e;
                $this->logger->error("Send attempt {$attempt} failed: " . $e->getMessage());

                $failedEndpoints = PublishingRouteManager::extractMessageQueueEndpoint($currentMessageQueue);
                if ($failedEndpoints !== null) {
                    $this->routeManager->isolateEndpoints($failedEndpoints);
                }

                if ($attempt < $maxAttempts) {
                    $delayMs = $this->settings->getRetryPolicy()->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        SwooleCompat::sleep($delayMs * 1000);
                    }
                }
            }
        }

        $latencyMs = (microtime(true) - $startTime) * 1000;
        ($this->interceptorExecutor)(MessageHookPoints::SEND, [
            'success' => false,
            'latencyMs' => $latencyMs,
            'topic' => $message->getTopic()->getName(),
            'messageType' => $this->detectMessageType($message, false),
            'sendException' => $lastException ? $lastException->getMessage() : '',
        ]);
        throw $lastException;
    }

    /**
     * Send a batch of messages with retry, deadline, and queue rotation.
     *
     * Verifies that the response entry count matches the request message count.
     * Any non-OK entry triggers a retry of the entire batch.
     *
     * @param SendMessageRequest $request The gRPC request
     * @param array<Message> $messages The original messages (for interceptor context)
     * @param array $candidates Candidate message queues for rotation
     * @param int $maxAttempts Maximum number of attempts
     * @return array<array{messageId: string, transactionId: string, recallHandle: string, code: int, message: string}>
     * @throws \RuntimeException If deadline exceeded or all attempts fail
     */
    public function sendBatchWithRetry(
        SendMessageRequest $request,
        array $messages,
        array $candidates,
        int $maxAttempts
    ): array {
        $lastException = null;
        $startTime = microtime(true);
        $topic = $messages[0]->getTopic()->getName();
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        $operationTimeout = ($this->operationTimeoutFn)('SEND_MESSAGE');
        $deadlineMicroseconds = $startTime + ($operationTimeout / 1000000);

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            $now = microtime(true);
            if ($now >= $deadlineMicroseconds) {
                throw new \RuntimeException(
                    "Batch send deadline exceeded after " .
                    round(($now - $startTime) * 1000, 2) . "ms"
                );
            }

            if ($attempt > 1 && $candidateCount > 1) {
                $queueIndex = IntMath::mod($attempt, $candidateCount);
                $currentMessageQueue = $candidates[$queueIndex];
                $request = $this->wrapSendMessageRequest($messages, $currentMessageQueue);
            }
            try {
                $remainingTimeUs = max(1000000, ($deadlineMicroseconds - microtime(true)) * 1000000);
                $remainingTimeMs = (int)($remainingTimeUs / 1000);
                $metadata = ($this->metadataBuilder)($remainingTimeMs);
                $callOptions = ['timeout' => min($remainingTimeUs, ClientConstants::GRPC_SEND_MESSAGE_TIMEOUT)];

                list($response, $status) = $this->client->SendMessage(
                    $request, $metadata, $callOptions
                )->wait();

                if ($status->code !== 0) {
                    throw new \RuntimeException("Batch send failed: " . $status->details);
                }

                $entries = $response->getEntries()
                    ? ProtobufUtil::repeatedFieldToArray($response->getEntries())
                    : [];

                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException(
                            "Batch send failed with code: " . $respStatus->getCode() .
                            ", message: " . $respStatus->getMessage()
                        );
                    }
                }

                // Verify response entry count matches request message count
                $entryCount = count($entries);
                $messageCount = count($messages);
                if ($entryCount !== $messageCount) {
                    throw new \RuntimeException(
                        "Batch response entry count ({$entryCount}) does not match " .
                        "request message count ({$messageCount})"
                    );
                }

                // Fail the batch on any non-OK entry to trigger retry
                $results = [];
                foreach ($entries as $i => $entry) {
                    $entryStatus = $entry->getStatus();
                    $code = $entryStatus ? $entryStatus->getCode() : 0;
                    $msg = $entryStatus ? $entryStatus->getMessage() : 'No status';

                    if ($code !== 20000) {
                        throw new \RuntimeException(
                            "Batch entry {$i} failed with code: {$code}, message: {$msg}"
                        );
                    }

                    $results[] = [
                        'messageId' => $entry->getMessageId(),
                        'transactionId' => $entry->getTransactionId() ?? '',
                        'recallHandle' => $entry->getRecallHandle() ?? '',
                        'code' => $code,
                        'message' => $msg,
                    ];
                }

                $latencyMs = (microtime(true) - $startTime) * 1000;
                ($this->interceptorExecutor)(MessageHookPoints::SEND, [
                    'success' => true,
                    'latencyMs' => $latencyMs,
                    'topic' => $topic,
                ]);

                return $results;

            } catch (\Exception $e) {
                $lastException = $e;
                $this->logger->error("Batch send attempt {$attempt} failed: " . $e->getMessage());

                $failedEndpoints = PublishingRouteManager::extractMessageQueueEndpoint($currentMessageQueue);
                if ($failedEndpoints !== null) {
                    $this->routeManager->isolateEndpoints($failedEndpoints);
                }

                if ($attempt < $maxAttempts) {
                    $delayMs = $this->settings->getRetryPolicy()->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        SwooleCompat::sleep($delayMs * 1000);
                    }
                }
            }
        }

        $latencyMs = (microtime(true) - $startTime) * 1000;
        ($this->interceptorExecutor)(MessageHookPoints::SEND, [
            'success' => false,
            'latencyMs' => $latencyMs,
            'topic' => $topic,
        ]);
        throw $lastException;
    }

    // ==================== Sync Fallbacks ====================

    /**
     * Generator fallback for sendAsync when Swoole is not available.
     *
     * @param Message $message
     * @return \Generator
     */
    private function sendSyncFallback(Message $message): \Generator
    {
        yield $this->send($message);
    }

    /**
     * Generator fallback for sendBatchAsync when Swoole is not available.
     *
     * @param array<Message> $messages
     * @return \Generator
     */
    private function sendBatchSyncFallback(array $messages): \Generator
    {
        yield $this->sendBatch($messages);
    }
}
