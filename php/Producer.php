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
use Apache\Rocketmq\V2\RecallMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\MessageType as V2MessageType;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;

/**
 * Producer — Message producer.
 *
 * Core features: singleton TelemetrySession, PublishingLoadBalancer, retry,
 * transaction support (via TransactionTrait), heartbeat (via HeartbeatManager),
 * interceptor support, Swoole coroutine async support.
 *
 * Use ProducerBuilder for convenient construction.
 */
class Producer implements TransactionCommitter, ClientTraitProvider
{
    use ClientTrait {
        buildMetadata as public;
        getOperationTimeout as public;
        parseEndpoints as public;
    }
    use TransactionTrait;

    private readonly MessagingServiceClient $client;
    private readonly string $clientId;
    private readonly TelemetrySession $telemetrySession;
    private readonly PublishingRouteManager $routeManager;
    private bool $isRunning = false;
    private bool $shutdownRequested = false;
    private int $maxAttempts = 3;
    private int $requestTimeout = 3000;
    private array $topics = [];
    private readonly string $namespace;
    private readonly Logger $logger;
    private ?SessionCredentials $credentials = null;
    private bool $validateMessageType = true;
    private int $maxBodySizeBytes = 4194304;
    private array $interceptors = [];
    private ?ExponentialBackoffRetryPolicy $retryPolicy = null;
    private readonly ?TlsCredentials $tlsCredentials;
    private readonly HeartbeatManager $heartbeatManager;

    /**
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options (use ProducerBuilder instead)
     * @deprecated Use ProducerBuilder instead.
     */
    public function __construct(
        private readonly string $endpoints,
        array $options = []
    ) {
        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        $this->maxAttempts = $options['maxAttempts'] ?? 3;
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->topics = $options['topics'] ?? [];
        $this->namespace = $options['namespace'] ?? '';
        $this->validateMessageType = $options['validateMessageType'] ?? true;
        $this->maxBodySizeBytes = $options['maxBodySizeBytes'] ?? 4194304;
        $this->tlsCredentials = $options['tlsCredentials'] ?? null;

        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        $this->retryPolicy = new ExponentialBackoffRetryPolicy($this->maxAttempts, 1000, 30000, 2.0);
        $this->logger = Logger::getInstance('Producer');

        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials, $this->namespace);
        $this->routeManager = new PublishingRouteManager($this->client, $endpoints, $this);
        $this->heartbeatManager = new HeartbeatManager($this->routeManager, $this->client, $this, $this->tlsCredentials);
    }

    // ==================== Lifecycle ====================

    public function start(): void
    {
        if ($this->isRunning) {
            return;
        }

        try {
            Logger::getInstance('Producer')->info("Begin to start the rocketmq producer, clientId={$this->clientId}");
            $this->establishTelemetrySession();
            $this->registerSettingsCallback();
            $this->registerTransactionCheckerCallback();

            $this->routeManager->warmUp($this->topics);

            $this->isRunning = true;
            $this->heartbeatManager->start();

            Logger::getInstance('Producer')->info("The rocketmq producer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            Logger::getInstance('Producer')->error("Failed to start: " . $e->getMessage());
            $this->shutdown();
            throw $e;
        }
    }

    public function shutdown(): void
    {
        if (!$this->isRunning) {
            return;
        }

        $this->shutdownRequested = true;
        $this->logger->info("Begin to shutdown the rocketmq producer, clientId={$this->clientId}");

        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            \Swoole\Coroutine::sleep(1);
        }

        $this->heartbeatManager->stop();
        $this->heartbeatManager->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->isRunning = false;
        $this->logger->info("Shutdown the rocketmq producer successfully, clientId={$this->clientId}");
    }

    public function __destruct()
    {
        $this->shutdown();
    }

    // ==================== Send ====================

    public function send(Message $message): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $this->validateMessage($message);

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->routeManager->getPublishingLoadBalancer($topic);

        $sysProps = $message->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        if ($hasMessageGroup) {
            $messageQueue = $loadBalancer->takeMessageQueueByMessageGroup($sysProps->getMessageGroup());
            if (!$messageQueue) {
                throw new \RuntimeException("No available message queue for message group: {$sysProps->getMessageGroup()}");
            }
            $candidates = [$messageQueue];
        } else {
            $candidates = $loadBalancer->takeMessageQueue($this->routeManager->getIsolatedBrokerNames(), $this->maxAttempts);
            if (empty($candidates)) {
                throw new \RuntimeException("No available message queue for topic: {$topic}");
            }
        }

        if ($this->validateMessageType) {
            $msgType = $this->detectMessageType($message, false);
            $loadBalancer->validateMessageTypeAgainstQueue($candidates[0], $msgType, $topic);
        }

        $request = $this->wrapSendMessageRequest([$message], $candidates[0]);
        return $this->sendMessageWithRetry($request, $message, $candidates, $this->maxAttempts);
    }

    public function sendAsync(Message $message)
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
            $data = $channel->pop($this->requestTimeout / 1000.0);
            if ($data === false) {
                throw new \RuntimeException("Send async Request timeout {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->sendSyncFallback($message);
    }

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

    // ==================== Batch Send ====================

    public function sendBatch(array $messages): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

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
            $this->validateMessage($msg);
            if ($this->validateMessageType) {
                $messageTypes[] = $this->detectMessageType($msg, false);
            }
            $sysProps = $msg->getSystemProperties();
            if ($sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup()) {
                $hasFifoMessage = true;
                $messageGroups[] = $sysProps->getMessageGroup();
            }
        }
        if ($this->validateMessageType && count(array_unique($messageTypes)) > 1) {
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
            $messageQueue = $loadBalancer->takeMessageQueue($isolatedBroker, $this->maxAttempts);
        }
        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }

        $request = $this->wrapSendMessageRequest($messages, $messageQueue[0]);
        return $this->sendBatchWithRetry($request, $messages, $messageQueue, $this->maxAttempts);
    }

    public function sendBatchAsync(array $messages)
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
            $data = $channel->pop($this->requestTimeout / 1000.0);
            if ($data === false) {
                throw new \RuntimeException("Send batch async Request timeout {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->sendBatchSyncFallback($messages);
    }

    /**
     * Generator fallback for sendBatchAsync when Swoole is not available.
     *
     * @param array $messages
     * @return \Generator
     */
    private function sendBatchSyncFallback(array $messages): \Generator
    {
        yield $this->sendBatch($messages);
    }

    // ==================== Convenience Send Methods ====================

    public function sendPriorityMessage($topic, $body, $priority, $tag = ''): array
    {
        return $this->send($this->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($priority) {
            $sp->setPriority($priority);
        }));
    }

    public function sendDelayedMessage($topic, $body, $deliveryTimestampUnixSec, $tag = ''): array
    {
        $ts = new Timestamp();
        $ts->setSeconds($deliveryTimestampUnixSec);
        $ts->setNanos(0);

        return $this->send($this->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($ts) {
            $sp->setDeliveryTimestamp($ts);
        }));
    }

    public function sendFifoMessage($topic, $body, $messageGroup, $tag = ''): array
    {
        return $this->send($this->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($messageGroup) {
            $sp->setMessageGroup($messageGroup);
        }));
    }

    // ==================== Recall ====================

    public function recallMessage($topic, $recallHandle): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new RecallMessageRequest();
        $request->setTopic($topicResource);
        $request->setRecallHandle($recallHandle);

        $metadata = $this->buildMetadata($this->requestTimeout);
        list($response, $status) = $this->client->RecallMessage($request, $metadata, $this->getCallOptions())->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Recall message failed: " . $status->details);
        }

        return [
            'messageId' => method_exists($response, 'getMessageId') ? $response->getMessageId() : '',
            'status' => $response->getStatus(),
        ];
    }

    public function recallMessageAsync($topic, $recallHandle)
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($topic, $recallHandle, $channel) {
                try {
                    $result = $this->recallMessage($topic, $recallHandle);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'exception' => $e]);
                }
            });
            $data = $channel->pop($this->requestTimeout / 1000.0);
            if ($data === false) {
                throw new \RuntimeException("Recall message async Request timeout {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->recallMessageSyncFallback($topic, $recallHandle);
    }

    /**
     * Generator fallback for recallMessageAsync when Swoole is not available.
     *
     * @param string $topic
     * @param string $recallHandle
     * @return \Generator
     */
    private function recallMessageSyncFallback($topic, $recallHandle): \Generator
    {
        yield $this->recallMessage($topic, $recallHandle);
    }

    // ==================== Interceptors ====================

    public function addInterceptor(MessageInterceptor $interceptor)
    {
        if (!isset($this->interceptors)) {
            $this->interceptors = [];
        }
        $this->interceptors[] = $interceptor;
        return $this;
    }

    public function executeInterceptors($hookPoint, $context = [])
    {
        if (empty($this->interceptors)) {
            return;
        }
        foreach ($this->interceptors as $interceptor) {
            try {
                $interceptor->intercept($hookPoint, $context);
            } catch (\Exception $e) {
                $this->logger->warning("Interceptor failed at {$hookPoint}: " . $e->getMessage());
            }
        }
    }

    // ==================== Getters ====================

    public function getClientId(): string { return $this->clientId; }
    public function isRunning(): bool { return $this->isRunning; }

    // ==================== ClientTrait Required Methods ====================

    protected function getCredentials(): ?SessionCredentials { return $this->credentials; }
    protected function getClientIdValue(): string { return $this->clientId; }
    protected function getNamespaceValue(): string { return $this->namespace; }

    // ==================== Private: Telemetry & Settings ====================

    private function establishTelemetrySession()
    {
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

        $publishing = new Publishing();
        $topicResources = [];
        foreach ($this->topics as $topicName) {
            $topicResource = new Resource();
            $topicResource->setName($topicName);
            $topicResources[] = $topicResource;
        }
        $publishing->setTopics($topicResources);

        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);
        $settings->setPublishing($publishing);

        $command = new TelemetryCommand();
        $command->setSettings($settings);

        if (!$this->telemetrySession->syncSettings($command)) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        SwooleCompat::sleep(500000);
    }

    private function registerSettingsCallback()
    {
        $self = $this;
        $this->telemetrySession->setOnSettingsChange(function ($settings) use ($self) {
            $self->onServerSettings($settings);
        });
    }

    private function onServerSettings($settings)
    {
        $this->logger->info("Processing server settings");

        if ($settings->hasPublishing()) {
            $publishing = $settings->getPublishing();
            if (method_exists($publishing, 'getMaxBodySize') && $publishing->getMaxBodySize() > 0) {
                $this->maxBodySizeBytes = $publishing->getMaxBodySize();
                $this->logger->info("Updated maxBodySize from server: {$this->maxBodySizeBytes}");
            }
            if (method_exists($publishing, 'getValidateMessageType')) {
                $this->validateMessageType = $publishing->getValidateMessageType();
                $this->logger->info("Updated validateMessageType from server: " . ($this->validateMessageType ? 'true' : 'false'));
            }
        }

        if (method_exists($settings, 'getBackoffPolicy') && $settings->hasBackoffPolicy()) {
            $serverPolicy = $settings->getBackoffPolicy();
            $this->logger->info("Received backoff policy from server");
            if (method_exists($serverPolicy, 'getDurations') && !ProtobufUtil::isRepeatedFieldEmpty($serverPolicy->getDurations())) {
                $this->retryPolicy = CustomizedBackoffRetryPolicy::fromProtobuf($serverPolicy);
                $this->logger->info("Updated retry policy from server backoff");
            }
        }
    }

    // ==================== Private: Message Validation & Building ====================

    private function validateMessage(Message $message)
    {
        if (!$message->hasTopic() || empty(trim($message->getTopic()->getName()))) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        if (empty($message->getBody())) {
            throw new \InvalidArgumentException("Message body is required");
        }
        if (strlen($message->getBody()) > $this->maxBodySizeBytes) {
            $mb = $this->maxBodySizeBytes / (1024 * 1024);
            throw new \InvalidArgumentException("Message size exceeds limit ({$mb}MB)");
        }
    }

    private function buildConvenienceMessage(string $topic, string $body, string $tag, callable $configurator): Message
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

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

    private function detectMessageType(Message $msg, bool $txEnabled = false): int
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();

        return match (true) {
            $txEnabled && !$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp
                => V2MessageType::TRANSACTION,
            !$txEnabled && $hasMessageGroup => V2MessageType::FIFO,
            !$txEnabled && $hasLiteTopic => V2MessageType::LITE,
            !$txEnabled && $hasDeliveryTimestamp => V2MessageType::DELAY,
            !$txEnabled && $hasPriority => V2MessageType::PRIORITY,
            default => V2MessageType::NORMAL,
        };
    }

    private function createTimestamp()
    {
        $now = microtime(true);
        $timestamp = new Timestamp();
        $timestamp->setSeconds((int)$now);
        $timestamp->setNanos((int)(($now - (int)$now) * 1000000000));
        return $timestamp;
    }

    private function toProtobufMessage(Message $msg, $messageQueue, $txEnabled = false)
    {
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');

        // Preserve encoding from input message; default to IDENTITY
        $inputSysProps = $msg->getSystemProperties();
        $encoding = Encoding::IDENTITY;
        if ($inputSysProps && method_exists($inputSysProps, 'getBodyEncoding')) {
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
            if (method_exists($inputSysProps, 'getTag') && $inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (method_exists($inputSysProps, 'getKeys') && !ProtobufUtil::isRepeatedFieldEmpty($inputSysProps->getKeys())) {
                $systemProperties->setKeys($inputSysProps->getKeys());
            }
            if (method_exists($inputSysProps, 'getMessageGroup') && $inputSysProps->hasMessageGroup()) {
                $systemProperties->setMessageGroup($inputSysProps->getMessageGroup());
            }
            if (method_exists($inputSysProps, 'getDeliveryTimestamp') && $inputSysProps->hasDeliveryTimestamp()) {
                $systemProperties->setDeliveryTimestamp($inputSysProps->getDeliveryTimestamp());
            }
            if (method_exists($inputSysProps, 'getLiteTopic') && $inputSysProps->hasLiteTopic()) {
                $systemProperties->setLiteTopic($inputSysProps->getLiteTopic());
            }
            if (method_exists($inputSysProps, 'getPriority') && $inputSysProps->hasPriority()) {
                $systemProperties->setPriority($inputSysProps->getPriority());
            }
            if (method_exists($inputSysProps, 'getTraceContext') && $inputSysProps->hasTraceContext()) {
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

    private function wrapSendMessageRequest($messages, $messageQueue)
    {
        $enriched = [];
        foreach ($messages as $msg) {
            $enriched[] = $this->toProtobufMessage($msg, $messageQueue);
        }
        $request = new SendMessageRequest();
        $request->setMessages($enriched);
        return $request;
    }

    private function wrapTransactionMessageRequest($messages, $messageQueue)
    {
        $enriched = [];
        foreach ($messages as $msg) {
            $enriched[] = $this->toProtobufMessage($msg, $messageQueue, true);
        }
        $request = new SendMessageRequest();
        $request->setMessages($enriched);
        return $request;
    }

    // ==================== Private: Retry Logic ====================

    private function sendMessageWithRetry($request, $message, $candidates, $maxAttempts)
    {
        $lastException = null;
        $startTime = microtime(true);
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        $operationTimeout = $this->getOperationTimeout('SEND_MESSAGE');
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
                $metadata = $this->buildMetadata($remainingTimeMs);
                $callOptions = ['timeout' => min($remainingTimeUs, ClientConstants::GRPC_SEND_MESSAGE_TIMEOUT)];

                list($response, $status) = $this->client->SendMessage($request, $metadata, $callOptions)->wait();

                if ($status->code !== 0) {
                    throw new \RuntimeException("Send message failed: " . $status->details);
                }

                $entries = $response->getEntries();

                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException("SendMessage failed with code: " . $respStatus->getCode() . ", message: " . $respStatus->getMessage());
                    }
                }

                if (count($entries) > 0) {
                    $entry = $entries[0];
                    $resultStatus = $entry->getStatus();

                    if ($resultStatus->getCode() !== 20000) {
                        throw new \RuntimeException("Send message failed with code: " . $resultStatus->getCode());
                    }

                    $latencyMs = (microtime(true) - $startTime) * 1000;
                    $this->executeInterceptors(MessageHookPoints::SEND, [
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
                    $delayMs = $this->retryPolicy->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        SwooleCompat::sleep($delayMs * 1000);
                    }
                }
            }
        }

        $latencyMs = (microtime(true) - $startTime) * 1000;
        $this->executeInterceptors(MessageHookPoints::SEND, [
            'success' => false,
            'latencyMs' => $latencyMs,
            'topic' => $message->getTopic()->getName(),
            'messageType' => $this->detectMessageType($message, false),
            'sendException' => $lastException ? $lastException->getMessage() : '',
        ]);
        throw $lastException;
    }

    private function sendBatchWithRetry($request, $messages, $candidates, $maxAttempts)
    {
        $lastException = null;
        $startTime = microtime(true);
        $topic = $messages[0]->getTopic()->getName();
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        $operationTimeout = $this->getOperationTimeout('SEND_MESSAGE');
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
                $metadata = $this->buildMetadata($remainingTimeMs);
                $callOptions = ['timeout' => min($remainingTimeUs, ClientConstants::GRPC_SEND_MESSAGE_TIMEOUT)];

                list($response, $status) = $this->client->SendMessage($request, $metadata, $callOptions)->wait();

                if ($status->code !== 0) {
                    throw new \RuntimeException("Batch send failed: " . $status->details);
                }

                $entries = $response->getEntries();

                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException("Batch send failed with code: " . $respStatus->getCode() . ", message: " . $respStatus->getMessage());
                    }
                }

                // Verify response entry count matches request message count
                $entryCount = count($entries);
                $messageCount = count($messages);
                if ($entryCount !== $messageCount) {
                    throw new \RuntimeException(
                        "Batch response entry count ({$entryCount}) does not match request message count ({$messageCount})"
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
                $this->executeInterceptors(MessageHookPoints::SEND, [
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
                    $delayMs = $this->retryPolicy->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        SwooleCompat::sleep($delayMs * 1000);
                    }
                }
            }
        }

        $latencyMs = (microtime(true) - $startTime) * 1000;
        $this->executeInterceptors(MessageHookPoints::SEND, [
            'success' => false,
            'latencyMs' => $latencyMs,
            'topic' => $topic,
        ]);
        throw $lastException;
    }
}
