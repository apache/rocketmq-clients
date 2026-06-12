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
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;

/**
 * Producer — Message producer.
 *
 * Core features: singleton TelemetrySession, PublishingLoadBalancer, retry,
 * transaction support (via TransactionTrait), heartbeat (via HeartbeatManager),
 * interceptor support, Swoole coroutine async support.
 *
 * Send and recall logic is delegated to SendMessageHandler and RecallMessageHandler.
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
    private readonly TelemetrySession $telemetrySession;
    private readonly PublishingRouteManager $routeManager;
    private readonly ProducerSettings $settings;
    protected MessageValidator $validator;
    private bool $isRunning = false;
    private bool $shutdownRequested = false;
    private array $interceptors = [];
    private readonly Logger $logger;
    private readonly HeartbeatManager $heartbeatManager;
    private readonly SendMessageHandler $sendHandler;
    private readonly RecallMessageHandler $recallHandler;

    /**
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options (use ProducerBuilder instead)
     *                      - clientId: string, custom client identifier (default: 'php-producer-{pid}-{time}')
     *                      - maxAttempts: int, max retry attempts on send failure (default: 3)
     *                      - requestTimeout: int, gRPC request timeout in ms (default: 3000)
     *                      - topics: string[], topics to publish messages to (default: [])
     *                      - namespace: string, resource namespace prefix (default: '')
     *                      - credentials: SessionCredentials|null, AK/SK authentication credentials
     *                      - validateMessageType: bool, validate message type against route (default: true)
     *                      - maxBodySizeBytes: int, max message body size in bytes (default: 4194304)
     *                      - tlsCredentials: TlsCredentials|null, TLS/SSL configuration
     *                      - sslEnabled: bool, enable SSL for gRPC channel (default: true)
     * @deprecated Use ProducerBuilder instead for better type safety and IDE support.
     */
    public function __construct(
        private readonly string $endpoints,
        array $options = []
    ) {
        $this->settings = new ProducerSettings($endpoints, $options);
        $this->validator = new MessageValidator(
            $options['maxBodySizeBytes'] ?? 4194304,
            $options['validateMessageType'] ?? true
        );
        $this->logger = Logger::getInstance('Producer');

        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'tlsCredentials' => $this->settings->getTlsCredentials(),
            'sslEnabled' => $this->settings->isSslEnabled(),
        ]);

        $this->telemetrySession = TelemetrySession::getInstance(
            $this->client, $endpoints, $this->settings->getClientId(),
            $this->settings->getCredentials(), $this->settings->getNamespace()
        );
        $this->routeManager = new PublishingRouteManager($this->client, $endpoints, $this);
        $this->heartbeatManager = new HeartbeatManager(
            $this->routeManager, $this->client, $this,
            $this->settings->getTlsCredentials(), $this->settings->isSslEnabled()
        );

        $metadataBuilder = fn(?int $timeoutMs = null) => $this->buildMetadata($timeoutMs);
        $callOptionsResolver = fn(?int $overrideTimeout = null) => $this->getCallOptions($overrideTimeout);
        $operationTimeoutFn = fn(string $op) => $this->getOperationTimeout($op);
        $interceptorExecutor = function (string $hookPoint, array $context = []) {
            $this->executeInterceptors($hookPoint, $context);
        };

        $this->sendHandler = new SendMessageHandler(
            $this->client,
            $this->settings,
            $this->validator,
            $this->routeManager,
            $interceptorExecutor,
            $metadataBuilder,
            $callOptionsResolver,
            $operationTimeoutFn,
        );

        $this->recallHandler = new RecallMessageHandler(
            $this->client,
            $this->settings,
            $metadataBuilder,
            $callOptionsResolver,
        );
    }

    // ==================== Lifecycle ====================

    public function start(): void
    {
        if ($this->isRunning) {
            return;
        }

        try {
            Logger::getInstance('Producer')->info("Begin to start the rocketmq producer, clientId={$this->settings->getClientId()}");
            $this->establishTelemetrySession();
            $this->registerSettingsCallback();
            $this->registerTransactionCheckerCallback();

            $this->routeManager->warmUp($this->settings->getTopics());

            $this->isRunning = true;
            $this->heartbeatManager->start();

            Logger::getInstance('Producer')->info("The rocketmq producer starts successfully, clientId={$this->settings->getClientId()}");
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
        $this->logger->info("Begin to shutdown the rocketmq producer, clientId={$this->settings->getClientId()}");

        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            \Swoole\Coroutine::sleep(1);
        }

        $this->heartbeatManager->stop();
        $this->heartbeatManager->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->isRunning = false;
        $this->logger->info("Shutdown the rocketmq producer successfully, clientId={$this->settings->getClientId()}");
    }

    public function __destruct()
    {
        $this->shutdown();
    }

    // ==================== Send ====================

    /**
     * Send a message
     * @param Message $message to send
     * @return array Send result containing:
     * - messageId: messageId
     * - messageQueue: messageQueue
     * - offset: offset
     * - requestId: requestId
     * - sendTime: sendTime
     * - transactionId: transactionId
     * - transactionState: transactionState
     * @throws \Exception if producer is not running
     */
    public function send(Message $message): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->sendHandler->send($message);
    }

    /**
     * Send a message asynchronously
     * @param Message $message to send
     * @return \Generator|mixed|null | void
     */
    public function sendAsync(Message $message): array|\Generator
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->sendHandler->sendAsync($message);
    }

    // ==================== Batch Send ====================

    /**
     * Send a batch of messages
     * @param array $messages to send
     * @return array Send result containing:
     * @throws \Exception if producer is not running
     */
    public function sendBatch(array $messages): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->sendHandler->sendBatch($messages);
    }

    /**
     * Send a batch of messages asynchronously
     * @param array $messages to send
     * @return \Generator|mixed|null | void
     */
    public function sendBatchAsync(array $messages): array|\Generator
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->sendHandler->sendBatchAsync($messages);
    }

    // ==================== Convenience Send Methods ====================

    /**
     * Send a priority message
     * @param $topic  string Topic name
     * @param $body  string Message body
     * @param $priority  int Message priority
     * @param $tag  string Message tag
     * @return array Send result containing:
     * @throws \Exception if producer is not running
     */
    public function sendPriorityMessage(string $topic, string $body, int $priority, string $tag = ''): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->send($this->sendHandler->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($priority) {
            $sp->setPriority($priority);
        }));
    }

    /**
     * Send a delayed message
     * @param $topic string Topic name
     * @param $body string Message body
     * @param $deliveryTimestampUnixSec Unix timestamp
     * @param $tag  string Message tag
     * @return array Send result containing:
     * @throws \Exception if producer is not running
     */
    public function sendDelayedMessage(string $topic, string $body, int $deliveryTimestampUnixSec, string $tag = ''): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        $ts = new \Google\Protobuf\Timestamp();
        $ts->setSeconds($deliveryTimestampUnixSec);
        $ts->setNanos(0);

        return $this->send($this->sendHandler->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($ts) {
            $sp->setDeliveryTimestamp($ts);
        }));
    }

    /**
     * Send a FIFO message
     *
     * @param $topic string Topic name
     * @param $body string Message body
     * @param $messageGroup  string FIFO message group
     * @param $tag  string  Message tag
     * @return array Send result containing:
     * @throws \Exception if producer is not running
     */
    public function sendFifoMessage(string $topic, string $body, string $messageGroup, string $tag = ''): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->send($this->sendHandler->buildConvenienceMessage($topic, $body, $tag, function (SystemProperties $sp) use ($messageGroup) {
            $sp->setMessageGroup($messageGroup);
        }));
    }

    // ==================== Recall ====================

    /**
     * Recall a message
     * @param $topic string Topic name
     * @param $recallHandle recall handle
     * @return array recall result containing:
     */
    public function recallMessage(string $topic, string $recallHandle): array
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->recallHandler->recall($topic, $recallHandle);
    }

    public function recallMessageAsync(string $topic, string $recallHandle): array|\Generator
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        return $this->recallHandler->recallAsync($topic, $recallHandle);
    }

    // ==================== Interceptors ====================

    /**
     * Add an interceptor
     * @param MessageInterceptor $interceptor The interceptor to add
     * @return $this  For method chaining
     * @throws \Exception if producer is not running
     */
    public function addInterceptor(MessageInterceptor $interceptor): self
    {
        $this->interceptors[] = $interceptor;
        return $this;
    }

    /**
     * Execute interceptors
     * @param $hookPoint
     * @param $context
     * @return void
     */
    public function executeInterceptors(string $hookPoint, array $context = []): void
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

    public function getClientId(): string { return $this->settings->getClientId(); }
    public function isRunning(): bool { return $this->isRunning; }

    // ==================== ClientTrait Required Methods ====================

    protected function getCredentials(): ?SessionCredentials { return $this->settings->getCredentials(); }
    protected function getClientIdValue(): string { return $this->settings->getClientId(); }
    protected function getNamespaceValue(): string { return $this->settings->getNamespace(); }

    // ==================== Private: Telemetry & Settings ====================

    private function establishTelemetrySession(): void
    {
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

        $publishing = new Publishing();
        $topicResources = [];
        foreach ($this->settings->getTopics() as $topicName) {
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

    private function registerSettingsCallback(): void
    {
        $self = $this;
        $this->telemetrySession->setOnSettingsChange(function ($settings) use ($self) {
            $self->onServerSettings($settings);
        });
    }

    private function onServerSettings(object $settings): void
    {
        $this->logger->info("Processing server settings");

        if ($settings->hasPublishing()) {
            $publishing = $settings->getPublishing();
            if ($publishing->getMaxBodySize() > 0) {
                $this->validator->setMaxBodySizeBytes($publishing->getMaxBodySize());
                $this->logger->info("Updated maxBodySize from server: {$this->validator->getMaxBodySizeBytes()}");
            }
            $this->validator->setValidateMessageType($publishing->getValidateMessageType());
            $this->logger->info("Updated validateMessageType from server: " . ($this->validator->isValidateMessageType() ? 'true' : 'false'));
        }

        $this->settings->applyServerBackoffPolicy($settings, $this->logger);
    }

    // ==================== TransactionTrait Delegation ====================
    // These methods provide the interface that TransactionTrait requires.
    // They delegate to private properties or SendMessageHandler.

    private function validateMessage(Message $message): void
    {
        $this->validator->validateMessage($message);
    }

    private function detectMessageType(Message $msg, bool $txEnabled = false): int
    {
        return $this->sendHandler->detectMessageType($msg, $txEnabled);
    }

    private function wrapTransactionMessageRequest(array $messages, object $messageQueue): V2\SendMessageRequest
    {
        return $this->sendHandler->wrapTransactionMessageRequest($messages, $messageQueue);
    }

    private function sendMessageWithRetry(V2\SendMessageRequest $request, Message $message, array $candidates, int $maxAttempts): array
    {
        return $this->sendHandler->sendMessageWithRetry($request, $message, $candidates, $maxAttempts);
    }

    // ==================== TransactionTrait Infrastructure Delegation ====================
    // Protected methods so the trait can access infrastructure without touching
    // private properties. Override in test fakes for isolation.

    protected function getPublishingLoadBalancer(string $topic): object
    {
        return $this->routeManager->getPublishingLoadBalancer($topic);
    }

    protected function getIsolatedBrokerNames(): array
    {
        return $this->routeManager->getIsolatedBrokerNames();
    }

    protected function getSettingsMaxAttempts(): int
    {
        return $this->settings->getMaxAttempts();
    }

    protected function getSettingsTlsCredentials(): ?TlsCredentials
    {
        return $this->settings->getTlsCredentials();
    }

    protected function isSettingsSslEnabled(): bool
    {
        return $this->settings->isSslEnabled();
    }

    protected function getClientForRpc(): MessagingServiceClient
    {
        return $this->client;
    }

    protected function getTelemetrySession(): TelemetrySession
    {
        return $this->telemetrySession;
    }

    protected function getLogger(): Logger
    {
        return $this->logger;
    }
}
