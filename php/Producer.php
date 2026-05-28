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
require_once __DIR__ . '/RpcClientManager.php';
require_once __DIR__ . '/MessageId.php';
require_once __DIR__ . '/MessageIdImpl.php';
require_once __DIR__ . '/MessageIdCodec.php';
require_once __DIR__ . '/TelemetrySession.php';
require_once __DIR__ . '/ConsumeResult.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/ClientTrait.php';
require_once __DIR__ . '/TransactionChecker.php';
require_once __DIR__ . '/ExponentialBackoffRetryPolicy.php';
require_once __DIR__ . '/SwooleCompat.php';
require_once __DIR__ . '/ProtobufUtil.php';
require_once __DIR__ . '/PublishingLoadBalancer.php';
require_once __DIR__ . '/Transaction.php';
require_once __DIR__ . '/IntMath.php';
require_once __DIR__ . '/MessageHookPoints.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\Permission;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\EndTransactionRequest;
use Apache\Rocketmq\V2\RecallMessageRequest;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Address;
use Apache\Rocketmq\V2\AddressScheme;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;
use Google\Protobuf\Duration;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

/**
 * Producer - Message producer
 *
 * Core features:
 * 1. Singleton TelemetrySession management
 * 2. PublishingLoadBalancer (Topic-level MessageQueue load balancing)
 * 3. Complete state management (FSM)
 * 4. Transaction message support
 * 5. Delayed message recall
 * 6. Interceptor support (Hook Points)
 * 7. ExponentialBackoffRetryPolicy wired for retries
 * 8. TransactionChecker for orphaned transaction recovery
 * 9. Batch send support
 * 10. Swoole coroutine async support
 */
class Producer
{
    use ClientTrait;

    private $client;
    private $endpoints;
    private $clientId;
    private $telemetrySession;
    private $publishingRouteDataCache = [];
    private $isRunning = false;
    private $shutdownRequested = false;
    private $maxAttempts = 3;
    private $requestTimeout = 3000; // ms
    private $topics = [];
    private $isolatedEndpoints = [];
    private $namespace = '';
    private $logger;
    private $credentials = null; // SessionCredentials for AK/SK auth
    private $validateMessageType = true;
    private $maxBodySizeBytes = 4194304; // 4MB default
    private $heartbeatPid = null;
    private $lastHeartbeatTime = 0;
    private $interceptors = [];
    private $transactionChecker = null;
    private $localTransactionExecuter = null;
    private $retryPolicy = null;
    private $tlsCredentials = null;

    /**
     * Constructor
     *
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options
     * @deprecated Use ProducerBuilder instead.
     */
    public function __construct($endpoints, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        $this->maxAttempts = $options['maxAttempts'] ?? 3;
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->topics = $options['topics'] ?? [];
        $this->namespace = $options['namespace'] ?? '';
        $this->validateMessageType = $options['validateMessageType'] ?? true;
        $this->maxBodySizeBytes = $options['maxBodySizeBytes'] ?? 4194304;
        $this->tlsCredentials = $options['tlsCredentials'] ?? null;

        // Set AK/SK credentials if provided
        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        // Initialize retry policy
        $this->retryPolicy = new ExponentialBackoffRetryPolicy($this->maxAttempts, 1000, 30000, 2.0);

        $this->logger = Logger::getInstance('Producer');

        // Use RpcClientManager for connection pooling
        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials, $this->namespace);
    }

    /**
     * Set transaction checker for orphaned transaction recovery.
     */
    public function setTransactionChecker(TransactionChecker $checker): self
    {
        $this->transactionChecker = $checker;
        return $this;
    }

    /**
     * Set local transaction executer for auto commit/rollback of half-messages.
     *
     * @param LocalTransactionExecuter $executer
     * @return $this
     */
    public function setLocalTransactionExecuter(LocalTransactionExecuter $executer): self
    {
        $this->localTransactionExecuter = $executer;
        return $this;
    }

    public function start()
    {
        if ($this->isRunning) {
            return;
        }

        try {
            Logger::getInstance('Producer')->info("Begin to start the rocketmq producer, clientId={$this->clientId}");

            // Establish Telemetry Session
            $this->establishTelemetrySession();

            // Register settings change callback
            $this->registerSettingsCallback();

            // Register transaction checker callback if set
            $this->registerTransactionCheckerCallback();

            // Warm up route cache
            foreach ($this->topics as $topic) {
                $this->getPublishingLoadBalancer($topic);
            }

            $this->isRunning = true;

            // Start periodic heartbeat
            $this->startHeartbeat();

            Logger::getInstance('Producer')->info("The rocketmq producer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            Logger::getInstance('Producer')->error("Failed to start: " . $e->getMessage());
            $this->shutdown();
            throw $e;
        }
    }

    /**
     * Synchronously send a message
     *
     * @param Message $message Message object
     * @return array Send result ['messageId' => ..., 'transactionId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $this->validateMessage($message);

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->getPublishingLoadBalancer($topic);

        $sysProps = $message->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        if ($hasMessageGroup) {
            $messageQueue = $loadBalancer->takeMessageQueueByMessageGroup($sysProps->getMessageGroup());
            if (!$messageQueue) {
                throw new \RuntimeException("No available message queue for message group: {$sysProps->getMessageGroup()}");
            }
            $candidates = [$messageQueue];
        } else {
            $candidates = $loadBalancer->takeMessageQueue($this->getIsolatedBrokerNames(), $this->maxAttempts);
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

    /**
     * Asynchronously send a message using Swoole coroutine if available.
     *
     * @param Message $message
     * @return array|\Generator
     */
    public function sendAsync(Message $message)
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($message, $channel) {
                try {
                    $result = $this->send($message);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'error' => $e]);
                }
            });
            return $channel->pop();
        }
        yield $this->send($message);
    }

    /**
     * Batch send messages. All messages must share the same topic.
     *
     * @param Message[] $messages
     * @return array Array of send results
     */
    public function sendBatch(array $messages)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        if (empty($messages)) {
            throw new \InvalidArgumentException("Batch messages cannot be empty");
        }

        // Validate all messages share the same topic
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
                $msgType = $this->detectMessageType($msg, false);
                $messageTypes[] = $msgType;
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

        $loadBalancer = $this->getPublishingLoadBalancer($topic);
        $isolatedBroker = array_keys($this->isolatedEndpoints);

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

    /**
     * Asynchronously batch send using Swoole coroutine if available.
     *
     * @param Message[] $messages
     * @return array|\Generator
     */
    public function sendBatchAsync(array $messages)
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($messages, $channel) {
                try {
                    $result = $this->sendBatch($messages);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'error' => $e]);
                }
            });
            return $channel->pop();
        }
        yield $this->sendBatch($messages);
    }

    /**
     * Send a transaction message (half-message + local transaction + commit/rollback).
     *
     * Java reference flow:
     * 1. Send half-message (invisible to consumers)
     * 2. Execute local transaction via LocalTransactionExecuter
     * 3. Auto-commit or rollback based on executor result
     *
     * @param Message $message Message to send as half-message
     * @param Transaction $transaction Transaction object to track receipts
     * @param LocalTransactionExecuter|null $executor Local transaction callback (optional)
     * @return array Send result
     */
    public function sendWithTransaction(Message $message, $transaction, ?LocalTransactionExecuter $executor = null)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $this->validateMessage($message);

        $sysProps = $message->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();

        if ($hasMessageGroup || $hasLiteTopic || $hasDeliveryTimestamp || $hasPriority) {
            throw new \InvalidArgumentException(
                "Transactional message should not set messageGroup, deliveryTimestamp, liteTopic, or priority"
            );
        }

        $topic = $message->getTopic()->getName();
        $loadBalancer = $this->getPublishingLoadBalancer($topic);
        $messageQueue = $loadBalancer->takeMessageQueue($this->getIsolatedBrokerNames(), $this->maxAttempts);

        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }

        if ($this->validateMessageType) {
            $msgType = $this->detectMessageType($message, true);
            $loadBalancer->validateMessageTypeAgainstQueue($messageQueue[0], $msgType, $topic);
        }

        $request = $this->wrapTransactionMessageRequest([$message], $messageQueue[0]);
        $result = $this->sendMessageWithRetry($request, $message, $messageQueue, $this->maxAttempts);

        if (isset($result['transactionId'])) {
            $transaction->tryAddMessage($message);
            $transaction->tryAddReceipt($message, $result, $this->extractMessageQueueEndpoint($messageQueue[0]));
        }

        // Execute local transaction and auto-commit/rollback
        if ($executor !== null) {
            $messageView = new MessageView($message, $result['recallHandle'] ?? null, null, 1);
            $resolution = $executor->execute($messageView);

            if ($resolution === TransactionResolution::COMMIT) {
                $transaction->commit();
            } elseif ($resolution === TransactionResolution::ROLLBACK) {
                $transaction->rollback();
            }
        }

        return $result;
    }

    public function beginTransaction()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        if ($this->transactionChecker === null) {
            throw new \RuntimeException("Transaction checker should not be null. Please set TransactionChecker the Producer.");
        }
        return new Transaction($this);
    }

    public function commitTransaction($messageId, $transactionId, $topic, $endpoints = null)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::COMMIT, $endpoints);
    }

    public function rollbackTransaction($messageId, $transactionId, $topic, $endpoints = null)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::ROLLBACK, $endpoints);
    }

    public function sendPriorityMessage($topic, $body, $priority, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setPriority($priority);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    public function sendDelayedMessage($topic, $body, $deliveryTimestampUnixSec, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $ts = new Timestamp();
        $ts->setSeconds($deliveryTimestampUnixSec);
        $ts->setNanos(0);

        $sysProps = new SystemProperties();
        $sysProps->setDeliveryTimestamp($ts);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    public function sendFifoMessage($topic, $body, $messageGroup, $tag = '')
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $sysProps = new SystemProperties();
        $sysProps->setMessageGroup($messageGroup);
        if (!empty($tag)) {
            $sysProps->setTag($tag);
        }

        $message = new Message();
        $message->setTopic($topicResource);
        $message->setBody($body);
        $message->setSystemProperties($sysProps);

        return $this->send($message);
    }

    public function recallMessage($topic, $recallHandle)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new RecallMessageRequest();
        $request->setTopic($topicResource);
        $request->setRecallHandle($recallHandle);

        $metadata = $this->buildMetadata();

        list($response, $status) = $this->client->RecallMessage($request, $metadata, $this->getCallOptions())->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Recall message failed: " . $status->details);
        }

        $messageId = '';
        if (method_exists($response, 'getMessageId')) {
            $messageId = $response->getMessageId();
        }

        return [
            'messageId' => $messageId,
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
                    $channel->push(['success' => false, 'error' => $e]);
                }
            });
            return $channel->pop();
        }
        yield $this->recallMessage($topic, $recallHandle);
    }

    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }

        $this->shutdownRequested = true;
        $this->logger->info("Begin to shutdown the rocketmq producer, clientId={$this->clientId}");

        // Drain in-flight async sends (Swoole)
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            // In Swoole context, give a short grace period for pending coroutines
            \Swoole\Coroutine::sleep(1);
        }

        // Stop heartbeat
        $this->stopHeartbeat();

        // Notify server of client termination
        $this->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->isRunning = false;

        $this->logger->info("Shutdown the rocketmq producer successfully, clientId={$this->clientId}");
    }

    public function getClientId()
    {
        return $this->clientId;
    }

    public function isRunning()
    {
        return $this->isRunning;
    }

    public function __destruct()
    {
        $this->shutdown();
    }

    /**
     * Start periodic heartbeat to all route endpoints.
     */
    private function startHeartbeat()
    {
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();

        if (function_exists('pcntl_signal') && function_exists('pcntl_alarm')) {
            $self = $this;
            pcntl_signal(SIGALRM, function() use ($self) {
                $self->onHeartbeatTick();
                pcntl_alarm(10);
            });
            pcntl_alarm(10);
        }
    }

    private function onHeartbeatTick()
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            $this->lastHeartbeatTime = $now;
            $this->doHeartbeat();
            static $lastRouteRefresh = 0;
            if ($now - $lastRouteRefresh >= 30) {
                $this->refreshRouteCache();
                $lastRouteRefresh = $now;
            }
        }
    }

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

    private function doHeartbeat()
    {
        if (empty($this->publishingRouteDataCache)) {
            return;
        }

        $brokerEndpoints = $this->getTotalRouteEndpoints();
        if (empty($brokerEndpoints)) {
            return;
        }

        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::PRODUCER);

        foreach ($brokerEndpoints as $endpoints) {
            $addresses = $endpoints->getAddresses();
            if (empty($addresses) || $addresses[0] === null) {
                continue;
            }
            $address = $addresses[0];
            $brokerKey = $address->getHost() . ':' . $address->getPort();
            try {
                $brokerClient = RpcClientManager::getInstance()->getClient($brokerKey, [
                    'tlsCredentials' => $this->tlsCredentials,
                ]);
                $metadata = $this->buildMetadata();
                list($response, $status) = $brokerClient->Heartbeat($request, $metadata, $this->getCallOptions())->wait();
                if ($status->code === 0) {
                    $this->logger->debug("Heartbeat to broker {$brokerKey} successful");
                    $this->isolatedEndpoints = [];
                } else {
                    $this->logger->warning("Heartbeat to broker {$brokerKey} failed:" . $status->detail);
                }
            } catch (\Exception $e) {
                $this->logger->warning("Heartbeat to broker {$brokerKey} failed:" . $e->getMessage());
            }
        }
    }

    private function notifyClientTermination()
    {
        if (empty($this->publishingRouteDataCache)) {
            return;
        }

        $request = new NotifyClientTerminationRequest();

        $metadata = $this->buildMetadata();

        try {
            list($response, $status) = $this->client->NotifyClientTermination($request, $metadata, $this->getCallOptions())->wait();
            if ($status->code === 0) {
                $this->logger->debug("NotifyClientTermination sent successfully");
            } else {
                $this->logger->warning("NotifyClientTermination failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("NotifyClientTermination exception: " . $e->getMessage());
        }
    }

    private function stopHeartbeat()
    {
        $this->heartbeatPid = null;
    }

    // ==================== Private Methods ====================

    private function registerSettingsCallback()
    {
        $self = $this;
        $this->telemetrySession->setOnSettingsChange(function ($settings) use ($self) {
            $self->onServerSettings($settings);
        });
    }

    /**
     * Register TransactionChecker callback on TelemetrySession.
     * When the server sends RecoverOrphanedTransactionCommand, this calls the checker
     * and responds with the resolution.
     */
    private function registerTransactionCheckerCallback()
    {
        if ($this->transactionChecker === null) {
            return;
        }

        $self = $this;
        $this->telemetrySession->setOnRecoverOrphanedTransaction(function ($command) use ($self) {
            $self->handleOrphanedTransaction($command);
        });
    }

    /**
     * Handle an orphaned transaction command from the server.
     */
    private function handleOrphanedTransaction($command)
    {
        if ($this->transactionChecker === null) {
            $this->logger->warning("Received orphaned transaction command but no TransactionChecker registered");
            return;
        }

        try {
            // Extract message from the command
            $message = null;
            if (method_exists($command, 'getMessage')) {
                $message = $command->getMessage();
            }

            if ($message === null) {
                $this->logger->warning("Orphaned transaction command has no message");
                return;
            }

            // Wrap in MessageView for the checker
            $messageView = new MessageView($message, null, null, 1);

            // Call the transaction checker
            $resolution = $this->transactionChecker->check($messageView);

            if ($resolution === null || $resolution === TransactionResolution::TRANSACTION_RESOLUTION_UNSPECIFIED) {
                $this->logger->debug("Transaction checker returned TRANSACTION_RESOLUTION_UNSPECIFIED, leaving transaction unresolved.");
                return;
            }

            // Send the resolution back
            $transactionId = '';
            if (method_exists($command, 'getTransactionId')) {
                $transactionId = $command->getTransactionId();
            }

            // Extract messageId from SystemProperties and topic from the message
            $messageId = '';
            $topicName = '';
            $sysProps = $message->getSystemProperties();
            if ($sysProps && method_exists($sysProps, 'getMessageId')) {
                $messageId = $sysProps->getMessageId();
            }
            if (method_exists($message, 'getTopic') && method_exists($message->getTopic(), 'getName')) {
                $topicName = $message->getTopic()->getName();
            }

            $endpoints = null;
            if (method_exists($message, 'getEndpoints') && $message->hasEndpoints()) {
                $endpoints = $message->getEndpoints();
            }
            if (!empty($messageId) && !empty($topicName)) {
                $this->endTransaction($messageId, $transactionId, $topicName, $resolution, $endpoints, \Apache\Rocketmq\V2\TransactionSource::SOURCE_SERVER_CHECK);
            }
        } catch (\Exception $e) {
            $this->logger->error("TransactionChecker threw exception: " . $e->getMessage());
        }
    }

    private function onServerSettings($settings)
    {
        $this->logger->info("Processing server settings");

        $pubSubCase = null;
        if ($settings->hasPublishing()) {
            $pubSubCase = 'PUBLISHING';
        } elseif ($settings->hasSubscription()) {
            $pubSubCase = 'SUBSCRIPTION';
        }

        if ($pubSubCase === 'PUBLISHING' && $settings->hasPublishing()) {
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

        // Process backoff policy from server and update retry policy
        if (method_exists($settings, 'getBackoffPolicy') && $settings->hasBackoffPolicy()) {
            $serverPolicy = $settings->getBackoffPolicy();
            $this->logger->info("Received backoff policy from server");
            if (method_exists($serverPolicy, 'getDurations') && !ProtobufUtil::isRepeatedFieldEmpty($serverPolicy->getDurations())) {
                $delays = [];
                foreach ($serverPolicy->getDurations() as $dur) {
                    if (method_exists($dur, 'getSeconds')) {
                        $delays[] = $dur->getSeconds() * 1000;
                    }
                }
                if (!empty($delays)) {
                    $this->retryPolicy = CustomizedBackoffRetryPolicy::fromProtobuf($serverPolicy);
                    $this->logger->info("Updated retry policy from server backoff");
                }
            }
        }
    }

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

        $success = $this->telemetrySession->syncSettings($command);

        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        usleep(500000); // 500ms
    }

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

    private function getPublishingLoadBalancer($topic)
    {
        if (!isset($this->publishingRouteDataCache[$topic])) {
            $routeData = $this->queryRoute($topic);
            $this->publishingRouteDataCache[$topic] = new PublishingLoadBalancer($routeData);
        }

        return $this->publishingRouteDataCache[$topic];
    }

    private function queryRoute($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->parseEndpoints($this->endpoints));

        $metadata = $this->buildMetadata();

        list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }

        return $response;
    }

    private function toProtobufMessage(Message $msg, $messageQueue, $txEnabled = false)
    {
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');
        $systemProperties->setBodyEncoding(Encoding::IDENTITY);
        $queueId = $messageQueue->getId();
        if ($queueId !== null) {
            $systemProperties->setQueueId($queueId);
        }
        $systemProperties->setMessageType($this->detectMessageType($msg, $txEnabled));

        $inputSysProps = $msg->getSystemProperties();
        if ($inputSysProps) {
            if (method_exists($inputSysProps, 'getTag') && $inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (method_exists($inputSysProps, 'getKeys')) {
                $keys = $inputSysProps->getKeys();
                if (!ProtobufUtil::isRepeatedFieldEmpty($keys)) {
                    $systemProperties->setKeys($keys);
                }
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

    private function detectMessageType(Message $msg, $txEnabled = false)
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();

        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::NORMAL;
        }
        if ($hasMessageGroup && !$txEnabled) {
            return V2MessageType::FIFO;
        }
        if ($hasLiteTopic && !$txEnabled) {
            return V2MessageType::LITE;
        }
        if ($hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::DELAY;
        }
        if ($hasPriority && !$txEnabled) {
            return V2MessageType::PRIORITY;
        }
        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && $txEnabled) {
            return V2MessageType::TRANSACTION;
        }

        return V2MessageType::NORMAL;
    }

    private function createTimestamp()
    {
        $now = microtime(true);
        $seconds = (int)$now;
        $nanos = (int)(($now - $seconds) * 1000000000);

        $timestamp = new Timestamp();
        $timestamp->setSeconds($seconds);
        $timestamp->setNanos($nanos);
        return $timestamp;
    }

    private function wrapSendMessageRequest($messages, $messageQueue)
    {
        $enrichedMessages = [];
        foreach ($messages as $msg) {
            $enrichedMessages[] = $this->toProtobufMessage($msg, $messageQueue);
        }

        $request = new SendMessageRequest();
        $request->setMessages($enrichedMessages);

        return $request;
    }

    private function wrapTransactionMessageRequest($messages, $messageQueue)
    {
        $enrichedMessages = [];
        foreach ($messages as $msg) {
            $enrichedMessages[] = $this->toProtobufMessage($msg, $messageQueue, true);
        }

        $request = new SendMessageRequest();
        $request->setMessages($enrichedMessages);

        return $request;
    }

    /**
     * Send message with retry using wired ExponentialBackoffRetryPolicy.
     */
    private function sendMessageWithRetry($request, $message, $candidates, $maxAttempts)
    {
        $lastException = null;
        $startTime = microtime(true);
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            if ($attempt > 1 && $candidateCount > 1) {
                $queueIndex = \Apache\Rocketmq\IntMath::mod($attempt, $candidateCount);
                $currentMessageQueue = $candidates[$queueIndex];
                $request = $this->wrapSendMessageRequest([$message], $currentMessageQueue);
            }
            try {
                $metadata = $this->buildMetadata();

                list($response, $status) = $this->client->SendMessage($request, $metadata, $this->getCallOptions())->wait();

                if ($status->code !== 0) {
                    throw new \RuntimeException("Send message failed: " . $status->details);
                }

                $entries = $response->getEntries();
                $entryCount = count($entries);

                if ($response->hasStatus()) {
                    $respStatus = $response->getStatus();
                    if ($respStatus->getCode() !== 20000) {
                        throw new \RuntimeException("SendMessage failed with code: " . $respStatus->getCode() . ", message: " . $respStatus->getMessage());
                    }
                }

                $this->logger->debug("SendMessage response: {$entryCount} entries");

                if ($entryCount > 0) {
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

                $failedEndpoints = $this->extractMessageQueueEndpoint($currentMessageQueue);
                if ($failedEndpoints !== null) {
                    $this->isolateEndpoints($failedEndpoints);
                }

                if ($attempt < $maxAttempts) {
                    $delayMs = $this->retryPolicy->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        usleep($delayMs * 1000);
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

    /**
     * Batch send with retry using wired ExponentialBackoffRetryPolicy.
     */
    private function sendBatchWithRetry($request, $messages, $candidates, $maxAttempts)
    {
        $lastException = null;
        $startTime = microtime(true);
        $topic = $messages[0]->getTopic()->getName();
        $candidateCount = count($candidates);
        $currentMessageQueue = $candidates[0];

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            if ($attempt > 1 && $candidateCount > 1) {
                $queueIndex = \Apache\Rocketmq\IntMath::mod($attempt, $candidateCount);
                $currentMessageQueue = $candidates[$queueIndex];
                $request = $this->wrapSendMessageRequest($messages, $currentMessageQueue);
            }
            try {
                $metadata = $this->buildMetadata();

                list($response, $status) = $this->client->SendMessage($request, $metadata, $this->getCallOptions())->wait();

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

                $results = [];
                foreach ($entries as $entry) {
                    $entryStatus = $entry->getStatus();
                    if ($entryStatus && $entryStatus->getCode() === 20000) {
                        $results[] = [
                            'messageId' => $entry->getMessageId(),
                            'transactionId' => $entry->getTransactionId() ?? '',
                            'recallHandle' => $entry->getRecallHandle() ?? '',
                            'code' => $entryStatus->getCode(),
                            'message' => $entryStatus->getMessage(),
                        ];
                    }
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

                $failedEndpoints = $this->extractMessageQueueEndpoint($currentMessageQueue);
                if ($failedEndpoints !== null) {
                    $this->isolateEndpoints($failedEndpoints);
                }

                if ($attempt < $maxAttempts) {
                    $delayMs = $this->retryPolicy->getNextDelayWithJitterMs($attempt);
                    if ($delayMs > 0) {
                        usleep($delayMs * 1000);
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

    private function endTransaction($messageId, $transactionId, $topic, $resolution, ?Endpoints $endpoints = null, $source = TransactionSource::SOURCE_CLIENT)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        $hookPoint = $resolution === TransactionResolution::COMMIT
            ? MessageHookPoints::COMMIT_TRANSACTION
            : MessageHookPoints::ROLLBACK_TRANSACTION;

        $this->executeInterceptors($hookPoint, [
            'messageId' => $messageId,
            'transactionId' => $transactionId,
            'topic' => $topic,
        ]);

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new EndTransactionRequest();
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        $request->setTopic($topicResource);
        $request->setResolution($resolution);
        $request->setSource($source);

        $metadata = $this->buildMetadata();

        if ($endpoints !== null) {
            $address = $endpoints->getAddresses();
            if (!empty($address) && $address[0] !== null) {
                $brokerKey = $address[0]->getHost() . ':' . $address[0]->getPort();
                $brokerClient = RpcClientManager::getInstance()->getClient($brokerKey, [
                    'tlsCredentials' => $this->tlsCredentials,
                ]);
                list($response, $status) = $brokerClient->EndTransaction($request, $metadata, $this->getCallOptions())->wait();
            } else {
                list($response, $status) = $this->client->EndTransaction($request, $metadata, $this->getCallOptions())->wait();
            }
        } else {
            list($response, $status) = $this->client->EndTransaction($request, $metadata, $this->getCallOptions())->wait();
        }

        if ($status->code !== 0) {
            throw new \RuntimeException("End transaction failed: " . $status->details);
        }

        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("End transaction failed with code: " . $statusCode);
            }
        }
    }

    private function isolateEndpoints(Endpoints $endpoints): void
    {
        foreach ($endpoints->getAddresses() as $address) {
            $key = $address->getHost() . ":" . $address->getPort();
            $this->isolatedEndpoints[$key] = $endpoints;
        }
    }

    private function getIsolatedBrokerNames(): array
    {
        $brokerNames = [];
        foreach ($this->publishingRouteDataCache as $loadBalancer) {
            foreach ($loadBalancer->getMessageQueues() as $messageQueue) {
                $ep = $this->extractMessageQueueEndpoint($messageQueue);
                if ($ep !== null) {
                    $key = $this->endpointsKey($ep);
                    if (isset($this->isolatedEndpoints[$key])) {
                        $brokerNames[] = $messageQueue->getBroker()->getName();
                    }
                }
            }
        }
        return array_unique($brokerNames);
    }

    private function extractMessageQueueEndpoint($messageQueue): ?Endpoints
    {
        $broker = $messageQueue->getBroker();
        if ($broker && $broker->hasEndpoints()) {
            return $broker->getEndpoints();
        }
        return null;
    }

    public function endpointsKey(Endpoints $endpoints): string
    {
        $addresses = $endpoints->getAddresses();
        if (!empty($addresses) && $addresses[0] !== null) {
            return $addresses[0]->getHost() . ':' . $addresses[0]->getPort();
        }
        return spl_object_hash($endpoints);
    }

    private function getTotalRouteEndpoints(): array
    {
        $endpointMap = [];
        foreach ($this->publishingRouteDataCache as $loadBalancer) {
            foreach ($loadBalancer->getMessageQueues() as $messageQueue) {
                $endpoints = $this->extractMessageQueueEndpoint($messageQueue);
                if ($endpoints !== null) {
                    $key = $this->endpointsKey($endpoints);
                    $endpointMap[$key] = $endpoints;
                }
            }
        }
        return array_values($endpointMap);
    }

    private function refreshRouteCache(): void
    {
        foreach ($this->topics as $topic) {
            try {
                $routeData = $this->queryRoute($topic);
                $existing = $this->publishingRouteDataCache[$topic] ?? null;
                $this->publishingRouteDataCache[$topic] = $existing !== null
                    ? $existing->update($routeData)
                    : new PublishingLoadBalancer($routeData);
                $this->logger->debug("Route refreshed for topic={$topic}");
            } catch (\Exception $e) {
                $this->logger->error("Failed to refresh route for topic={$topic}", ['exception' => $e]);
            }
        }
    }

    // ClientTrait required methods
    protected function getCredentials(): ?SessionCredentials { return $this->credentials; }
    protected function getClientIdValue(): string { return $this->clientId; }
    protected function getNamespaceValue(): string { return $this->namespace; }
}
