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
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\AckMessageEntry;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Grpc\ChannelCredentials;
use Google\Protobuf\Duration;

/**
 * SimpleConsumer
 *
 * Integrates TelemetrySession to implement:
 * 1. Singleton Stream management
 * 2. Serial write
 * 3. Backpressure handling
 */
class SimpleConsumer
{
    use ClientTrait;

    private readonly MessagingServiceClient $client;
    private readonly string $clientId;
    private readonly TelemetrySession $telemetrySession;
    private array $subscriptions = [];
    private bool $isStarted = false;
    private readonly Logger $logger;
    private ?SessionCredentials $credentials = null;
    private string $namespace = '';
    private int $requestTimeout = 3000; // ms
    private array $interceptors = [];
    private $parsedEndpoints = null;
    private int $awaitDuration = 30; // seconds
    private int $lastHeartbeatTime = 0;
    private array $brokerClients = [];
    private array $subscriptionRouteDataCache = [];
    private int $topicIndex = 0;
    private $heartbeatCoroutineId = null;
    private readonly ?TlsCredentials $tlsCredentials;
    private bool $heartbeatInProgress = false;
    private int $heartbeatTimerId = -1;

    /**
     * Constructor
     *
     * @param string $endpoints gRPC server endpoint
     * @param string $consumerGroup Consumer group name
     * @param array $options Configuration options:
     *                        - clientId: string, custom client identifier
     *                        - namespace: string, resource namespace
     *                        - requestTimeout: int, gRPC request timeout in ms (default 3000)
     *                        - awaitDuration: int, long polling timeout in seconds (default 30)
     *                        - tlsCredentials: TlsCredentials|null, TLS configuration
     *                        - credentials: SessionCredentials|null, AK/SK credentials
     *                        - subscriptionExpressions: array<string,string>, pre-populated subscriptions (topic => expression)
     */
    public function __construct(
        private readonly string $endpoints,
        private readonly string $consumerGroup,
        array $options = []
    ) {
        $this->clientId = $options['clientId'] ?? ('php-consumer-' . getmypid() . '-' . time());
        $this->namespace = $options['namespace'] ?? '';
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->awaitDuration = $options['awaitDuration'] ?? 30;
        $this->tlsCredentials = $options['tlsCredentials'] ?? null;

        // Pre-populate subscriptions from options, since subscribe() requires isStarted=true
        // and start() requires non-empty subscriptions — breaking the cyclic dependency.
        if (isset($options['subscriptionExpressions']) && is_array($options['subscriptionExpressions'])) {
            $this->subscriptions = $options['subscriptionExpressions'];
        }

        // Set AK/SK credentials if provided
        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        }

        // Create gRPC client via connection pool
        $this->client = RpcClientManager::getInstance()->getClient($endpoints, [
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId, $this->credentials, $this->namespace);
        $this->logger = Logger::getInstance('SimpleConsumer');
    }

    /**
     * Subscribe to a topic
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression (default "*")
     * @return $this
     */
    public function subscribe(string $topic, string $expression = '*'): self
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer is not started");
        }
        
        $this->subscriptions[$topic] = $expression;
        return $this;
    }

    /**
     * Unsubscribe from a topic
     *
     * @param string $topic Topic name
     * @return $this
     */
    public function unsubscribe(string $topic): self
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer is not started");
        }
        
        unset($this->subscriptions[$topic]);
        return $this;
    }

    /**
     * Get all subscription expressions.
     *
     * @return array<string, string> Map of topic to filter expression
     */
    public function getSubscriptionExpressions(): array
    {
        return $this->subscriptions;
    }

    /**
     * Start the consumer
     *
     * @return void
     * @throws \RuntimeException If no subscriptions configured or Telemetry Session fails
     */
    public function start(): void
    {
        if ($this->isStarted) {
            return;
        }

        if (empty($this->subscriptions)) {
            throw new \RuntimeException("No subscriptions configured");
        }

        // Establish Telemetry Session
        $this->establishTelemetrySession();

        $this->isStarted = true;

        // Start periodic heartbeat via SIGALRM timer
        $this->startHeartbeat();
    }

    /**
     * Heartbeat tick handler - call from main loop to keep connection alive.
     * Sends heartbeat every 10 seconds to the broker.
     *
     * @return void
     */
    public function onHeartbeatTick(): void
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            // Check concurrency guard before sending heartbeat
            if ($this->heartbeatInProgress) {
                $this->logger->debug("Heartbeat already in progress, skipping this tick");
                return;
            }
            
            $this->heartbeatInProgress = true;
            try {
                $this->doHeartbeat();
                $this->lastHeartbeatTime = $now;
            } catch (\Throwable $e) {
                $this->logger->warning("Heartbeat tick failed: " . $e->getMessage());
            } finally {
                $this->heartbeatInProgress = false;
            }
        }
    }

    /**
     * Send heartbeat to the broker to keep connection alive.
     *
     * @return void
     */
    public function doHeartbeat(): void
    {
        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::SIMPLE_CONSUMER);
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request->setGroup($groupResource);

        $metadata = $this->buildMetadata($this->requestTimeout);

        try {
            list($response, $status) = $this->client->Heartbeat($request, $metadata, $this->getCallOptions())->wait();
            if ($status->code === 0) {
                $this->logger->debug("Heartbeat sent successfully");
            } else {
                $this->logger->warning("Heartbeat failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("Heartbeat failed: " . $e->getMessage());
        }
    }

    /**
     * Establish Telemetry Session with the broker.
     *
     * Sends settings command synchronously to register subscriptions.
     *
     * @return void
     * @throws \RuntimeException If settings sync fails
     */
    private function establishTelemetrySession()
    {
        // Create UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion(ClientConstants::CLIENT_VERSION);

        // Create SubscriptionEntry list
        $subscriptionEntries = [];
        foreach ($this->subscriptions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);
            $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);

            $topicResource = new Resource();
            $topicResource->setName($topic);

            $subscriptionEntry = new SubscriptionEntry();
            $subscriptionEntry->setTopic($topicResource);
            $subscriptionEntry->setExpression($filterExpression);

            $subscriptionEntries[] = $subscriptionEntry;
        }

        // Create Subscription configuration
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);

        // Create Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);

        // Create TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);

        // Send Settings synchronously
        $success = $this->telemetrySession->syncSettings($command);

        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }

        // Wait for server processing
        SwooleCompat::sleep(500000); // 500ms
    }

    /**
     * Register a message interceptor.
     *
     * @param MessageInterceptor $interceptor
     * @return $this
     */
    public function addInterceptor(MessageInterceptor $interceptor)
    {
        $this->interceptors[] = $interceptor;
        return $this;
    }

    /**
     * Execute interceptors at a given hook point.
     *
     * @param string $hookPoint One of MessageHookPoints constants
     * @param array $context Context data for the hook point
     * @return void
     */
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

    /**
     * Receive messages from all queues of subscribed topics.
     * Iterates through ALL queues of each topic (not just one round-robin queue)
     * with short per-queue timeout, so the full cycle completes quickly.
     *
     * @param int $maxMessages Maximum number of messages to receive in total
     * @param int $invisibleDuration Invisible duration in seconds for received messages
     * @return array List of received MessageView objects
     * @throws \RuntimeException If consumer not started
     * @throws \InvalidArgumentException If maxMessages <= 0
     */
    public function receive($maxMessages = 10, $invisibleDuration = 30)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        if ($maxMessages <= 0) {
            throw new \InvalidArgumentException("Invalid maxMessages must be greater than 0");
        }

        $startTime = microtime(true);
        
        // Receive from all queues
        $allMessages = $this->receiveFromAllQueues($maxMessages, $invisibleDuration);

        $latencyMs = (microtime(true) - $startTime) * 1000;
        $this->executeInterceptors(MessageHookPoints::RECEIVE, [
            'success' => true,
            'latencyMs' => $latencyMs,
            'messageCount' => count($allMessages),
        ]);

        // Periodic heartbeat with concurrency guard
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            if (!$this->heartbeatInProgress) {
                $this->heartbeatInProgress = true;
                try {
                    $this->doHeartbeat();
                    $this->lastHeartbeatTime = $now;
                } catch (\Throwable $e) {
                    $this->logger->warning("Receive-triggered heartbeat failed: " . $e->getMessage());
                } finally {
                    $this->heartbeatInProgress = false;
                }
            }
        }

        if (isset($this->telemetrySession)) {
            try {
                $this->telemetrySession->pollTelemetry();
            } catch (\Throwable $e) {
                $this->logger->debug("Telemetry poll failed: " . $e->getMessage());
            }
        }
        return $allMessages;
    }

    /**
     * Receive messages from all queues of all subscribed topics.
     * Iterates through ALL queues with short per-queue timeout.
     *
     * @param int $maxMessages Maximum number of messages to receive in total
     * @param int $invisibleDuration Invisible duration in seconds for received messages
     * @return array List of received Message objects
     */
    private function receiveFromAllQueues($maxMessages, $invisibleDuration)
    {
        $allMessages = [];
        $topics = array_keys($this->subscriptions);
        
        if (empty($topics)) {
            return [];
        }

        // Count total queues across all topics
        $totalQueues = 0;
        $topicInfo = [];
        
        foreach ($topics as $topic) {
            $expression = $this->subscriptions[$topic];
            $loadBalancer = $this->getSubscriptionLoadBalancer($topic);
            $queues = $loadBalancer->getMessageQueues();
            $topicInfo[$topic] = [
                'expression' => $expression,
                'queues' => $queues,
                'loadBalancer' => $loadBalancer,
            ];
            $totalQueues += count($queues);
        }

        if ($totalQueues === 0) {
            return [];
        }

        // Short per-queue timeout: distribute awaitDuration across all queues
        // This ensures the full cycle completes quickly
        $avgQueueOverhead = 2;
        $targetCallTime = 30;
        $perQueueTimeout = max(2, (int)($this->awaitDuration / max(1, $totalQueues)));
        $perQueueTimeout = min($perQueueTimeout, 5);
        $batchSize = (int)ceil($targetCallTime / $avgQueueOverhead);
        $batchSize = max(4, min($batchSize, $totalQueues));
        $batchSize = min($batchSize, 32);
        $selectedQueues = [];
        $seenKeys = [];
        foreach ($topicInfo as $topic => $info) {
            $lb = $info['loadBalancer'];
            $topicQueueCount = count($info['queues']);
            $attempts = 0;
            while (count($selectedQueues) < $batchSize && $attempts < $topicQueueCount * 2) {
                $attempts++;
                $queue = $lb->takeMessageQueue();
                if (!$queue) {
                    break;
                }
                $key = spl_object_id($queue);
                if (!isset($seenKeys[$key])) {
                    $seenKeys[$key] = true;
                    $selectedQueues[] = ['topic' => $topic, 'queue' => $queue];
                }
            }
        }

        // Poll each selected queue in the batch
        $debugBatch = getenv('SC_DEBUG') ? true : false;
        $batchResults = [];
        foreach ($selectedQueues as $sq) {
            if (count($allMessages) >= $maxMessages) {
                break;
            }
            $messages = $this->receiveFromSpecificQueue(
                $sq['topic'],
                $topicInfo[$sq['topic']]['expression'],
                $sq['queue'],
                $maxMessages - count($allMessages),
                $invisibleDuration,
                $perQueueTimeout
            );
            if ($debugBatch) {
                $broker = $sq['queue']->getBroker();
                $brokerName = $broker ? $broker->getName() : '';
                $batchResults[] = $brokerName . '=' . count($messages);
            }
            if (!empty($messages)) {
                $allMessages = array_merge($allMessages, $messages);
            }
            if ($debugBatch) {
                $totalFound = count($allMessages);
                $this->logger->debug("Batch: " . implode(',', $batchResults) . " (found $totalFound)");
            }
        }
        return $allMessages;
    }

    /**
     * Receive messages from a specific topic via streaming gRPC call.
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression
     * @param int $maxMessages Maximum number of messages to receive
     * @param int $invisibleDuration Invisible duration in seconds
     * @param int|null $longPollingTimeout Long polling timeout in seconds
     * @return array List of received Message objects
     */
    private function receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration, $longPollingTimeout = null)
    {
        // Query route to get MessageQueue first
        $messageQueue = $this->getMessageQueue($topic);

        if (!$messageQueue) {
            return [];
        }

        return $this->receiveFromSpecificQueue($topic, $expression, $messageQueue, $maxMessages, $invisibleDuration, $longPollingTimeout);
    }

    /**
     * Receive messages from a specific message queue via streaming gRPC call.
     *
     * @param string $topic Topic name
     * @param string $expression Filter expression
     * @param \Apache\Rocketmq\V2\MessageQueue $messageQueue The specific message queue to receive from
     * @param int $maxMessages Maximum number of messages to receive
     * @param int $invisibleDuration Invisible duration in seconds
     * @param int|null $longPollingTimeout Long polling timeout in seconds
     * @return array List of received Message objects
     */
    private function receiveFromSpecificQueue($topic, $expression, $messageQueue, $maxMessages, $invisibleDuration, $longPollingTimeout = null)
    {
        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);
        $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessages);
        $request->setAutoRenew(false);

        $invisibleDurationObj = new Duration();
        $invisibleDurationObj->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);

        $effectiveLongPollingTimeout = $longPollingTimeout ?? $this->awaitDuration;
        $longPollingTimeoutObj = new Duration();
        $longPollingTimeoutObj->setSeconds($effectiveLongPollingTimeout);
        $request->setLongPollingTimeout($longPollingTimeoutObj);

        $attemptId = $this->generateAttemptId();
        $request->setAttemptId($attemptId);

        // Route ReceiveMessage to broker endpoints from MessageQueue
        $receiveClient = $this->getBrokerClient($messageQueue);

        // Calculate total gRPC timeout including long polling (convert to milliseconds for buildMetadata)
        $grpcTimeoutMs = $this->requestTimeout + $effectiveLongPollingTimeout * 1000;
        $grpcTimeoutUs = $grpcTimeoutMs * 1000;
        // Use signed metadata via ClientTrait with deadline
        $metadata = $this->buildMetadata($grpcTimeoutMs);

        $callOptions = ['timeout' => $grpcTimeoutUs];
        $this->logger->debug("ReceiveMessage: topic={$topic}, batchSize={$maxMessages}, grpcTimeout={$grpcTimeoutMs}ms, attemptId={$attemptId}");

        // Receive messages - deadline enforced by server
        $call = $receiveClient->ReceiveMessage($request, $metadata, $callOptions);

        $messages = [];
        try {
            foreach ($call->responses() as $response) {
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $statusCode = $status->getCode();
                    if ($statusCode !== 20000 && $statusCode !== 40404) {
                        $this->logger->warning("Non-OK status from ReceiveMessage: code={$statusCode}, message=" . $status->getMessage());
                    }
                }

                if ($response->hasMessage()) {
                    $messages[] = $response->getMessage();
                }

                // Check if maximum count is reached
                if (count($messages) >= $maxMessages) {
                    break;
                }
            }
        } catch (\Exception $e) {
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }

        return $messages;
    }

    /**
     * Get MessageQueue via QueryRoute + SubscriptionLoadBalancer.
     * SimpleConsumer uses QueryRoute (not QueryAssignment) like Java's SimpleConsumerImpl.
     * Round-robin across subscribed topics.
     *
     * @param string $topic Topic name
     * @return \Apache\Rocketmq\V2\MessageQueue|null
     */
    private function getMessageQueue($topic)
    {
        $loadBalancer = $this->getSubscriptionLoadBalancer($topic);
        return $loadBalancer->takeMessageQueue();
    }

    /**
     * Get SubscriptionLoadBalancer for a topic.
     * Caches route data per topic, creating load balancer on first access.
     *
     * @param string $topic Topic name
     * @return SubscriptionLoadBalancer
     */
    private function getSubscriptionLoadBalancer($topic)
    {
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            $routeData = $this->getRouteData($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($routeData);
        }

        return $this->subscriptionRouteDataCache[$topic];
    }

    /**
     * Query route data for a topic via QueryRoute RPC.
     *
     * @param string $topic Topic name
     * @return \Apache\Rocketmq\V2\QueryRouteResponse|null
     */
    private function getRouteData($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->getParsedEndpoints());

        // Set gRPC deadline using operation-specific timeout
        $queryRouteTimeoutMs = $this->getOperationTimeout('QUERY_ROUTE') / 1000; // Convert to milliseconds for buildMetadata
        $metadata = $this->buildMetadata($queryRouteTimeoutMs);
        
        // Also set client-side timeout as safety net (in microseconds)
        $callOptions = ['timeout' => $this->getOperationTimeout('QUERY_ROUTE')];

        try {
            list($response, $status) = $this->client->QueryRoute($request, $metadata, $callOptions)->wait();

            if ($status->code !== 0) {
                $this->logger->warning("QueryRoute failed for topic={$topic}: " . $status->details);
                return null;
            }

            return $response;
        } catch (\Exception $e) {
            $this->logger->warning("QueryRoute exception for topic={$topic}: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Acknowledge messages.
     *
     * @param array $messages List of message objects to acknowledge
     * @return void
     * @throws \RuntimeException If consumer is not started
     */
    public function ack($messages)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        if (empty($messages)) {
            return;
        }

        // Group messages by topic for batch ack
        $messagesByTopic = [];
        foreach ($messages as $message) {
            $topic = $this->extractTopic($message);
            if (!$topic) {
                continue;
            }
            if (!isset($messagesByTopic[$topic])) {
                $messagesByTopic[$topic] = [];
            }
            $messagesByTopic[$topic][] = $message;
        }

        foreach ($messagesByTopic as $topic => $topicMessages) {
            $this->ackMessagesForTopic($topic, $topicMessages);
        }
    }

    /**
     * Acknowledge messages for a specific topic with retry logic.
     *
     * @param string $topic Topic name
     * @param array $messages List of message objects to acknowledge
     * @return void
     */
    private function ackMessagesForTopic($topic, $messages)
    {
        $entries = [];
        foreach ($messages as $message) {
            $receiptHandle = $this->extractReceiptHandle($message);
            $messageId = $this->extractMessageId($message);

            if (!$receiptHandle) {
                $this->logger->warning("Skip ack: no receipt handle for message");
                continue;
            }

            $entry = new AckMessageEntry();
            if ($messageId) {
                $entry->setMessageId($messageId);
            }
            $entry->setReceiptHandle($receiptHandle);
            $entries[] = $entry;
        }

        if (empty($entries)) {
            return;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setEntries($entries);

        // Set gRPC deadline using operation-specific timeout
        $ackTimeoutMs = $this->getOperationTimeout('ACK_MESSAGE') / 1000; // Convert to milliseconds for buildMetadata
        $metadata = $this->buildMetadata($ackTimeoutMs);
        
        // Also set client-side timeout as safety net (in microseconds)
        $callOptions = ['timeout' => $this->getOperationTimeout('ACK_MESSAGE')];

        $attempt = 0;
        $maxRetries = 16;
        $successCount = 0;
        $failureCount = 0;

        while ($attempt < $maxRetries) {
            try {
                list($response, $status) = $this->client->AckMessage($request, $metadata, $callOptions)->wait();
                if ($status->code !== 0) {
                    $this->logger->warning("AckMessage attempt {$attempt}: gRPC status error: " . $status->details);
                } else {
                    $responseEntries = $response->getEntries();
                    if (!ProtobufUtil::isRepeatedFieldEmpty($responseEntries)) {
                        // Collect indices to remove (successful or permanent failure entries)
                        $indicesToRemove = [];
                        
                        foreach ($responseEntries as $i => $resultEntry) {
                            $messageId = isset($entries[$i]) ? $entries[$i]->getMessageId() : 'unknown';
                            
                            if ($resultEntry->hasStatus()) {
                                $entryCode = $resultEntry->getStatus()->getCode();
                                
                                if ($entryCode === 20000) {
                                    // SUCCESS - mark for removal
                                    $successCount++;
                                    $indicesToRemove[] = $i;
                                    $this->logger->debug("AckMessage success for messageId={$messageId}");
                                } elseif ($entryCode === 40013) {
                                    // INVALID_RECEIPT_HANDLE - permanent failure, mark for removal
                                    $failureCount++;
                                    $indicesToRemove[] = $i;
                                    $this->logger->warning("AckMessage failed with INVALID_RECEIPT_HANDLE for messageId={$messageId}, not retrying");
                                } else {
                                    // Other errors - check if retryable
                                    $isRetryable = $this->isRetryableErrorCode($entryCode);
                                    if (!$isRetryable) {
                                        // Permanent failure - mark for removal
                                        $failureCount++;
                                        $indicesToRemove[] = $i;
                                        $this->logger->warning("AckMessage permanent error code={$entryCode} for messageId={$messageId}, not retrying");
                                    } else {
                                        // Retryable - keep in array
                                        $this->logger->debug("AckMessage retryable error code={$entryCode} for messageId={$messageId}, will retry");
                                    }
                                }
                            } else {
                                // No status in response entry, keep for retry
                                $this->logger->debug("AckMessage no status for messageId={$messageId}, will retry");
                            }
                        }
                        
                        // If all entries are terminal (success or permanent failure), we're finished
                        if (count($indicesToRemove) === count($entries)) {
                            $this->logger->info("AckMessage batch completed: success={$successCount}, failure={$failureCount}");
                            return;
                        }

                        // Remove terminal entries in reverse order to maintain indices
                        if (!empty($indicesToRemove)) {
                            rsort($indicesToRemove);
                            foreach ($indicesToRemove as $index) {
                                array_splice($entries, $index, 1);
                            }
                        }
                        
                        // Update request with remaining retryable entries
                        $request->setEntries($entries);
                        $attempt++;
                        SwooleCompat::sleep(1000000);
                        continue;
                    }
                    // Empty response entries, consider all successful
                    $successCount = count($entries);
                    $this->logger->info("AckMessage batch completed with empty response: success={$successCount}");
                    return;
                }
            } catch (\Exception $e) {
                $this->logger->warning("AckMessage attempt {$attempt} failed: " . $e->getMessage());
            }
            $attempt++;
            SwooleCompat::sleep(1000000);
        }
        
        // Exhausted all retries
        $remainingCount = count($entries);
        $failureCount += $remainingCount;
        $this->logger->error("AckMessage exceeded max retries {$maxRetries}, giving up. Remaining entries: {$remainingCount}, Total failures: {$failureCount}");
    }

    /**
     * Change message visibility duration
     *
     * @param object $message Message to modify
     * @param int $invisibleDurationSeconds New invisible duration
     * @return bool true if operation succeeded, false if skipped or failed
     * @throws \RuntimeException If consumer is not started
     */
    public function changeInvisibleDuration($message, $invisibleDurationSeconds)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }

        $receiptHandle = $this->extractReceiptHandle($message);
        $messageId = $this->extractMessageId($message);
        $topic = $this->extractTopic($message);

        if (!$receiptHandle) {
            $this->logger->warning("SimpleConsumer changeInvisibleDuration: no receipt handle, skipping");
            return false;
        }

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);

        $topicResource = new Resource();
        $topicResource->setName($topic);

        $duration = new Duration();
        $duration->setSeconds($invisibleDurationSeconds);
        $duration->setNanos(0);

        $request = new \Apache\Rocketmq\V2\ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);
        if ($messageId) {
            $request->setMessageId($messageId);
        }

        // Set gRPC deadline in metadata (server-side enforcement)
        $metadata = $this->buildMetadata($this->requestTimeout);
        $grpcTimeoutUs = $this->getOperationTimeout('CHANGE_INVISIBLE');
        $metadata['grpc-timeout'] = [$grpcTimeoutUs . 'u']; // microseconds
        
        // Also set client-side timeout as safety net
        $callOptions = ['timeout' => $grpcTimeoutUs];

        try {
            list($response, $status) = $this->client->ChangeInvisibleDuration($request, $metadata, $callOptions)->wait();
            if ($status->code !== 0) {
                $this->logger->warning("SimpleConsumer changeInvisibleDuration failed: " . $status->details);
                return false;
            }
            // Update receipt handle with the new one returned by server
            if (method_exists($response, 'getReceiptHandle') && $response->getReceiptHandle() !== '') {
                $sysProps = $message->getSystemProperties();
                if ($sysProps !== null && method_exists($sysProps, 'setReceiptHandle')) {
                    $sysProps->setReceiptHandle($response->getReceiptHandle());
                }
            }
            return true;
        } catch (\Exception $e) {
            $this->logger->error("SimpleConsumer changeInvisibleDuration exception: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Notify server that this client is terminating.
     *
     * @return void
     */
    private function notifyClientTermination()
    {
        $request = new NotifyClientTerminationRequest();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $request->setGroup($groupResource);

        // Set gRPC deadline in metadata (server-side enforcement)
        $metadata = $this->buildMetadata($this->requestTimeout);
        $grpcTimeoutUs = $this->getOperationTimeout('HEARTBEAT');
        $metadata['grpc-timeout'] = [$grpcTimeoutUs . 'u']; // microseconds
        
        // Also set client-side timeout as safety net
        $callOptions = ['timeout' => $grpcTimeoutUs];

        try {
            list($response, $status) = $this->client->NotifyClientTermination($request, $metadata, $callOptions)->wait();
            if ($status->code === 0) {
                $this->logger->debug("NotifyClientTermination sent successfully");
            } else {
                $this->logger->warning("NotifyClientTermination failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("NotifyClientTermination exception: " . $e->getMessage());
        }
    }

    /**
     * Shut down the consumer
     *
     * @return void
     */
    public function shutdown(): void
    {
        if (!$this->isStarted) {
            return;
        }

        $this->stopHeartbeat();

        $this->notifyClientTermination();

        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }

        $this->isStarted = false;
    }

    /**
     * Start periodic heartbeat via SIGALRM timer.
     * Sends initial heartbeat then schedules recurring heartbeat every 10s.
     *
     * @return void
     */
    public function startHeartbeat()
    {
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();
        // Prefer Swoole tick timer in coroutine context (non-blocking, no signal overhead)
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $this->heartbeatTimerId = SwooleCompat::tick(10000, function () {
                if ($this->heartbeatInProgress) {
                    $this->logger->debug("Swoole tick but heartbeat already in progress, skipping");
                    return;
                }
                $this->heartbeatInProgress = true;
                try {
                    $this->doHeartbeat();
                    $this->lastHeartbeatTime = time();
                } catch (\Throwable $e) {
                    $this->logger->warning("Swoole tick heartbeat failed: " . $e->getMessage());
                } finally {
                    $this->heartbeatInProgress = false;
                }
            });
            if ($this->heartbeatCoroutineId >0 ) {
                $this->logger->debug("Heartbeat started with Swoole timer (ID : {$this->heartbeatTimerId}");
                return;
            }
        }
        
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(SIGALRM, function () use ($self) {
                // Concurrency guard: prevent reentrant heartbeat
                if ($self->heartbeatInProgress) {
                    $self->logger->debug("SIGALRM received but heartbeat already in progress, skipping");
                    $self->scheduleNextHeartbeat();
                    return;
                }
                
                $self->heartbeatInProgress = true;
                try {
                    $self->doHeartbeat();
                    $self->lastHeartbeatTime = time();
                } catch (\Throwable $e) {
                    $self->logger->warning("SIGALRM heartbeat failed: " . $e->getMessage());
                } finally {
                    $self->heartbeatInProgress = false;
                    $self->scheduleNextHeartbeat();
                }
            }, true); // restart = true to handle nested signals
        }

        // Schedule recurring heartbeat
        $this->scheduleNextHeartbeat();
    }

    /**
     * Schedule next heartbeat alarm in 10 seconds.
     *
     * @return void
     */
    private function scheduleNextHeartbeat()
    {
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(10);
        }
    }

    /**
     * Stop heartbeat timer (cancel SIGALRM).
     *
     * @return void
     */
    public function stopHeartbeat()
    {
        if ($this->heartbeatTimerId > 0) {
            SwooleCompat::clearTimer($this->heartbeatTimerId);
            $this->heartbeatTimerId = -1;
            $this->logger->debug("Swoole heartbeat timer cleared");
        }
        if ($this->heartbeatCoroutineId !== null) {
            \Swoole\Coroutine::cancel($this->heartbeatCoroutineId);
            $this->logger->info("Swoole coroutine heartbeat stopped, coroutine_id=" . $this->heartbeatCoroutineId);
            $this->heartbeatCoroutineId = null;
        }
        
        // Cancel pending alarm
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(0);
        }
        
        // Reset signal handler to default
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGALRM, SIG_DFL);
        }
        
        // Wait for any in-progress heartbeat to complete
        $waitCount = 0;
        while ($this->heartbeatInProgress && $waitCount < 10) {
            SwooleCompat::sleep(10000); // Wait 10ms
            $waitCount++;
        }
        
        if ($this->heartbeatInProgress) {
            $this->logger->warning("Heartbeat still in progress after waiting, forcing shutdown");
        } else {
            $this->logger->debug("Heartbeat timer stopped cleanly");
        }
    }

    /**
     * Asynchronously receive messages via Swoole coroutine.
     *
     * @param int $maxMessages Maximum messages
     * @param int $invisibleDuration Invisible duration in seconds
     * @return array|\Generator Array of messages when Swoole available, Generator otherwise
     */
    public function receiveAsync($maxMessages = 10, $invisibleDuration = 30)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $maxMessages, $invisibleDuration, $channel) {
                try {
                    $messages = $self->receive($maxMessages, $invisibleDuration);
                    $channel->push(['result' => $messages]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            // Add timeout to prevent permanent blocking
            $data = $channel->pop($this->requestTimeout / 1000.0); // Convert ms to seconds
            if ($data === false) {
                throw new \RuntimeException("Receive async timeout after {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'];
        }
        return $this->receiveSyncFallback($maxMessages, $invisibleDuration);
    }

    /**
     * Generator fallback for receiveAsync when Swoole is not available.
     *
     * @param int $maxMessages
     * @param int $invisibleDuration
     * @return \Generator
     */
    private function receiveSyncFallback($maxMessages, $invisibleDuration): \Generator
    {
        yield $this->receive($maxMessages, $invisibleDuration);
    }

    /**
     * Asynchronously acknowledge messages via Swoole coroutine.
     *
     * @param array $messages Messages to ack
     * @return bool|\Generator True on success when Swoole available, Generator otherwise
     */
    public function ackAsync($messages)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $messages, $channel) {
                try {
                    $self->ack($messages);
                    $channel->push(['success' => true]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            // Add timeout to prevent permanent blocking
            $data = $channel->pop($this->requestTimeout / 1000.0); // Convert ms to seconds
            if ($data === false) {
                throw new \RuntimeException("Ack async timeout after {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        return $this->ackSyncFallback($messages);
    }

    /**
     * Generator fallback for ackAsync when Swoole is not available.
     *
     * @param array $messages
     * @return \Generator
     */
    private function ackSyncFallback($messages): \Generator
    {
        yield $this->ack($messages);
    }

    /**
     * Asynchronously change invisible duration via Swoole coroutine.
     *
     * @param object $message Message to modify
     * @param int $invisibleDurationSeconds New invisible duration
     * @return bool|\Generator True on success when Swoole available, Generator otherwise
     */
    public function changeInvisibleDurationAsync($message, $invisibleDurationSeconds)
    {
        if (SwooleCompat::isAvailable() && !SwooleCompat::inCoroutine()) {
            $self = $this;
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($self, $message, $invisibleDurationSeconds, $channel) {
                try {
                    $success = $self->changeInvisibleDuration($message, $invisibleDurationSeconds);
                    $channel->push(['success' => $success]);
                } catch (\Throwable $e) {
                    $channel->push(['exception' => $e]);
                }
            });
            // Add timeout to prevent permanent blocking
            $data = $channel->pop($this->requestTimeout / 1000.0); // Convert ms to seconds
            if ($data === false) {
                throw new \RuntimeException("Change invisible duration async timeout after {$this->requestTimeout}ms");
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['success'];
        }
        return $this->changeInvisibleDurationSyncFallback($message, $invisibleDurationSeconds);
    }

    /**
     * Generator fallback for changeInvisibleDurationAsync when Swoole is not available.
     *
     * @param object $message
     * @param int $invisibleDurationSeconds
     * @return \Generator
     */
    private function changeInvisibleDurationSyncFallback($message, $invisibleDurationSeconds): \Generator
    {
        yield $this->changeInvisibleDuration($message, $invisibleDurationSeconds);
    }

    /**
     * Get the client ID.
     *
     * @return string
     */
    public function getClientId(): string
    {
        return $this->clientId;
    }

    /**
     * Get the consumer group name.
     *
     * @return string
     */
    public function getConsumerGroup()
    {
        return $this->consumerGroup;
    }

    /**
     * Check if heartbeat is currently in progress.
     *
     * @return bool
     */
    public function isHeartbeatInProgress(): bool
    {
        return $this->heartbeatInProgress;
    }

    /**
     * Get the namespace.
     *
     * @return string
     */
    public function getNamespace()
    {
        return $this->namespace;
    }

    /**
     * Set the namespace.
     *
     * @param string $namespace
     * @return $this
     */
    public function setNamespace(string $namespace)
    {
        $this->namespace = $namespace;
        return $this;
    }

    /**
     * Set credentials.
     *
     * @param SessionCredentials $credentials
     * @return $this
     */
    public function setCredentials(SessionCredentials $credentials)
    {
        $this->credentials = $credentials;
        return $this;
    }

    /**
     * Get the parsed endpoints Endpoints protobuf object.
     *
     * @return \Apache\Rocketmq\V2\Endpoints
     */
    public function getParsedEndpoints()
    {
        if ($this->parsedEndpoints === null) {
            $this->parsedEndpoints = $this->parseEndpoints($this->endpoints);
        }
        return $this->parsedEndpoints;
    }

    /**
     * Get the await duration in seconds.
     *
     * @return int Long polling timeout in seconds
     */
    public function getAwaitDuration()
    {
        return $this->awaitDuration;
    }

    /**
     * Get a gRPC client connected to the broker endpoints from a MessageQueue.
     * Reuses cached client if available, creates new one otherwise.
     * Falls back to proxy client if broker endpoints not available.
     *
     * @param MessageQueue $messageQueue
     * @return MessagingServiceClient
     */
    public function getBrokerClient($messageQueue)
    {
        $broker = $messageQueue->getBroker();
        if (!$broker || !$broker->hasEndpoints()) {
            $this->logger->debug("Broker has no endpoints, falling back to proxy client");
            return $this->client;
        }

        $endpointsProto = $broker->getEndpoints();
        $addresses = $endpointsProto->getAddresses();
        $addressArray = ProtobufUtil::repeatedFieldToArray($addresses);
        if (empty($addressArray) || $addressArray[0] === null) {
            $this->logger->debug("No addresses in broker endpoints, falling back to proxy client");
            return $this->client;
        }

        $address = $addressArray[0];
        $brokerKey = $address->getHost() . ':' . $address->getPort();

        // Reuse cached client if available
        if (isset($this->brokerClients[$brokerKey])) {
            $this->logger->debug("Reusing cached broker client for {$brokerKey}");
            return $this->brokerClients[$brokerKey];
        }

        // Create new gRPC client to broker
        $this->logger->info("Creating new broker client for {$brokerKey}");
        $this->brokerClients[$brokerKey] = RpcClientManager::getInstance()->getClient($brokerKey, [
            'tlsCredentials' => $this->tlsCredentials,
        ]);

        return $this->brokerClients[$brokerKey];
    }

    /**
     * Get the underlying MessagingServiceClient.
     *
     * @return MessagingServiceClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * Get credentials for signing (ClientTrait required method).
     *
     * @return SessionCredentials|null
     */
    protected function getCredentials(): ?SessionCredentials
    {
        return $this->credentials;
    }

    /**
     * Get client ID value for signing (ClientTrait required method).
     *
     * @return string
     */
    protected function getClientIdValue(): string
    {
        return $this->clientId;
    }

    /**
     * Get namespace value for signing (ClientTrait required method).
     *
     * @return string
     */
    protected function getNamespaceValue(): string
    {
        return $this->namespace;
    }

    /**
     * Generate a unique attempt ID for receiveMessage retry tracking.
     *
     * @return string
     */
    protected function generateAttemptId(): string
    {
        return 'php-' . uniqid('', true);
    }

    /**
     * Destructor - shuts down the consumer gracefully.
     *
     * @return void
     */
    public function __destruct()
    {
        $this->shutdown();
    }

    /**
     * Check if an error code is retryable (transient error).
     *
     * Retryable errors:
     * - 50001: INTERNAL_SERVER_ERROR
     * - 50002: HA_NOT_AVAILABLE
     * - 50400: PROXY_TIMEOUT
     * - 50401: MASTER_PERSISTENCE_TIMEOUT
     * - 50402: SLAVE_PERSISTENCE_TIMEOUT
     * - 42900: TOO_MANY_REQUESTS
     *
     * Non-retryable errors:
     * - 20000: OK
     * - 40013: INVALID_RECEIPT_HANDLE
     * - Other 4xx client errors
     *
     * @param int $code Error code from Status
     * @return bool True if the error is transient and should be retried
     */
    private function isRetryableErrorCode(int $code): bool
    {
        // Server-side transient errors (5xx)
        if ($code >= 50000 && $code < 60000) {
            return true;
        }
        
        return match ($code) {
            42900 => true, // TOO_MANY_REQUESTS
            default => false,
        };
    }

    /**
     * Filter entries that need to be retried based on response status.
     * Deprecated: Use inline logic in ackMessage() instead.
     *
     * @param array $entries Original request entries
     * @param array $responseEntries Response entries with status
     * @return array Entries that should be retried
     * @deprecated This method is no longer used. Logic moved to ackMessage().
     */
    private function filterRetryableEntries($entries, $responseEntries)
    {
        $responseCodes = [];
        foreach ($responseEntries as $i => $resultEntry) {
            if ($resultEntry->hasStatus()) {
                $responseCodes[$i] = $resultEntry->getStatus()->getCode();
            }
        }
        $retryable = [];
        foreach ($entries as $i => $entry) {
            if (!isset($responseCodes[$i]) || $this->isRetryableErrorCode($responseCodes[$i])) {
                $retryable[] = $entry;
            }
        }
        return $retryable;
    }
}
