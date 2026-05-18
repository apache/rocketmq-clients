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

require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/MessageId.php';
require_once __DIR__ . '/MessageIdImpl.php';
require_once __DIR__ . '/MessageIdCodec.php';
require_once __DIR__ . '/TelemetrySession.php';

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\SendMessageRequest;
use Apache\Rocketmq\V2\EndTransactionRequest;
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
use Apache\Rocketmq\V2\TransactionResolution;
use Apache\Rocketmq\V2\TransactionSource;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;
use Google\Protobuf\Duration;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

/**
 * Producer - Message producer (reference implementation based on Java ProducerImpl)
 *
 * Core features:
 * 1. Singleton TelemetrySession management
 * 2. PublishingLoadBalancer (Topic-level MessageQueue load balancing)
 * 3. Complete state management (FSM)
 * 4. Transaction message support
 * 5. Delayed message recall
 * 6. Interceptor support (Hook Points)
 */
class ProducerOptimized
{
    private $client;
    private $endpoints;
    private $clientId;
    private $telemetrySession;
    private $publishingRouteDataCache = [];
    private $isRunning = false;
    private $maxAttempts = 3;
    private $requestTimeout = 3000; // ms
    private $topics = [];
    private $isolatedEndpoints = [];
    private $namespace = '';
    
    /**
     * Constructor
     * 
     * @param string $endpoints gRPC 服务端点
     * @param array $options 配置选项
     */
    public function __construct($endpoints, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        $this->maxAttempts = $options['maxAttempts'] ?? 3;
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->topics = $options['topics'] ?? [];
        $this->namespace = $options['namespace'] ?? '';
        
        // 创建 gRPC 客户端
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // 初始化 Telemetry Session（单例）
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints);
    }
    
    /**
     * Start the Producer
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }
        
        try {
            error_log("[Producer] Begin to start the rocketmq producer, clientId={$this->clientId}");
            
            // 建立 Telemetry Session
            $this->establishTelemetrySession();
            
            // 预热路由缓存
            foreach ($this->topics as $topic) {
                $this->getPublishingLoadBalancer($topic);
            }
            
            $this->isRunning = true;
            
            error_log("[Producer] The rocketmq producer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            error_log("[Producer] Failed to start: " . $e->getMessage());
            $this->shutdown();
            throw $e;
        }
    }
    
    /**
     * Synchronously send a message (reference Java send method)
     *
     * @param Message $message Message object
     * @return array Send result ['messageId' => ..., 'transactionId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        // Check Producer status
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }

        // Validate message
        $this->validateMessage($message);

        // Get Topic
        $topic = $message->getTopic()->getName();

        // Get PublishingLoadBalancer
        $loadBalancer = $this->getPublishingLoadBalancer($topic);

        // Select MessageQueue (round-robin)
        $messageQueue = $loadBalancer->takeMessageQueue($this->isolatedEndpoints, 1);
        
        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }
        
        // Build send request
        $request = $this->wrapSendMessageRequest([$message], $messageQueue[0]);

        // Execute send (with retry)
        return $this->sendMessageWithRetry($request, $message, $this->maxAttempts);
    }
    
    /**
     * Asynchronously send a message (TODO: Requires Swoole coroutine support)
     *
     * @param Message $message Message object
     * @return \Generator Coroutine generator
     */
    public function sendAsync(Message $message)
    {
        // TODO: 使用 Swoole Coroutine 实现真正的异步
        yield $this->send($message);
    }
    
    /**
     * Send a transaction message
     *
     * @param Message $message Message object
     * @param Transaction $transaction Transaction object
     * @return array Send result
     */
    public function sendWithTransaction(Message $message, $transaction)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        // TODO: Implement transaction message logic
        // 1. Send half message
        // 2. Execute local transaction
        // 3. Commit or rollback transaction
        
        throw new \RuntimeException("Transaction message not implemented yet");
    }
    
    /**
     * Begin a transaction
     *
     * @return Transaction Transaction object
     */
    public function beginTransaction()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        // TODO: Implement Transaction class
        return new Transaction($this);
    }
    
    /**
     * Commit a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     */
    public function commitTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::COMMIT);
    }
    
    /**
     * Rollback a transaction
     *
     * @param string $messageId Message ID
     * @param string $transactionId Transaction ID
     * @param string $topic Topic name
     */
    public function rollbackTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::ROLLBACK);
    }
    
    /**
     * Recall a delayed message
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle
     * @return array Recall result
     */
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
        
        list($response, $status) = $this->client->RecallMessage($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Recall message failed: " . $status->details);
        }
        
        return [
            'status' => $response->getStatus(),
        ];
    }
    
    /**
     * Asynchronously recall a message (TODO: Requires Swoole coroutine support)
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle
     * @return \Generator
     */
    public function recallMessageAsync($topic, $recallHandle)
    {
        yield $this->recallMessage($topic, $recallHandle);
    }
    
    /**
     * Shutdown the Producer
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }
        
        error_log("[Producer] Begin to shutdown the rocketmq producer, clientId={$this->clientId}");
        
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isRunning = false;
        
        error_log("[Producer] Shutdown the rocketmq producer successfully, clientId={$this->clientId}");
    }
    
    /**
     * Get Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * Check if running
     */
    public function isRunning()
    {
        return $this->isRunning;
    }
    
    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->shutdown();
    }
    
    // ==================== Private Methods ====================
    
    /**
     * Establish Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // Create UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // Create Publishing configuration
        $publishing = new Publishing();
        
        // Add Topics
        $topicResources = [];
        foreach ($this->topics as $topicName) {
            $topicResource = new Resource();
            $topicResource->setName($topicName);
            $topicResources[] = $topicResource;
        }
        $publishing->setTopics($topicResources);
        
        // Create Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);
        $settings->setPublishing($publishing);
        
        // Create TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Synchronously send Settings
        $success = $this->telemetrySession->syncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        
        // Wait for server processing
        usleep(500000); // 500ms
    }
    
    /**
     * Validate message
     */
    private function validateMessage(Message $message)
    {
        // Check Topic
        if (!$message->hasTopic() || empty($message->getTopic()->getName())) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        
        // Check message body
        if (empty($message->getBody())) {
            throw new \InvalidArgumentException("Message body is required");
        }
        
        // 检查消息大小（默认 4MB）
        $maxSize = 4 * 1024 * 1024;
        if (strlen($message->getBody()) > $maxSize) {
            throw new \InvalidArgumentException("Message size exceeds limit (4MB)");
        }
    }
    
    /**
     * 获取 PublishingLoadBalancer
     */
    private function getPublishingLoadBalancer($topic)
    {
        if (!isset($this->publishingRouteDataCache[$topic])) {
            // 查询路由并创建负载均衡器
            $routeData = $this->queryRoute($topic);
            $this->publishingRouteDataCache[$topic] = new PublishingLoadBalancer($routeData);
        }
        
        return $this->publishingRouteDataCache[$topic];
    }
    
    /**
     * 查询路由
     */
    private function queryRoute($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }
        
        return $response;
    }
    
    /**
     * Convert a Message to enriched protobuf Message
     * Reference: Java PublishingMessageImpl.toProtobuf(namespace, mq)
     */
    private function toProtobufMessage(Message $msg, $messageQueue)
    {
        // Generate message ID
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        // Build SystemProperties
        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');
        $systemProperties->setBodyEncoding(Encoding::IDENTITY);
        $systemProperties->setQueueId($messageQueue->getId());
        $systemProperties->setMessageType($this->detectMessageType($msg));

        // Copy optional system properties from input message
        $inputSysProps = $msg->getSystemProperties();
        if ($inputSysProps) {
            if (method_exists($inputSysProps, 'getTag') && $inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (method_exists($inputSysProps, 'getKeys')) {
                $keys = $inputSysProps->getKeys();
                if (!empty($keys)) {
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

        // Build topic Resource with namespace
        $topicResource = new Resource();
        $topicResource->setName($msg->getTopic()->getName());
        if (!empty($this->namespace)) {
            $topicResource->setResourceNamespace($this->namespace);
        }

        // Build protobuf Message
        $protoMsg = new Message();
        $protoMsg->setTopic($topicResource);
        $protoMsg->setBody($msg->getBody());
        $protoMsg->setSystemProperties($systemProperties);

        // Copy user properties
        $userProps = $msg->getUserProperties();
        if (!empty($userProps)) {
            foreach ($userProps as $key => $value) {
                $protoMsg->getUserProperties()[$key] = $value;
            }
        }

        return $protoMsg;
    }

    /**
     * Detect message type based on message properties
     * Reference: Java PublishingMessageImpl constructor
     */
    private function detectMessageType(Message $msg)
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();

        // FIFO message
        if ($hasMessageGroup) {
            return V2MessageType::FIFO;
        }
        // Delay message
        if ($hasDeliveryTimestamp) {
            return V2MessageType::DELAY;
        }
        // Lite message
        if ($hasLiteTopic) {
            return V2MessageType::LITE;
        }
        // Priority message
        if ($hasPriority) {
            return V2MessageType::PRIORITY;
        }

        return V2MessageType::NORMAL;
    }

    /**
     * Create a Protobuf Timestamp with current time
     */
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

    /**
     * 构建 SendMessageRequest
     */
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
    
    /**
     * 发送消息（带重试）
     */
    private function sendMessageWithRetry($request, $message, $maxAttempts)
    {
        $lastException = null;
        
        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $metadata = $this->buildMetadata();
                
                list($response, $status) = $this->client->SendMessage($request, $metadata)->wait();
                
                if ($status->code !== 0) {
                    throw new \RuntimeException("Send message failed: " . $status->details);
                }
                
                // 解析响应
                $entries = $response->getEntries();
                if (count($entries) > 0) {
                    $entry = $entries[0];
                    $resultStatus = $entry->getStatus();
                    
                    // 检查状态码
                    if ($resultStatus->getCode() !== 20000) {
                        throw new \RuntimeException("Send message failed with code: " . $resultStatus->getCode());
                    }
                    
                    return [
                        'messageId' => $entry->getMessageId(),
                        'transactionId' => $entry->getTransactionId(),
                        'code' => $resultStatus->getCode(),
                        'message' => $resultStatus->getMessage(),
                    ];
                }
                
                throw new \RuntimeException("No response entries");
                
            } catch (\Exception $e) {
                $lastException = $e;
                error_log("[Producer] Send attempt {$attempt} failed: " . $e->getMessage());
                
                // 如果不是最后一次尝试，等待后重试
                if ($attempt < $maxAttempts) {
                    usleep(pow(2, $attempt) * 100000); // 指数退避
                }
            }
        }
        
        throw $lastException;
    }
    
    /**
     * 结束事务
     */
    private function endTransaction($messageId, $transactionId, $topic, $resolution)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new EndTransactionRequest();
        $request->setMessageId($messageId);
        $request->setTransactionId($transactionId);
        $request->setTopic($topicResource);
        $request->setResolution($resolution);
        $request->setSource(TransactionSource::SOURCE_CLIENT);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->EndTransaction($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("End transaction failed: " . $status->details);
        }
        
        // 检查响应状态
        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("End transaction failed with code: " . $statusCode);
            }
        }
    }
    
    /**
     * 构建元数据
     */
    private function buildMetadata()
    {
        // 参考 Java Signature.sign() 方法生成完整的 metadata
        // Required fields according to Java implementation
        $dateTime = gmdate('Ymd\THis\Z'); // Format: yyyyMMdd'T'HHmmss'Z'
        $requestId = sprintf('%08x-%04x-%04x-%04x-%012x',
            mt_rand(0, 0xffffffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) & 0x0fff | 0x4000,
            mt_rand(0, 0x3fff) | 0x8000,
            mt_rand(0, 0xffffffffffff)
        );
        
        return [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
            'x-mq-protocol' => ['v2'],
            'x-mq-date-time' => [$dateTime],
            'x-mq-request-id' => [$requestId],
            'x-mq-namespace' => [$this->namespace ?? ''],
        ];
    }
}

/**
 * PublishingLoadBalancer - 发布负载均衡器（参考 Java 实现）
 */
class PublishingLoadBalancer
{
    private $index;
    private $messageQueues = [];
    
    public function __construct($routeData)
    {
        // 初始化随机索引（参考 Java RandomUtils.nextInt）
        $this->index = rand(0, PHP_INT_MAX);

        // 过滤可写的 MessageQueue
        if ($routeData && method_exists($routeData, 'getMessageQueues')) {
            $allQueues = $routeData->getMessageQueues();
            $writableCount = 0;

            foreach ($allQueues as $queue) {
                // Permission: 1=READ_ONLY, 2=WRITE_ONLY, 4=NONE, 6=READ_WRITE
                $permission = $queue->getPermission();
                // Accept WRITE_ONLY(2), READ_WRITE(6), or NONE(4) for compatibility
                if ($permission == 2 || $permission == 4 || $permission == 6) {
                    $this->messageQueues[] = $queue;
                    $writableCount++;
                }
            }

            error_log("[PublishingLoadBalancer] Topic queues: {$writableCount} writable / " . count($allQueues) . " total");
        }

        if (empty($this->messageQueues)) {
            throw new \InvalidArgumentException("No writable message queue found");
        }
    }
    
    /**
     * 根据消息组选择 MessageQueue（FIFO 消息）
     * 
     * @param string $messageGroup 消息组
     * @return object MessageQueue
     */
    public function takeMessageQueueByMessageGroup($messageGroup)
    {
        if (empty($this->messageQueues)) {
            throw new \RuntimeException("No message queues available");
        }
        
        // 使用 SipHash 计算哈希值（简化版，使用 crc32）
        $hashCode = crc32($messageGroup);
        $index = abs($hashCode) % count($this->messageQueues);
        
        return $this->messageQueues[$index];
    }
    
    /**
     * 选择 MessageQueue 列表（轮询 + 排除隔离的 Endpoints）
     * 
     * @param array $excluded 要排除的 Endpoints
     * @param int $count 需要的数量
     * @return array MessageQueue 列表
     */
    public function takeMessageQueue($excluded = [], $count = 1)
    {
        if (empty($this->messageQueues)) {
            return [];
        }
        
        $candidates = [];
        $candidateBrokerNames = [];
        
        $next = $this->index++;
        
        // 第一轮：排除隔离的 Endpoints
        for ($i = 0; $i < count($this->messageQueues); $i++) {
            $queueIndex = $next++ % count($this->messageQueues);
            $messageQueue = $this->messageQueues[$queueIndex];
            
            $broker = $messageQueue->getBroker();
            $brokerName = $broker->getName();
            
            // 检查是否被排除
            $isExcluded = false;
            foreach ($excluded as $endpoint) {
                // TODO: 比较 Endpoints
                if ($brokerName === $endpoint) {
                    $isExcluded = true;
                    break;
                }
            }
            
            if (!$isExcluded && !in_array($brokerName, $candidateBrokerNames)) {
                $candidateBrokerNames[] = $brokerName;
                $candidates[] = $messageQueue;
            }
            
            if (count($candidates) >= $count) {
                return $candidates;
            }
        }
        
        // 第二轮：如果所有 Endpoints 都被隔离，使用全部队列
        if (empty($candidates)) {
            for ($i = 0; $i < count($this->messageQueues); $i++) {
                $queueIndex = $next++ % count($this->messageQueues);
                $messageQueue = $this->messageQueues[$queueIndex];
                
                $broker = $messageQueue->getBroker();
                $brokerName = $broker->getName();
                
                if (!in_array($brokerName, $candidateBrokerNames)) {
                    $candidateBrokerNames[] = $brokerName;
                    $candidates[] = $messageQueue;
                }
                
                if (count($candidates) >= $count) {
                    break;
                }
            }
        }
        
        return $candidates;
    }
    
    /**
     * 获取所有 MessageQueue
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}

/**
 * Transaction - 事务对象（占位符）
 */
class Transaction
{
    private $producer;
    private $messages = [];
    private $receipts = [];
    
    public function __construct($producer)
    {
        $this->producer = $producer;
    }
    
    public function commit()
    {
        // TODO: 实现事务提交
    }
    
    public function rollback()
    {
        // TODO: 实现事务回滚
    }
}
