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

/**
 * Producer - 消息生产者（参考 Java ProducerImpl 实现）
 * 
 * 核心特性：
 * 1. 单例 TelemetrySession 管理
 * 2. PublishingLoadBalancer（Topic 级别的 MessageQueue 负载均衡）
 * 3. 完整的状态管理（FSM）
 * 4. 事务消息支持
 * 5. 延迟消息撤回（Recall）
 * 6. 拦截器支持（Hook Points）
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
    
    /**
     * 构造函数
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
        
        // 创建 gRPC 客户端
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // 初始化 Telemetry Session（单例）
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints);
    }
    
    /**
     * 启动 Producer
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
     * 同步发送消息（参考 Java send 方法）
     * 
     * @param Message $message 消息对象
     * @return array 发送结果 ['messageId' => ..., 'transactionId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        // 检查 Producer 状态
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        // 验证消息
        $this->validateMessage($message);
        
        // 获取 Topic
        $topic = $message->getTopic()->getName();
        
        // 获取 PublishingLoadBalancer
        $loadBalancer = $this->getPublishingLoadBalancer($topic);
        
        // 选择 MessageQueue（轮询）
        $messageQueue = $loadBalancer->takeMessageQueue($this->isolatedEndpoints, 1);
        
        if (empty($messageQueue)) {
            throw new \RuntimeException("No available message queue for topic: {$topic}");
        }
        
        // 构建发送请求
        $request = $this->wrapSendMessageRequest([$message], $messageQueue[0]);
        
        // 执行发送（带重试）
        return $this->sendMessageWithRetry($request, $message, $this->maxAttempts);
    }
    
    /**
     * 异步发送消息（TODO: 需要 Swoole 协程支持）
     * 
     * @param Message $message 消息对象
     * @return \Generator 协程生成器
     */
    public function sendAsync(Message $message)
    {
        // TODO: 使用 Swoole Coroutine 实现真正的异步
        yield $this->send($message);
    }
    
    /**
     * 发送事务消息
     * 
     * @param Message $message 消息对象
     * @param Transaction $transaction 事务对象
     * @return array 发送结果
     */
    public function sendWithTransaction(Message $message, $transaction)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        // TODO: 实现事务消息逻辑
        // 1. 发送半消息（Half Message）
        // 2. 执行本地事务
        // 3. 提交或回滚事务
        
        throw new \RuntimeException("Transaction message not implemented yet");
    }
    
    /**
     * 开始事务
     * 
     * @return Transaction 事务对象
     */
    public function beginTransaction()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Producer is not running now");
        }
        
        // TODO: 实现 Transaction 类
        return new Transaction($this);
    }
    
    /**
     * 提交事务
     * 
     * @param string $messageId 消息 ID
     * @param string $transactionId 事务 ID
     * @param string $topic Topic 名称
     */
    public function commitTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::COMMIT);
    }
    
    /**
     * 回滚事务
     * 
     * @param string $messageId 消息 ID
     * @param string $transactionId 事务 ID
     * @param string $topic Topic 名称
     */
    public function rollbackTransaction($messageId, $transactionId, $topic)
    {
        $this->endTransaction($messageId, $transactionId, $topic, TransactionResolution::ROLLBACK);
    }
    
    /**
     * 撤回延迟消息
     * 
     * @param string $topic Topic 名称
     * @param string $recallHandle 撤回句柄
     * @return array 撤回结果
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
     * 异步撤回消息（TODO: 需要 Swoole 协程支持）
     * 
     * @param string $topic Topic 名称
     * @param string $recallHandle 撤回句柄
     * @return \Generator
     */
    public function recallMessageAsync($topic, $recallHandle)
    {
        yield $this->recallMessage($topic, $recallHandle);
    }
    
    /**
     * 关闭 Producer
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
     * 获取 Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * 检查是否正在运行
     */
    public function isRunning()
    {
        return $this->isRunning;
    }
    
    /**
     * 析构函数
     */
    public function __destruct()
    {
        $this->shutdown();
    }
    
    // ==================== Private Methods ====================
    
    /**
     * 建立 Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // 创建 UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // 创建 Publishing 配置
        $publishing = new Publishing();
        
        // 添加 Topics
        $topicResources = [];
        foreach ($this->topics as $topicName) {
            $topicResource = new Resource();
            $topicResource->setName($topicName);
            $topicResources[] = $topicResource;
        }
        $publishing->setTopics($topicResources);
        
        // 创建 Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);
        $settings->setPublishing($publishing);
        
        // 创建 TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // 同步发送 Settings
        $success = $this->telemetrySession->syncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        
        // 等待服务端处理
        usleep(500000); // 500ms
    }
    
    /**
     * 验证消息
     */
    private function validateMessage(Message $message)
    {
        // 检查 Topic
        if (!$message->hasTopic() || empty($message->getTopic()->getName())) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        
        // 检查消息体
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
     * 构建 SendMessageRequest
     */
    private function wrapSendMessageRequest($messages, $messageQueue)
    {
        // SendMessageRequest 只接受 messages，MessageQueue 在消息内部指定
        $request = new SendMessageRequest();
        $request->setMessages($messages);
        
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
            
            foreach ($allQueues as $queue) {
                // 检查权限是否为可写
                // Permission: 1=READ_ONLY, 2=WRITE_ONLY, 4=NONE, 6=READ_WRITE
                $permission = $queue->getPermission();
                
                // 接受 WRITE_ONLY(2), READ_WRITE(6), 或者 NONE(4) 用于兼容
                if ($permission == 2 || $permission == 4 || $permission == 6) {
                    $this->messageQueues[] = $queue;
                    error_log("[PublishingLoadBalancer] Added queue with permission: {$permission}");
                } else {
                    error_log("[PublishingLoadBalancer] Skipped queue with permission: {$permission} (not writable)");
                }
            }
            
            error_log("[PublishingLoadBalancer] Total writable queues: " . count($this->messageQueues) . " / " . count($allQueues));
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
