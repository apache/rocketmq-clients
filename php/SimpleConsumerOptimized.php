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
use Apache\Rocketmq\V2\ReceiveMessageRequest;
use Apache\Rocketmq\V2\AckMessageRequest;
use Apache\Rocketmq\V2\ChangeInvisibleDurationRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Grpc\ChannelCredentials;
use Google\Protobuf\Duration;

/**
 * SimpleConsumer - 简单消费者（参考 Java 实现优化）
 * 
 * 核心特性：
 * 1. 单例 TelemetrySession 管理
 * 2. Topic 级别的 SubscriptionLoadBalancer（轮询分配 MessageQueue）
 * 3. 异步 receive 支持
 * 4. ACK 和修改可见性时长
 * 5. 完整的状态管理
 */
class SimpleConsumerOptimized
{
    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $telemetrySession;
    private $subscriptionExpressions = [];
    private $subscriptionRouteDataCache = [];
    private $topicIndex = 0;
    private $awaitDuration = 30; // 秒
    private $isRunning = false;
    
    /**
     * 构造函数
     * 
     * @param string $endpoints gRPC 服务端点
     * @param string $consumerGroup 消费组名称
     * @param array $options 配置选项
     */
    public function __construct($endpoints, $consumerGroup, $options = [])
    {
        $this->endpoints = $endpoints;
        $this->consumerGroup = $consumerGroup;
        $this->clientId = $options['clientId'] ?? ('php-consumer-' . getmypid() . '-' . time());
        $this->awaitDuration = $options['awaitDuration'] ?? 30;
        
        // 创建 gRPC 客户端
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // 初始化 Telemetry Session（单例，带 Settings 同步确认）
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints, $this->clientId);
    }
    
    /**
     * 订阅 Topic
     * 
     * @param string $topic Topic 名称
     * @param string $expression 过滤表达式（默认 "*"）
     * @return $this
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->checkRunning();
        
        // 获取路由数据
        $this->getRouteData($topic);
        
        // 保存订阅表达式（新订阅会覆盖旧的）
        $this->subscriptionExpressions[$topic] = $expression;
        
        return $this;
    }
    
    /**
     * 取消订阅 Topic
     * 
     * @param string $topic Topic 名称
     * @return $this
     */
    public function unsubscribe($topic)
    {
        $this->checkRunning();
        
        unset($this->subscriptionExpressions[$topic]);
        unset($this->subscriptionRouteDataCache[$topic]);
        
        return $this;
    }
    
    /**
     * 获取所有订阅表达式
     * 
     * @return array
     */
    public function getSubscriptionExpressions()
    {
        return $this->subscriptionExpressions;
    }
    
    /**
     * 启动 Consumer
     */
    public function start()
    {
        if ($this->isRunning) {
            return;
        }
        
        try {
            error_log("[SimpleConsumer] Begin to start the rocketmq simple consumer, clientId={$this->clientId}");
            
            // 建立 Telemetry Session
            $this->establishTelemetrySession();
            
            $this->isRunning = true;
            
            error_log("[SimpleConsumer] The rocketmq simple consumer starts successfully, clientId={$this->clientId}");
        } catch (\Exception $e) {
            error_log("[SimpleConsumer] Failed to start: " . $e->getMessage());
            throw $e;
        }
    }
    
    /**
     * 同步接收消息（参考 Java receive0 实现）
     * 
     * @param int $maxMessageNum 最大消息数量
     * @param int $invisibleDuration 不可见时长（秒）
     * @return array 消息列表
     */
    public function receive($maxMessageNum, $invisibleDuration = 30)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        if ($maxMessageNum <= 0) {
            throw new \InvalidArgumentException("maxMessageNum must be greater than 0");
        }
        
        // 复制订阅表达式
        $topics = array_keys($this->subscriptionExpressions);
        
        if (empty($topics)) {
            throw new \RuntimeException("There is no topic to receive message");
        }
        
        // 轮询选择 Topic（参考 Java IntMath.mod）
        $topicIndex = $this->topicIndex++;
        $topic = $topics[$topicIndex % count($topics)];
        $expression = $this->subscriptionExpressions[$topic];
        
        error_log("[SimpleConsumer] Receiving messages from topic: {$topic}");
        
        // 获取 SubscriptionLoadBalancer
        $loadBalancer = $this->getSubscriptionLoadBalancer($topic);
        
        // 从负载均衡器中获取一个 MessageQueue
        $messageQueue = $loadBalancer->takeMessageQueue();
        
        if (!$messageQueue) {
            error_log("[SimpleConsumer] No message queue available for topic: {$topic}");
            return [];
        }
        
        // 构建接收请求
        $request = $this->wrapReceiveMessageRequest(
            $maxMessageNum,
            $messageQueue,
            $expression,
            $invisibleDuration,
            $this->awaitDuration
        );
        
        // 发送请求（使用 MessageQueue 中的 Broker Endpoints）
        // 参考 Node.js: receiveMessage(request, mq, awaitDuration)
        return $this->receiveMessage($request, $messageQueue, $this->awaitDuration);
    }
    
    /**
     * 异步接收消息（TODO: 需要 Swoole 协程支持）
     * 
     * @param int $maxMessageNum 最大消息数量
     * @param int $invisibleDuration 不可见时长（秒）
     * @return \Generator 协程生成器
     */
    public function receiveAsync($maxMessageNum, $invisibleDuration = 30)
    {
        // TODO: 使用 Swoole Coroutine 实现真正的异步
        // 这里返回一个生成器作为占位符
        yield $this->receive($maxMessageNum, $invisibleDuration);
    }
    
    /**
     * 同步 ACK 消息
     * 
     * @param object $messageView 消息对象
     */
    public function ack($messageView)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        $receiptHandle = $this->extractReceiptHandle($messageView);
        
        if (!$receiptHandle) {
            throw new \InvalidArgumentException("Invalid message view, receipt handle not found");
        }
        
        $this->ackMessage($receiptHandle, $messageView);
    }
    
    /**
     * 异步 ACK 消息（TODO: 需要 Swoole 协程支持）
     * 
     * @param object $messageView 消息对象
     * @return \Generator
     */
    public function ackAsync($messageView)
    {
        yield $this->ack($messageView);
    }
    
    /**
     * 修改消息可见性时长
     * 
     * @param object $messageView 消息对象
     * @param int $invisibleDuration 新的不可见时长（秒）
     */
    public function changeInvisibleDuration($messageView, $invisibleDuration)
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
        
        $receiptHandle = $this->extractReceiptHandle($messageView);
        
        if (!$receiptHandle) {
            throw new \InvalidArgumentException("Invalid message view, receipt handle not found");
        }
        
        $this->changeInvisibleDuration0($receiptHandle, $messageView, $invisibleDuration);
    }
    
    /**
     * 异步修改可见性时长（TODO: 需要 Swoole 协程支持）
     * 
     * @param object $messageView 消息对象
     * @param int $invisibleDuration 新的不可见时长（秒）
     * @return \Generator
     */
    public function changeInvisibleDurationAsync($messageView, $invisibleDuration)
    {
        yield $this->changeInvisibleDuration($messageView, $invisibleDuration);
    }
    
    /**
     * 关闭 Consumer
     */
    public function shutdown()
    {
        if (!$this->isRunning) {
            return;
        }
        
        error_log("[SimpleConsumer] Begin to shutdown the rocketmq simple consumer, clientId={$this->clientId}");
        
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isRunning = false;
        
        error_log("[SimpleConsumer] Shutdown the rocketmq simple consumer successfully, clientId={$this->clientId}");
    }
    
    /**
     * 获取 Client ID
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * 获取消费组
     */
    public function getConsumerGroup()
    {
        return $this->consumerGroup;
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
     * 检查运行状态
     */
    private function checkRunning()
    {
        if (!$this->isRunning) {
            throw new \RuntimeException("Simple consumer is not running");
        }
    }
    
    /**
     * 建立 Telemetry Session
     */
    private function establishTelemetrySession()
    {
        // 创建 UserAgent
        $ua = new UA();
        $ua->setLanguage(Language::PHP);
        $ua->setVersion('5.0.0');
        
        // 创建 SubscriptionEntry 列表
        $subscriptionEntries = [];
        foreach ($this->subscriptionExpressions as $topic => $expression) {
            $filterExpression = new FilterExpression();
            $filterExpression->setExpression($expression);
            
            $topicResource = new Resource();
            $topicResource->setName($topic);
            
            $subscriptionEntry = new SubscriptionEntry();
            $subscriptionEntry->setTopic($topicResource);
            $subscriptionEntry->setExpression($filterExpression);
            
            $subscriptionEntries[] = $subscriptionEntry;
        }
        
        // 创建 Subscription 配置
        $subscription = new Subscription();
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $subscription->setGroup($groupResource);
        $subscription->setSubscriptions($subscriptionEntries);
        
        // 创建 Settings
        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);
        $settings->setSubscription($subscription);
        
        // 创建 TelemetryCommand
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // 发送并等待 Settings 确认（阻塞，最多 5 秒）- 参考 Java ClientSessionImpl.syncSettings
        $success = $this->telemetrySession->establishAndSyncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish and sync Telemetry Session");
        }
    }
    
    /**
     * 获取 Topic 路由数据
     */
    private function getRouteData($topic)
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
     * 获取 SubscriptionLoadBalancer
     */
    private function getSubscriptionLoadBalancer($topic)
    {
        if (!isset($this->subscriptionRouteDataCache[$topic])) {
            // 查询路由并创建负载均衡器
            $routeData = $this->getRouteData($topic);
            $this->subscriptionRouteDataCache[$topic] = new SubscriptionLoadBalancer($routeData);
        }
        
        return $this->subscriptionRouteDataCache[$topic];
    }
    
    /**
     * 构建 ReceiveMessageRequest（完全参考 Java ConsumerImpl.wrapReceiveMessageRequest）
     */
    private function wrapReceiveMessageRequest($maxMessageNum, $messageQueue, $expression, $invisibleDuration, $awaitDuration)
    {
        // 创建 FilterExpression
        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);
        $filterExpression->setType(0); // TAG type
        
        // 创建 Group Resource
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        // 创建 Request
        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessageNum);
        $request->setAutoRenew(false); // SimpleConsumer 不使用自动续期
        
        // 设置 AttemptId（参考 Java: attemptId = UUID.randomUUID().toString()）
        $attemptId = 'php-' . uniqid('', true);
        $request->setAttemptId($attemptId);
        
        // 设置 InvisibleDuration（参考 Java: Durations.fromNanos(invisibleDuration.toNanos())）
        $invisibleDurationObj = $this->createDurationFromSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);
        
        // 设置 LongPollingTimeout（参考 Java: Durations.fromNanos(longPollingTimeout.toNanos())）
        $awaitDurationObj = $this->createDurationFromSeconds($awaitDuration);
        $request->setLongPollingTimeout($awaitDurationObj);
        
        error_log("[SimpleConsumer] Created ReceiveMessageRequest:");
        error_log("  - BatchSize: {$maxMessageNum}");
        error_log("  - AttemptId: {$attemptId}");
        error_log("  - InvisibleDuration: {$invisibleDuration}s (" . $invisibleDurationObj->getSeconds() . "s + " . $invisibleDurationObj->getNanos() . "ns)");
        error_log("  - LongPollingTimeout: {$awaitDuration}s (" . $awaitDurationObj->getSeconds() . "s + " . $awaitDurationObj->getNanos() . "ns)");
        error_log("  - AutoRenew: false");
        
        return $request;
    }
    
    /**
     * 从秒数创建 Duration 对象（参考 Java Durations.fromNanos）
     * 
     * @param int|float $seconds 秒数（可以是小数）
     * @return Duration
     */
    private function createDurationFromSeconds($seconds)
    {
        $duration = new Duration();
        
        // 分离秒数和纳秒数
        $secs = intval($seconds);
        $nanos = intval(($seconds - $secs) * 1000000000);
        
        $duration->setSeconds($secs);
        $duration->setNanos($nanos);
        
        return $duration;
    }
    
    /**
     * 接收消息（参考 Node.js receiveMessage 实现）
     */
    private function receiveMessage($request, $messageQueue, $awaitDuration = 30)
    {
        // 从 MessageQueue 获取 Broker Endpoints
        $broker = $messageQueue->getBroker();
        if (!$broker) {
            error_log("[SimpleConsumer] Broker not available in message queue");
            return [];
        }
        
        if (!$broker->hasEndpoints()) {
            error_log("[SimpleConsumer] Broker has no endpoints");
            // 尝试使用默认的 endpoints
            $brokerAddress = $this->endpoints;
            error_log("[SimpleConsumer] Using default endpoints: {$brokerAddress}");
        } else {
            $endpoints = $broker->getEndpoints();
            $addresses = $endpoints->getAddresses();
            
            error_log("[SimpleConsumer] Broker endpoints addresses count: " . count($addresses));
            
            if (empty($addresses)) {
                error_log("[SimpleConsumer] No addresses found in broker endpoints, using default");
                $brokerAddress = $this->endpoints;
            } else {
                // 构建 Broker 地址字符串
                $address = $addresses[0];
                if ($address === null) {
                    error_log("[SimpleConsumer] First address is null, using default endpoints");
                    $brokerAddress = $this->endpoints;
                } else {
                    $brokerAddress = $address->getHost() . ':' . $address->getPort();
                }
            }
        }
        
        // 计算总超时时间（参考 Node.js: timeout = requestTimeout + awaitDuration）
        // requestTimeout 默认 3000ms，awaitDuration 默认 30000ms
        $requestTimeoutMs = 3000;  // 3秒
        $awaitDurationMs = $awaitDuration * 1000;  // 转换为毫秒
        $totalTimeoutMs = $requestTimeoutMs + $awaitDurationMs;
        $totalTimeoutSecs = $totalTimeoutMs / 1000.0;
        
        error_log("[SimpleConsumer] Sending ReceiveMessage request to broker: {$brokerAddress}");
        error_log("[SimpleConsumer]   Topic: " . $messageQueue->getTopic()->getName());
        error_log("[SimpleConsumer]   Batch Size: " . $request->getBatchSize());
        error_log("[SimpleConsumer]   Request Timeout: {$requestTimeoutMs}ms");
        error_log("[SimpleConsumer]   Await Duration: {$awaitDurationMs}ms");
        error_log("[SimpleConsumer]   Total Timeout: {$totalTimeoutMs}ms ({$totalTimeoutSecs}s)");
        error_log("[SimpleConsumer]   Long Polling Timeout: " . $request->getLongPollingTimeout()->getSeconds() . "s");
        
        // 【关键修复】使用已有的 $this->client，而不是创建新的客户端
        // 这样可以确保 Telemetry Stream 和 Business RPC 共享同一个 gRPC Channel
        // Broker 才能通过 Client ID 关联 Settings 和超时状态
        // 参考 Java/Node.js：它们都复用同一个 MessagingServiceClient 实例
        error_log("[SimpleConsumer] Using shared client (same as Telemetry Session)");
        
        $metadata = $this->buildMetadata();
        
        $messages = [];
        $statuses = [];
        $responseCount = 0;
        
        try {
            // 使用 $this->client 而不是创建新的 brokerClient
            $call = $this->client->ReceiveMessage($request, $metadata);
            
            foreach ($call->responses() as $response) {
                $responseCount++;
                
                // 处理 STATUS 类型响应
                if ($response->hasStatus()) {
                    $status = $response->getStatus();
                    $statusCode = $status->getCode();
                    $statusMessage = $status->getMessage();
                    
                    error_log("[SimpleConsumer] Response #{$responseCount}: STATUS - Code: {$statusCode}, Message: {$statusMessage}");
                    $statuses[] = $status;
                    
                    // 如果状态不是 OK，记录但不立即失败（可能只是没有消息）
                    if ($statusCode !== 20000 && $statusCode !== 40404) { // 20000=OK, 40404=NOT_FOUND
                        error_log("[SimpleConsumer] Non-OK status received: {$statusCode} - {$statusMessage}");
                    }
                }
                
                // 处理 MESSAGE 类型响应
                if ($response->hasMessage()) {
                    $message = $response->getMessage();
                    error_log("[SimpleConsumer] Response #{$responseCount}: MESSAGE received");
                    
                    if ($message->hasSystemProperties()) {
                        $sysProps = $message->getSystemProperties();
                        if ($sysProps->hasMessageId()) {
                            error_log("[SimpleConsumer]   Message ID: " . $sysProps->getMessageId());
                        }
                    }
                    
                    $messages[] = $message;
                }
                
                // 处理 DELIVERY_TIMESTAMP 类型响应
                if ($response->hasDeliveryTimestamp()) {
                    $timestamp = $response->getDeliveryTimestamp();
                    error_log("[SimpleConsumer] Response #{$responseCount}: DELIVERY_TIMESTAMP - " . $timestamp->getSeconds());
                }
            }
            
            error_log("[SimpleConsumer] Total responses: {$responseCount}, Messages received: " . count($messages));
            
        } catch (\Exception $e) {
            error_log("[SimpleConsumer] Error receiving messages: " . $e->getMessage());
            
            // 忽略超时错误（长轮询正常行为）
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }
        
        return $messages;
    }
    
    /**
     * ACK 消息
     */
    private function ackMessage($receiptHandle, $messageView)
    {
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        $topic = $this->extractTopic($messageView);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new AckMessageRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->AckMessage($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Ack message failed: " . $status->details);
        }
        
        // 检查响应状态
        if ($response->hasStatus()) {
            $statusCode = $response->getStatus()->getCode();
            if ($statusCode !== 20000) {
                throw new \RuntimeException("Ack message failed with code: " . $statusCode);
            }
        }
    }
    
    /**
     * 修改可见性时长
     */
    private function changeInvisibleDuration0($receiptHandle, $messageView, $invisibleDuration)
    {
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        $topic = $this->extractTopic($messageView);
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $duration = new Duration();
        $duration->setSeconds($invisibleDuration);
        
        $request = new ChangeInvisibleDurationRequest();
        $request->setGroup($groupResource);
        $request->setTopic($topicResource);
        $request->setReceiptHandle($receiptHandle);
        $request->setInvisibleDuration($duration);
        
        $metadata = $this->buildMetadata();
        
        list($response, $status) = $this->client->ChangeInvisibleDuration($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Change invisible duration failed: " . $status->details);
        }
    }
    
    /**
     * 提取 Receipt Handle
     */
    private function extractReceiptHandle($messageView)
    {
        // 从消息的系统属性中提取 receipt_handle
        if (method_exists($messageView, 'getSystemProperties')) {
            $sysProps = $messageView->getSystemProperties();
            if (method_exists($sysProps, 'getReceiptHandle')) {
                return $sysProps->getReceiptHandle();
            }
        }
        return null;
    }
    
    /**
     * 提取 Topic
     */
    private function extractTopic($messageView)
    {
        if (method_exists($messageView, 'getTopic')) {
            $topic = $messageView->getTopic();
            if (method_exists($topic, 'getName')) {
                return $topic->getName();
            }
        }
        return null;
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
 * SubscriptionLoadBalancer - 订阅负载均衡器（参考 Java 实现）
 */
class SubscriptionLoadBalancer
{
    private $messageQueues = [];
    private $queueIndex = 0;
    
    public function __construct($routeData)
    {
        if ($routeData && method_exists($routeData, 'getMessageQueues')) {
            $allQueues = $routeData->getMessageQueues();
            
            // 过滤可读的 MessageQueue
            // Permission: 1=READ_ONLY, 2=WRITE_ONLY, 4=NONE, 6=READ_WRITE
            foreach ($allQueues as $queue) {
                $permission = $queue->getPermission();
                
                // 接受 READ_ONLY(1), READ_WRITE(6), 或者 NONE(4) 用于兼容
                // 注意：RocketMQ v5 中 Permission=4 可能表示默认权限，应该接受
                if ($permission == 1 || $permission == 4 || $permission == 6) {
                    $this->messageQueues[] = $queue;
                    error_log("[SubscriptionLoadBalancer] Added queue with permission: {$permission}");
                } else {
                    error_log("[SubscriptionLoadBalancer] Skipped queue with permission: {$permission} (not readable)");
                }
            }
            
            error_log("[SubscriptionLoadBalancer] Total readable queues: " . count($this->messageQueues) . " / " . count($allQueues));
        }
    }
    
    /**
     * 获取下一个 MessageQueue（轮询）
     */
    public function takeMessageQueue()
    {
        if (empty($this->messageQueues)) {
            error_log("[SubscriptionLoadBalancer] No message queues available");
            return null;
        }
        
        $index = $this->queueIndex++ % count($this->messageQueues);
        $queue = $this->messageQueues[$index];
        
        error_log("[SubscriptionLoadBalancer] Selected queue index: {$index}");
        
        return $queue;
    }
    
    /**
     * 获取所有 MessageQueue
     */
    public function getMessageQueues()
    {
        return $this->messageQueues;
    }
}
