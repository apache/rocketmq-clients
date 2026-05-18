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
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression;
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
 * SimpleConsumer - 简单消费者
 * 
 * 集成 TelemetrySession 实现：
 * 1. 单例 Stream 管理
 * 2. 串行写入
 * 3. 背压处理
 */
class SimpleConsumer
{
    private $client;
    private $endpoints;
    private $clientId;
    private $consumerGroup;
    private $telemetrySession;
    private $subscriptions = [];
    private $isStarted = false;
    
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
        
        // 创建 gRPC 客户端
        $this->client = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
        ]);
        
        // 初始化 Telemetry Session（单例）
        $this->telemetrySession = TelemetrySession::getInstance($this->client, $endpoints);
    }
    
    /**
     * 订阅 Topic
     * 
     * @param string $topic Topic 名称
     * @param string $expression 过滤表达式（默认 "*"）
     */
    public function subscribe($topic, $expression = '*')
    {
        $this->subscriptions[$topic] = $expression;
    }
    
    /**
     * 启动 Consumer
     */
    public function start()
    {
        if ($this->isStarted) {
            return;
        }
        
        if (empty($this->subscriptions)) {
            throw new \RuntimeException("No subscriptions configured");
        }
        
        // 建立 Telemetry Session
        $this->establishTelemetrySession();
        
        $this->isStarted = true;
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
        foreach ($this->subscriptions as $topic => $expression) {
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
        
        // 同步发送 Settings
        $success = $this->telemetrySession->syncSettings($command);
        
        if (!$success) {
            throw new \RuntimeException("Failed to establish Telemetry Session");
        }
        
        // 等待服务端处理
        usleep(500000); // 500ms
    }
    
    /**
     * 接收消息
     * 
     * @param int $maxMessages 最大消息数量
     * @param int $invisibleDuration 不可见时长（秒）
     * @return array 消息列表
     */
    public function receive($maxMessages = 10, $invisibleDuration = 30)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Consumer not started");
        }
        
        $messages = [];
        
        // 遍历所有订阅的 Topic
        foreach ($this->subscriptions as $topic => $expression) {
            $received = $this->receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration);
            $messages = array_merge($messages, $received);
            
            // 如果已经达到最大数量，停止
            if (count($messages) >= $maxMessages) {
                break;
            }
        }
        
        return $messages;
    }
    
    /**
     * 从指定 Topic 接收消息
     */
    private function receiveFromTopic($topic, $expression, $maxMessages, $invisibleDuration)
    {
        // 首先查询路由获取 MessageQueue
        $messageQueue = $this->getMessageQueue($topic);
        
        if (!$messageQueue) {
            return [];
        }
        
        $filterExpression = new FilterExpression();
        $filterExpression->setExpression($expression);
        
        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        
        $request = new ReceiveMessageRequest();
        $request->setGroup($groupResource);
        $request->setMessageQueue($messageQueue);
        $request->setFilterExpression($filterExpression);
        $request->setBatchSize($maxMessages);
        
        $invisibleDurationObj = new Duration();
        $invisibleDurationObj->setSeconds($invisibleDuration);
        $request->setInvisibleDuration($invisibleDurationObj);
        
        // 准备元数据
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // 接收消息
        $call = $this->client->ReceiveMessage($request, $metadata);
        
        $messages = [];
        try {
            foreach ($call->responses() as $response) {
                if ($response->hasMessage()) {
                    $messages[] = $response->getMessage();
                }
                
                // 检查是否达到最大数量
                if (count($messages) >= $maxMessages) {
                    break;
                }
            }
        } catch (\Exception $e) {
            // 忽略超时等错误
            if (strpos($e->getMessage(), 'DEADLINE_EXCEEDED') === false) {
                throw $e;
            }
        }
        
        return $messages;
    }
    
    /**
     * 获取 MessageQueue
     */
    private function getMessageQueue($topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        try {
            list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
            
            if ($status->code !== 0) {
                return null;
            }
            
            $queues = $response->getMessageQueues();
            if (empty($queues)) {
                return null;
            }
            
            // 返回第一个可用的队列
            return $queues[0];
        } catch (\Exception $e) {
            return null;
        }
    }
    
    /**
     * 确认消息
     * 
     * @param array $receiptHandles 收据句柄列表
     */
    public function ack($receiptHandles)
    {
        // TODO: 实现 ACK 逻辑
        // 需要使用 AckMessageRequest
    }
    
    /**
     * 关闭 Consumer
     */
    public function shutdown()
    {
        if ($this->telemetrySession) {
            $this->telemetrySession->close();
        }
        
        $this->isStarted = false;
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
     * 析构函数
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
