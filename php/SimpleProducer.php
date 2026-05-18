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
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\UA;
use Apache\Rocketmq\V2\Language;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Publishing;
use Grpc\ChannelCredentials;
use Google\Protobuf\Timestamp;

/**
 * SimpleProducer - 消息生产者
 * 
 * 集成 TelemetrySession 实现：
 * 1. 单例 Stream 管理
 * 2. 串行写入
 * 3. 背压处理
 */
class SimpleProducer
{
    private $client;
    private $endpoints;
    private $clientId;
    private $telemetrySession;
    private $topicPublishInfo = [];
    private $isStarted = false;
    
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
        if ($this->isStarted) {
            return;
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
        
        // 创建 Publishing 配置
        $publishing = new Publishing();
        // 可以添加 topics
        
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
     * 发送消息
     * 
     * @param Message $message 消息对象
     * @return array 发送结果 ['messageId' => ..., 'status' => ...]
     */
    public function send(Message $message)
    {
        if (!$this->isStarted) {
            throw new \RuntimeException("Producer not started");
        }
        
        // 查询路由
        $topic = $message->getTopic()->getName();
        $this->ensureTopicRoute($topic);
        
        // 创建发送请求
        $request = new SendMessageRequest();
        $request->setMessages([$message]);
        
        // 准备元数据
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        // 发送消息
        list($response, $status) = $this->client->SendMessage($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Send message failed: " . $status->details);
        }
        
        // 解析响应
        $entries = $response->getEntries();
        if (count($entries) > 0) {
            $entry = $entries[0];
            $resultStatus = $entry->getStatus();
            
            return [
                'messageId' => $entry->getMessageId(),
                'code' => $resultStatus->getCode(),
                'message' => $resultStatus->getMessage(),
            ];
        }
        
        throw new \RuntimeException("No response entries");
    }
    
    /**
     * 确保 Topic 路由已缓存
     */
    private function ensureTopicRoute($topic)
    {
        if (isset($this->topicPublishInfo[$topic])) {
            return;
        }
        
        $topicResource = new Resource();
        $topicResource->setName($topic);
        
        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        
        $metadata = [
            'x-mq-client-id' => [$this->clientId],
            'x-mq-language' => ['PHP'],
            'x-mq-client-version' => ['5.0.0'],
        ];
        
        list($response, $status) = $this->client->QueryRoute($request, $metadata)->wait();
        
        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }
        
        $this->topicPublishInfo[$topic] = [
            'queues' => $response->getMessageQueues(),
            'timestamp' => time(),
        ];
    }
    
    /**
     * 关闭 Producer
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
     * 析构函数
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
