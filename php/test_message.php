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

// 直接引用必要的文件
require_once __DIR__ . '/ClientServiceProvider.php';
require_once __DIR__ . '/ClientConfiguration.php';
require_once __DIR__ . '/Consumer/MessageListener.php';
require_once __DIR__ . '/Consumer/ConsumeResult.php';
require_once __DIR__ . '/Consumer/FilterExpression.php';
require_once __DIR__ . '/Consumer/FilterExpressionType.php';
require_once __DIR__ . '/Consumer/PushConsumer.php';
require_once __DIR__ . '/Consumer/SimpleConsumer.php';
require_once __DIR__ . '/Message/Message.php';
require_once __DIR__ . '/Message/MessageView.php';
require_once __DIR__ . '/Message/MessageId.php';
require_once __DIR__ . '/Producer/Producer.php';
require_once __DIR__ . '/Producer/Transaction.php';
require_once __DIR__ . '/Producer/SendReceipt.php';
require_once __DIR__ . '/Producer/RecallReceipt.php';
require_once __DIR__ . '/Producer/TransactionChecker.php';
require_once __DIR__ . '/Producer/TransactionResolution.php';
require_once __DIR__ . '/Builder/ProducerBuilder.php';
require_once __DIR__ . '/Builder/MessageBuilder.php';
require_once __DIR__ . '/Builder/PushConsumerBuilder.php';
require_once __DIR__ . '/Builder/SimpleConsumerBuilder.php';
require_once __DIR__ . '/Builder/LitePushConsumerBuilder.php';
require_once __DIR__ . '/Builder/LiteSimpleConsumerBuilder.php';

use Apache\Rocketmq\ClientServiceProvider;
use Apache\Rocketmq\ClientServiceProviderImpl;
use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Consumer\PushConsumerBuilder;
use Apache\Rocketmq\Consumer\MessageListener;
use Apache\Rocketmq\Consumer\ConsumeResult;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;
use Apache\Rocketmq\Message\MessageBuilder;
use Apache\Rocketmq\Producer\ProducerBuilder;
use Apache\Rocketmq\Producer\SendReceipt;

// 测试配置
$endpoints = '127.0.0.1:8081';
$topic = 'topic-php';
$consumerGroup = 'GID-php';

// 测试发送消息
function testSendMessage() {
    global $endpoints, $topic;
    
    echo "=== 测试发送消息 ===\n";
    
    try {
        // 获取客户端服务提供者
        $provider = ClientServiceProviderImpl::loadService();
        
        // 创建生产者构建器
        $producerBuilder = $provider->newProducerBuilder();
        
        // 构建生产者
        $producer = $producerBuilder
            ->setEndpoints($endpoints)
            ->setTopics($topic)
            ->build();
        
        // 创建消息构建器
        $messageBuilder = $provider->newMessageBuilder();
        
        // 构建消息
        $message = $messageBuilder
            ->setTopic($topic)
            ->setBody('Hello RocketMQ from PHP!')
            ->setTag('test')
            ->setKeys(['key1', 'key2'])
            ->build();
        
        // 发送消息
        $sendReceipt = $producer->send($message);
        
        echo "消息发送成功！\n";
        echo "消息ID: " . $sendReceipt->getMessageId() . "\n";
        echo "主题: " . $sendReceipt->getTopic() . "\n";
        echo "队列ID: " . $sendReceipt->getQueueId() . "\n";
        echo "偏移量: " . $sendReceipt->getOffset() . "\n";
        
        // 关闭生产者
        $producer->close();
        
        echo "生产者已关闭\n";
    } catch (Exception $e) {
        echo "发送消息失败: " . $e->getMessage() . "\n";
    }
}

// 测试消费消息
function testConsumeMessage() {
    global $endpoints, $topic, $consumerGroup;
    
    echo "\n=== 测试消费消息 ===\n";
    
    try {
        // 获取客户端服务提供者
        $provider = ClientServiceProviderImpl::loadService();
        
        // 创建推送消费者构建器
        $consumerBuilder = $provider->newPushConsumerBuilder();
        
        // 构建消费者
        $consumer = $consumerBuilder
            ->setEndpoints($endpoints)
            ->setConsumerGroup($consumerGroup)
            ->setTopic($topic)
            ->setMessageListener(new class implements MessageListener {
                public function consume(array $messages) {
                    echo "收到" . count($messages) . "条消息:\n";
                    foreach ($messages as $message) {
                        echo "消息ID: " . $message->getMessageId() . "\n";
                        echo "主题: " . $message->getTopic() . "\n";
                        echo "标签: " . ($message->getTag() ?? '无') . "\n";
                        echo "键: " . implode(', ', $message->getKeys()) . "\n";
                        echo "内容: " . $message->getBody() . "\n";
                        echo "---\n";
                    }
                    return ConsumeResult::SUCCESS;
                }
            })
            ->build();
        
        // 订阅主题
        $filterExpression = new FilterExpression('*', FilterExpressionType::TAG);
        $consumer->subscribe($topic, $filterExpression);
        
        echo "消费者已启动，开始消费消息...\n";
        echo "按 Ctrl+C 停止消费\n";
        
        // 保持消费者运行
        while (true) {
            sleep(1);
        }
    } catch (Exception $e) {
        echo "消费消息失败: " . $e->getMessage() . "\n";
    }
}

// 运行测试
testSendMessage();
testConsumeMessage();
