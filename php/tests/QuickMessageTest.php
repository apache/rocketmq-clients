<?php
/**
 * 快速消息收发测试
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\SimpleConsumer;

$endpoints = '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);
$config->withSslEnabled(false);

echo "=== 快速消息测试 ===\n\n";

// 测试普通消息
$topic = 'topic-normal';
$consumerGroup = 'GID-normal-consumer';

try {
    echo "1. 创建 Producer...\n";
    $producer = Producer::getInstance($config, $topic);
    $producer->start();
    echo "   ✓ Producer 启动成功\n\n";
    
    echo "2. 发送消息...\n";
    $message = $producer->newMessage($topic)
        ->withBody("Test Message")
        ->withTag("test")
        ->withKey("test-key")
        ->build();
    
    $receipt = $producer->send($message);
    echo "   ✓ 消息发送成功\n";
    echo "   Message ID: " . $receipt->getMessageId() . "\n\n";
    
    echo "3. 接收消息...\n";
    $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
    $consumer->setMaxMessageNum(32);
    $consumer->setInvisibleDuration(30);
    
    sleep(2); // 等待消息可用
    
    $messages = $consumer->receive(32, 30);
    echo "   接收到 " . count($messages) . " 条消息\n";
    
    foreach ($messages as $msg) {
        echo "   - Message ID: " . $msg->getSystemProperties()->getMessageId() . "\n";
        echo "     Body: " . $msg->getBody() . "\n";
        $consumer->ack($msg);
    }
    
    echo "   ✓ 消息接收并ACK成功\n\n";
    
    $producer->shutdown();
    
    echo "✅ 测试通过！\n";
    
} catch (\Exception $e) {
    echo "❌ 测试失败: " . $e->getMessage() . "\n";
    echo "堆栈:\n" . $e->getTraceAsString() . "\n";
    exit(1);
}
