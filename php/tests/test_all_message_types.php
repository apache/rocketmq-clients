<?php
/**
 * 完整消息收发测试套件
 * 
 * 测试所有消息类型：
 * 1. 普通消息 (topic-normal)
 * 2. 顺序消息 (topic-order)
 * 3. 延时消息 (topic-delay)
 * 4. 事务消息 (topic-transaction)
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\LocalTransactionState;
use Apache\Rocketmq\Producer\TransactionChecker;
use Apache\Rocketmq\Producer\TransactionResolution;
use Apache\Rocketmq\Message\MessageView;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\FilterExpressionType;

// Simple TransactionChecker implementation
class SimpleTransactionChecker implements TransactionChecker {
    public function check(MessageView $messageView): TransactionResolution {
        $messageId = $messageView->getMessageId();
        echo "    [事务检查] Message ID: {$messageId} → COMMIT\n";
        return TransactionResolution::COMMIT;
    }
}

// 配置
$endpoints = '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);
// 禁用 SSL 验证（用于自签名证书）
$config->withSslEnabled(false);

echo "========================================\n";
echo "  RocketMQ 完整消息收发测试\n";
echo "========================================\n\n";
echo "接入点: {$endpoints}\n\n";

// 测试结果统计
$results = [
    'normal' => ['sent' => 0, 'received' => 0, 'success' => true],
    'order' => ['sent' => 0, 'received' => 0, 'success' => true],
    'delay' => ['sent' => 0, 'received' => 0, 'success' => true],
    'transaction' => ['sent' => 0, 'received' => 0, 'success' => true],
];

/**
 * 测试1: 普通消息
 */
function testNormalMessage($config, &$results) {
    echo "[测试 1/4] 普通消息 (topic-normal)\n";
    echo str_repeat("-", 60) . "\n";
    
    $topic = 'topic-normal';
    $consumerGroup = 'GID-normal-consumer';
    $testCount = 10;
    
    try {
        // 创建 Producer
        echo "  → 创建 Producer (topic: {$topic})...\n";
        $producer = Producer::getInstance($config, $topic);
        $producer->start();
        
        // 发送消息
        echo "  → 发送 {$testCount} 条普通消息...\n";
        for ($i = 1; $i <= $testCount; $i++) {
            $msgBuilder = new MessageBuilder();
            $message = $msgBuilder
                ->setTopic($topic)
                ->setBody("Normal Message #{$i}")
                ->setTag("tag-normal")
                ->setKeys(["key-normal-{$i}"])
                ->build();
            
            $receipt = $producer->send($message);
            $results['normal']['sent']++;
            
            if ($i % 5 == 0) {
                echo "    已发送: {$i}/{$testCount}\n";
            }
        }
        echo "  ✓ 成功发送 {$testCount} 条消息\n\n";
        
        // 创建 SimpleConsumer 接收消息
        echo "  → 创建 SimpleConsumer 接收消息...\n";
        $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
        
        // 启动 Consumer
        $consumer->start();
        
        $consumer->setMaxMessageNum(32);
        $consumer->setInvisibleDuration(30);
        
        $received = 0;
        $maxAttempts = 20;
        $attempt = 0;
        
        while ($received < $testCount && $attempt < $maxAttempts) {
            try {
                $messages = $consumer->receive(32, 30);
                foreach ($messages as $message) {
                    $consumer->ack($message);
                    $received++;
                    $results['normal']['received']++;
                }
                
                if ($received < $testCount) {
                    usleep(500000);
                }
            } catch (\Exception $e) {
                // 忽略超时等错误
            }
            $attempt++;
        }
        
        echo "  ✓ 成功接收 {$received}/{$testCount} 条消息\n";
        
        if ($received >= $testCount) {
            echo "  ✅ 普通消息测试通过\n\n";
        } else {
            echo "  ❌ 普通消息测试失败: 期望至少 {$testCount}, 实际 {$received}\n\n";
            $results['normal']['success'] = false;
        }
        
        $producer->shutdown();
        
    } catch (\Exception $e) {
        echo "  ❌ 普通消息测试异常: " . $e->getMessage() . "\n";
        echo "     堆栈: " . $e->getTraceAsString() . "\n\n";
        $results['normal']['success'] = false;
    }
}

/**
 * 测试2: 顺序消息
 */
function testOrderMessage($config, &$results) {
    echo "[测试 2/4] 顺序消息 (topic-order)\n";
    echo str_repeat("-", 60) . "\n";
    
    $topic = 'topic-order';
    $consumerGroup = 'GID-order-consumer';
    $testCount = 10;
    $messageGroup = 'order-group-1';
    
    try {
        // 创建 Producer
        echo "  → 创建 Producer (topic: {$topic})...\n";
        $producer = Producer::getInstance($config, $topic);
        $producer->start();
        
        // 发送顺序消息
        echo "  → 发送 {$testCount} 条顺序消息 (MessageGroup: {$messageGroup})...\n";
        for ($i = 1; $i <= $testCount; $i++) {
            $msgBuilder = new MessageBuilder();
            $message = $msgBuilder
                ->setTopic($topic)
                ->setBody("Order Message #{$i}")
                ->setTag("tag-order")
                ->setKeys(["key-order-{$i}"])
                ->setMessageGroup($messageGroup)
                ->build();
            
            $receipt = $producer->send($message);
            $results['order']['sent']++;
            
            if ($i % 5 == 0) {
                echo "    已发送: {$i}/{$testCount}\n";
            }
        }
        echo "  ✓ 成功发送 {$testCount} 条顺序消息\n\n";
        
        // 使用 SimpleConsumer 接收
        echo "  → 创建 SimpleConsumer 接收消息...\n";
        $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
        
        // 启动 Consumer
        $consumer->start();
        
        $consumer->setMaxMessageNum(32);
        $consumer->setInvisibleDuration(30);
        
        $received = 0;
        $maxAttempts = 20;
        $attempt = 0;
        
        while ($received < $testCount && $attempt < $maxAttempts) {
            try {
                $messages = $consumer->receive(32, 30);
                foreach ($messages as $message) {
                    $consumer->ack($message);
                    $received++;
                    $results['order']['received']++;
                }
                
                if ($received < $testCount) {
                    usleep(500000);
                }
            } catch (\Exception $e) {
                // 忽略超时
            }
            $attempt++;
        }
        
        echo "  ✓ 成功接收 {$received}/{$testCount} 条顺序消息\n";
        
        if ($received == $testCount) {
            echo "  ✅ 顺序消息测试通过\n\n";
        } else {
            echo "  ❌ 顺序消息测试失败: 期望 {$testCount}, 实际 {$received}\n\n";
            $results['order']['success'] = false;
        }
        
        $producer->shutdown();
        
    } catch (\Exception $e) {
        echo "  ❌ 顺序消息测试异常: " . $e->getMessage() . "\n";
        echo "     堆栈: " . $e->getTraceAsString() . "\n\n";
        $results['order']['success'] = false;
    }
}

/**
 * 测试3: 延时消息
 */
function testDelayMessage($config, &$results) {
    echo "[测试 3/4] 延时消息 (topic-delay)\n";
    echo str_repeat("-", 60) . "\n";
    
    $topic = 'topic-delay';
    $consumerGroup = 'GID-delay-consumer';
    $testCount = 5;
    $delaySeconds = 10;
    
    try {
        // 创建 Producer
        echo "  → 创建 Producer (topic: {$topic})...\n";
        $producer = Producer::getInstance($config, $topic);
        $producer->start();
        
        // 发送延时消息
        echo "  → 发送 {$testCount} 条延时消息 (延迟 {$delaySeconds} 秒)...\n";
        $startTime = time();
        
        for ($i = 1; $i <= $testCount; $i++) {
            $msgBuilder = new MessageBuilder();
            $message = $msgBuilder
                ->setTopic($topic)
                ->setBody("Delay Message #{$i}")
                ->setTag("tag-delay")
                ->setKeys(["key-delay-{$i}"])
                ->setDeliveryTimestamp(time() + $delaySeconds)
                ->build();
            
            $receipt = $producer->send($message);
            $results['delay']['sent']++;
            
            if ($i % 2 == 0) {
                echo "    已发送: {$i}/{$testCount}\n";
            }
        }
        echo "  ✓ 成功发送 {$testCount} 条延时消息\n";
        echo "  → 等待 {$delaySeconds} 秒让消息到期...\n";
        sleep($delaySeconds + 2);
        
        // 接收延时消息
        echo "  → 创建 SimpleConsumer 接收消息...\n";
        $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
        
        // 启动 Consumer
        $consumer->start();
        
        $consumer->setMaxMessageNum(32);
        $consumer->setInvisibleDuration(30);
        
        $received = 0;
        $maxAttempts = 10;
        $attempt = 0;
        
        while ($received < $testCount && $attempt < $maxAttempts) {
            try {
                $messages = $consumer->receive(32, 30);
                foreach ($messages as $message) {
                    $consumer->ack($message);
                    $received++;
                    $results['delay']['received']++;
                }
                
                if ($received < $testCount) {
                    usleep(1000000);
                }
            } catch (\Exception $e) {
                // 忽略超时
            }
            $attempt++;
        }
        
        $elapsed = time() - $startTime;
        echo "  ✓ 成功接收 {$received}/{$testCount} 条延时消息 (耗时: {$elapsed}秒)\n";
        
        if ($received == $testCount) {
            echo "  ✅ 延时消息测试通过\n\n";
        } else {
            echo "  ❌ 延时消息测试失败: 期望 {$testCount}, 实际 {$received}\n\n";
            $results['delay']['success'] = false;
        }
        
        $producer->shutdown();
        
    } catch (\Exception $e) {
        echo "  ❌ 延时消息测试异常: " . $e->getMessage() . "\n";
        echo "     堆栈: " . $e->getTraceAsString() . "\n\n";
        $results['delay']['success'] = false;
    }
}

/**
 * 测试4: 事务消息
 */
function testTransactionMessage($config, &$results) {
    echo "[测试 4/4] 事务消息 (topic-transaction)\n";
    echo str_repeat("-", 60) . "\n";
    
    $topic = 'topic-transaction';
    $consumerGroup = 'GID-transaction-consumer';
    $testCount = 5;
    
    try {
        // 创建带事务检查器的 Producer
        echo "  → 创建 Producer (topic: {$topic}, 带事务检查器)...\n";
        
        $producer = Producer::getInstance($config, $topic);
        $producer->setTransactionChecker(new SimpleTransactionChecker());
        $producer->start();
        
        // 发送事务消息
        echo "  → 发送 {$testCount} 条事务消息...\n";
        $committedCount = 0;
        
        for ($i = 1; $i <= $testCount; $i++) {
            // 开始事务
            $transaction = $producer->beginTransaction();
            
            try {
                $msgBuilder = new MessageBuilder();
                $message = $msgBuilder
                    ->setTopic($topic)
                    ->setBody("Transaction Message #{$i}")
                    ->setTag("tag-transaction")
                    ->setKeys(["key-transaction-{$i}"])
                    ->build();
                
                // 发送半消息
                $receipts = $producer->sendTransactionMessage(
                    "Transaction Message #{$i}",
                    $transaction,
                    "tag-transaction",
                    "key-transaction-{$i}"
                );
                $receipt = $receipts[0];
                
                echo "    [半消息] Message ID: {$receipt->getMessageId()}\n";
                
                // 模拟本地事务执行
                usleep(50000); // 50ms
                
                // 提交事务
                $transaction->commit();
                echo "    [事务提交] SUCCESS\n";
                
                $results['transaction']['sent']++;
                $committedCount++;
                
            } catch (\Exception $e) {
                echo "    [事务回滚] FAILED: {$e->getMessage()}\n";
                try {
                    $transaction->rollback();
                } catch (\Exception $rollbackError) {
                    echo "    [回滚失败] {$rollbackError->getMessage()}\n";
                }
            }
            
            if ($i % 2 == 0) {
                echo "    已发送: {$i}/{$testCount}\n";
            }
        }
        echo "  ✓ 成功发送 {$testCount} 条事务消息 (已提交: {$committedCount})\n";
        echo "  → 等待事务状态确认...\n";
        sleep(3);
        
        // 接收事务消息
        echo "  → 创建 SimpleConsumer 接收消息...\n";
        $consumer = SimpleConsumer::getInstance($config, $consumerGroup, $topic);
        
        // 启动 Consumer
        $consumer->start();
        
        $consumer->setMaxMessageNum(32);
        $consumer->setInvisibleDuration(30);
        
        $received = 0;
        $maxAttempts = 15;
        $attempt = 0;
        
        while ($received < $testCount && $attempt < $maxAttempts) {
            try {
                $messages = $consumer->receive(32, 30);
                foreach ($messages as $message) {
                    $consumer->ack($message);
                    $received++;
                    $results['transaction']['received']++;
                }
                
                if ($received < $testCount) {
                    usleep(1000000);
                }
            } catch (\Exception $e) {
                // 忽略超时
            }
            $attempt++;
        }
        
        echo "  ✓ 成功接收 {$received}/{$testCount} 条事务消息\n";
        
        if ($received == $testCount) {
            echo "  ✅ 事务消息测试通过\n\n";
        } else {
            echo "  ❌ 事务消息测试失败: 期望 {$testCount}, 实际 {$received}\n\n";
            $results['transaction']['success'] = false;
        }
        
        $producer->shutdown();
        
    } catch (\Exception $e) {
        echo "  ❌ 事务消息测试异常: " . $e->getMessage() . "\n";
        echo "     堆栈: " . $e->getTraceAsString() . "\n\n";
        $results['transaction']['success'] = false;
    }
}

// 执行所有测试
try {
    testNormalMessage($config, $results);
    testOrderMessage($config, $results);
    testDelayMessage($config, $results);
    testTransactionMessage($config, $results);
} catch (\Exception $e) {
    echo "\n❌ 测试执行异常: " . $e->getMessage() . "\n";
    echo "堆栈跟踪:\n" . $e->getTraceAsString() . "\n";
}

// 输出测试结果汇总
echo "\n";
echo "========================================\n";
echo "  测试结果汇总\n";
echo "========================================\n\n";

$totalSent = 0;
$totalReceived = 0;
$allPassed = true;

foreach ($results as $type => $result) {
    $typeName = [
        'normal' => '普通消息',
        'order' => '顺序消息',
        'delay' => '延时消息',
        'transaction' => '事务消息',
    ][$type];
    
    $status = $result['success'] ? '✅ 通过' : '❌ 失败';
    $icon = $result['success'] ? '✓' : '✗';
    
    echo "{$icon} {$typeName}:\n";
    echo "   状态: {$status}\n";
    echo "   发送: {$result['sent']} 条\n";
    echo "   接收: {$result['received']} 条\n";
    echo "\n";
    
    $totalSent += $result['sent'];
    $totalReceived += $result['received'];
    if (!$result['success']) {
        $allPassed = false;
    }
}

echo str_repeat("-", 60) . "\n";
echo "总计:\n";
echo "  发送: {$totalSent} 条\n";
echo "  接收: {$totalReceived} 条\n";
echo "  成功率: " . ($totalSent > 0 ? round($totalReceived / $totalSent * 100, 2) : 0) . "%\n\n";

if ($allPassed) {
    echo "🎉 所有测试通过！\n";
    exit(0);
} else {
    echo "⚠️  部分测试失败，请检查上述错误信息。\n";
    exit(1);
}
