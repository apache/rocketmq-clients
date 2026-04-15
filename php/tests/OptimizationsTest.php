<?php
/**
 * Test script for PHP client optimizations
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Credentials;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\ProducerManager;
use Apache\Rocketmq\PublishingSettings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\Logger;

echo "=== PHP Client Optimization Tests ===\n\n";

$testsPassed = 0;
$testsFailed = 0;

// Test 1: ProducerManager Singleton Pattern
echo "Test 1: ProducerManager Singleton Pattern\n";
try {
    $endpoints = '127.0.0.1:8080';
    $config = new ClientConfiguration($endpoints);
    
    // Get producer first time
    $producer1 = ProducerManager::getProducer($config, 'test-topic-1');
    
    // Get producer second time (should be same instance)
    $producer2 = ProducerManager::getProducer($config, 'test-topic-1');
    
    if ($producer1 === $producer2) {
        echo "✓ PASS: Same producer instance returned (singleton working)\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Different producer instances returned\n";
        $testsFailed++;
    }
    
    // Cleanup
    ProducerManager::shutdownAll();
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 2: PublishingSettings Creation
echo "Test 2: PublishingSettings Creation\n";
try {
    $settings = new PublishingSettings(
        '',  // namespace
        'test-client-id',
        '127.0.0.1:8080',
        ['topic-1', 'topic-2'],
        3,   // max attempts
        3000 // timeout
    );
    
    if ($settings->getClientId() === 'test-client-id') {
        echo "✓ PASS: Settings created with correct client ID\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Settings has wrong client ID\n";
        $testsFailed++;
    }
    
    if (count($settings->getTopics()) === 2) {
        echo "✓ PASS: Settings has correct number of topics\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Settings has wrong number of topics\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 3: PublishingSettings Topic Management
echo "Test 3: PublishingSettings Topic Management\n";
try {
    $settings = new PublishingSettings('', 'client', '127.0.0.1:8080', ['topic-1']);
    $settings->addTopic('topic-2');
    $settings->addTopic('topic-1'); // Duplicate should not be added
    
    if (count($settings->getTopics()) === 2) {
        echo "✓ PASS: Topics managed correctly (no duplicates)\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Wrong topic count: " . count($settings->getTopics()) . "\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 4: PublishingSettings Configuration
echo "Test 4: PublishingSettings Configuration\n";
try {
    $settings = new PublishingSettings('', 'client', '127.0.0.1:8080');
    
    // Test max body size
    $settings->setMaxBodySize(8 * 1024 * 1024);
    if ($settings->getMaxBodySize() === 8 * 1024 * 1024) {
        echo "✓ PASS: Max body size configured correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Max body size not set correctly\n";
        $testsFailed++;
    }
    
    // Test message type validation
    $settings->setValidateMessageType(false);
    if ($settings->isValidateMessageType() === false) {
        echo "✓ PASS: Message type validation toggle works\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Message type validation not toggled\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 5: ProducerManager Statistics
echo "Test 5: ProducerManager Statistics\n";
try {
    ProducerManager::shutdownAll(); // Clear previous state
    
    $config = new ClientConfiguration('127.0.0.1:8080');
    
    // Get stats before creating producers
    $stats1 = ProducerManager::getStats();
    if ($stats1['total_producers'] === 0) {
        echo "✓ PASS: Initial stats show 0 producers\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Initial stats incorrect\n";
        $testsFailed++;
    }
    
    // Create a producer
    $producer = ProducerManager::getProducer($config, 'test-topic-stats');
    
    // Get stats after creating producer
    $stats2 = ProducerManager::getStats();
    if ($stats2['total_producers'] >= 1) {
        echo "✓ PASS: Stats updated after creating producer\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Stats not updated\n";
        $testsFailed++;
    }
    
    // Cleanup
    ProducerManager::shutdownAll();
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 6: MessageInterceptor Interface
echo "Test 6: MessageInterceptor Interface\n";
try {
    // Check if interface exists and has required methods
    $reflection = new ReflectionClass('Apache\Rocketmq\MessageInterceptor');
    $methods = $reflection->getMethods();
    $methodNames = array_map(fn($m) => $m->getName(), $methods);
    
    $requiredMethods = ['beforeSend', 'afterSend', 'beforeConsume', 'afterConsume'];
    $hasAllMethods = true;
    foreach ($requiredMethods as $method) {
        if (!in_array($method, $methodNames)) {
            $hasAllMethods = false;
            break;
        }
    }
    
    if ($hasAllMethods) {
        echo "✓ PASS: MessageInterceptor has all required methods\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: MessageInterceptor missing required methods\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 7: Multiple Producers for Different Topics
echo "Test 7: Multiple Producers for Different Topics\n";
try {
    ProducerManager::shutdownAll(); // Clear previous state
    
    $config = new ClientConfiguration('127.0.0.1:8080');
    
    // Create producers for different topics
    $producer1 = ProducerManager::getProducer($config, 'topic-A');
    $producer2 = ProducerManager::getProducer($config, 'topic-B');
    $producer3 = ProducerManager::getProducer($config, 'topic-C');
    
    $stats = ProducerManager::getStats();
    if ($stats['total_producers'] === 3) {
        echo "✓ PASS: Created 3 separate producers for 3 topics\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Expected 3 producers, got " . $stats['total_producers'] . "\n";
        $testsFailed++;
    }
    
    // Verify active topics
    $activeTopics = $stats['active_topics'];
    if (count($activeTopics) === 3) {
        echo "✓ PASS: Active topics tracked correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Active topics count incorrect\n";
        $testsFailed++;
    }
    
    // Cleanup
    ProducerManager::shutdownAll();
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 8: Graceful Shutdown
echo "Test 8: Graceful Shutdown\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    
    // Create multiple producers
    ProducerManager::getProducer($config, 'shutdown-test-1');
    ProducerManager::getProducer($config, 'shutdown-test-2');
    
    // Shutdown all
    ProducerManager::shutdownAll();
    
    // Verify all are shutdown
    $stats = ProducerManager::getStats();
    if ($stats['total_producers'] === 0) {
        echo "✓ PASS: All producers shutdown successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Some producers still active after shutdown\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception during shutdown - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Summary
echo "=== Test Summary ===\n";
echo "Total Tests: " . ($testsPassed + $testsFailed) . "\n";
echo "Passed: {$testsPassed}\n";
echo "Failed: {$testsFailed}\n";

if ($testsFailed === 0) {
    echo "\n✓ All tests passed!\n";
    exit(0);
} else {
    echo "\n✗ Some tests failed\n";
    exit(1);
}
