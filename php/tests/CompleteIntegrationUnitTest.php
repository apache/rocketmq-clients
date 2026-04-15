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

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Message\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\SwooleAsyncSupport;
use Apache\Rocketmq\TelemetrySession;

echo "=== Complete Integration Test ===\n\n";

$testResults = [];

// ============================================
// Test 1: Producer Auto Metrics Initialization
// ============================================
echo "Test 1: Producer auto metrics initialization\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $producer = Producer::getInstance($config, 'test-topic');
    
    // Check that meterManager is auto-initialized
    $meterManager = $producer->getMeterManager();
    
    if ($meterManager !== null) {
        echo "✓ PASS: MeterManager auto-initialized\n";
        $testResults['producer_auto_metrics'] = true;
    } else {
        echo "✗ FAIL: MeterManager not initialized\n";
        $testResults['producer_auto_metrics'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['producer_auto_metrics'] = false;
}
echo "\n";

// ============================================
// Test 2: Consumer Auto Metrics Initialization
// ============================================
echo "Test 2: Consumer auto metrics initialization\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $consumer = SimpleConsumer::getInstance($config, 'test-group', 'test-topic');
    
    // Check that meterManager is auto-initialized
    $meterManager = $consumer->getMeterManager();
    
    if ($meterManager !== null) {
        echo "✓ PASS: MeterManager auto-initialized\n";
        $testResults['consumer_auto_metrics'] = true;
    } else {
        echo "✗ FAIL: MeterManager not initialized\n";
        $testResults['consumer_auto_metrics'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['consumer_auto_metrics'] = false;
}
echo "\n";

// ============================================
// Test 3: Producer Enable Metrics
// ============================================
echo "Test 3: Producer enableMetrics() method\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $producer = Producer::getInstance($config, 'test-topic');
    
    $result = $producer->enableMetrics('http://localhost:4318/v1/metrics', 30);
    
    if ($result === $producer) {
        echo "✓ PASS: enableMetrics() returns self for chaining\n";
        
        $meterManager = $producer->getMeterManager();
        if ($meterManager !== null && $meterManager->isEnabled()) {
            echo "✓ PASS: Metrics enabled successfully\n";
            $testResults['producer_enable_metrics'] = true;
        } else {
            echo "✗ FAIL: Metrics not enabled\n";
            $testResults['producer_enable_metrics'] = false;
        }
    } else {
        echo "✗ FAIL: enableMetrics() does not return self\n";
        $testResults['producer_enable_metrics'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['producer_enable_metrics'] = false;
}
echo "\n";

// ============================================
// Test 4: Consumer Enable Metrics
// ============================================
echo "Test 4: Consumer enableMetrics() method\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $consumer = SimpleConsumer::getInstance($config, 'test-group', 'test-topic');
    
    $result = $consumer->enableMetrics('http://localhost:4318/v1/metrics', 30);
    
    if ($result === $consumer) {
        echo "✓ PASS: enableMetrics() returns self for chaining\n";
        
        $meterManager = $consumer->getMeterManager();
        if ($meterManager !== null && $meterManager->isEnabled()) {
            echo "✓ PASS: Metrics enabled successfully\n";
            $testResults['consumer_enable_metrics'] = true;
        } else {
            echo "✗ FAIL: Metrics not enabled\n";
            $testResults['consumer_enable_metrics'] = false;
        }
    } else {
        echo "✗ FAIL: enableMetrics() does not return self\n";
        $testResults['consumer_enable_metrics'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['consumer_enable_metrics'] = false;
}
echo "\n";

// ============================================
// Test 5: TelemetrySession Class Exists
// ============================================
echo "Test 5: TelemetrySession class availability\n";
try {
    if (class_exists('\\Apache\\Rocketmq\\TelemetrySession')) {
        echo "✓ PASS: TelemetrySession class exists\n";
        
        // Check methods
        $methods = ['start', 'stop', 'isActive', 'send', 'setSettingsCallback'];
        $missingMethods = [];
        foreach ($methods as $method) {
            if (!method_exists('\\Apache\\Rocketmq\\TelemetrySession', $method)) {
                $missingMethods[] = $method;
            }
        }
        
        if (empty($missingMethods)) {
            echo "✓ PASS: All required methods exist\n";
            $testResults['telemetry_class'] = true;
        } else {
            echo "✗ FAIL: Missing methods: " . implode(', ', $missingMethods) . "\n";
            $testResults['telemetry_class'] = false;
        }
    } else {
        echo "✗ FAIL: TelemetrySession class not found\n";
        $testResults['telemetry_class'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['telemetry_class'] = false;
}
echo "\n";

// ============================================
// Test 6: SwooleAsyncSupport Availability
// ============================================
echo "Test 6: SwooleAsyncSupport availability\n";
try {
    if (class_exists('\\Apache\\Rocketmq\\SwooleAsyncSupport')) {
        echo "✓ PASS: SwooleAsyncSupport class exists\n";
        
        $requirements = SwooleAsyncSupport::checkRequirements();
        
        if ($requirements['available']) {
            echo "✓ PASS: Swoole extension loaded (version: {$requirements['version']})\n";
            
            if (SwooleAsyncSupport::isCoroutineEnabled()) {
                echo "✓ PASS: Coroutines enabled\n";
            } else {
                echo "⚠ WARNING: Coroutines not available\n";
            }
            
            $testResults['swoole_support'] = true;
        } else {
            echo "⚠ WARNING: Swoole not available\n";
            foreach ($requirements['issues'] as $issue) {
                echo "  - {$issue}\n";
            }
            $testResults['swoole_support'] = false;
        }
    } else {
        echo "✗ FAIL: SwooleAsyncSupport class not found\n";
        $testResults['swoole_support'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['swoole_support'] = false;
}
echo "\n";

// ============================================
// Test 7: Metrics Collection (without server)
// ============================================
echo "Test 7: Metrics collection functionality\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $producer = Producer::getInstance($config, 'test-topic');
    $producer->enableMetrics();
    
    $meterManager = $producer->getMeterManager();
    
    // Record some test metrics
    $meterManager->record('test_histogram', ['tag' => 'value1'], 100.5);
    $meterManager->setGauge('test_gauge', ['tag' => 'value2'], 42.0);
    $meterManager->incrementCounter('test_counter', ['tag' => 'value3'], 1);
    
    // Update gauges
    $meterManager->updateGauges();
    
    // Export metrics
    $metrics = $meterManager->exportMetrics();
    
    if (is_array($metrics) && count($metrics) > 0) {
        echo "✓ PASS: Metrics collected successfully\n";
        echo "  - Total metric types: " . count($metrics) . "\n";
        foreach ($metrics as $name => $dataPoints) {
            if (is_array($dataPoints)) {
                echo "  - {$name}: " . count($dataPoints) . " data points\n";
            } else {
                echo "  - {$name}: exported\n";
            }
        }
        $testResults['metrics_collection'] = true;
    } else {
        echo "✗ FAIL: No metrics collected\n";
        $testResults['metrics_collection'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['metrics_collection'] = false;
}
echo "\n";

// ============================================
// Test 8: Disable Metrics
// ============================================
echo "Test 8: Disable metrics functionality\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $producer = Producer::getInstance($config, 'test-topic');
    $producer->enableMetrics();
    
    $producer->disableMetrics();
    
    $meterManager = $producer->getMeterManager();
    if (!$meterManager->isEnabled()) {
        echo "✓ PASS: Metrics disabled successfully\n";
        $testResults['disable_metrics'] = true;
    } else {
        echo "✗ FAIL: Metrics still enabled after disable\n";
        $testResults['disable_metrics'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['disable_metrics'] = false;
}
echo "\n";

// ============================================
// Test 9: Method Chaining
// ============================================
echo "Test 9: Method chaining support\n";
try {
    $config = new ClientConfiguration('127.0.0.1:8080');
    $producer = Producer::getInstance($config, 'test-topic');
    
    // Test chaining
    $result = $producer->enableMetrics()->disableMetrics()->enableMetrics();
    
    if ($result === $producer) {
        echo "✓ PASS: Method chaining works correctly\n";
        $testResults['method_chaining'] = true;
    } else {
        echo "✗ FAIL: Method chaining broken\n";
        $testResults['method_chaining'] = false;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: " . $e->getMessage() . "\n";
    $testResults['method_chaining'] = false;
}
echo "\n";

// ============================================
// Summary
// ============================================
echo str_repeat("=", 60) . "\n";
echo "TEST SUMMARY\n";
echo str_repeat("=", 60) . "\n\n";

$totalTests = count($testResults);
$passedTests = count(array_filter($testResults));
$failedTests = $totalTests - $passedTests;

foreach ($testResults as $test => $result) {
    $status = $result ? '✓ PASS' : '✗ FAIL';
    printf("%-40s %s\n", $test, $status);
}

echo "\n" . str_repeat("-", 60) . "\n";
echo "Total: {$totalTests} | Passed: {$passedTests} | Failed: {$failedTests}\n";

if ($failedTests === 0) {
    echo "\n🎉 All tests passed!\n";
    exit(0);
} else {
    echo "\n⚠ Some tests failed. Please review the output above.\n";
    exit(1);
}
