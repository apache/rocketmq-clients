<?php
/**
 * Test script for metrics constants
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\MetricLabels;
use Apache\Rocketmq\HistogramEnum;
use Apache\Rocketmq\GaugeEnum;
use Apache\Rocketmq\InvocationStatus;

echo "=== Metrics Constants Tests ===\n\n";

$testsPassed = 0;
$testsFailed = 0;

// Test 1: MetricLabels
echo "Test 1: MetricLabels\n";
try {
    if (MetricLabels::TOPIC === 'topic') {
        echo "✓ PASS: TOPIC constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: TOPIC constant is wrong\n";
        $testsFailed++;
    }
    
    if (MetricLabels::CLIENT_ID === 'client_id') {
        echo "✓ PASS: CLIENT_ID constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: CLIENT_ID constant is wrong\n";
        $testsFailed++;
    }
    
    if (MetricLabels::CONSUMER_GROUP === 'consumer_group') {
        echo "✓ PASS: CONSUMER_GROUP constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: CONSUMER_GROUP constant is wrong\n";
        $testsFailed++;
    }
    
    if (MetricLabels::INVOCATION_STATUS === 'invocation_status') {
        echo "✓ PASS: INVOCATION_STATUS constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: INVOCATION_STATUS constant is wrong\n";
        $testsFailed++;
    }
    
    $allLabels = MetricLabels::getAll();
    if (count($allLabels) === 4 && in_array('topic', $allLabels)) {
        echo "✓ PASS: getAll() returns all standard labels\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getAll() is incorrect\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 2: HistogramEnum
echo "Test 2: HistogramEnum\n";
try {
    if (HistogramEnum::SEND_COST_TIME === 'rocketmq_send_cost_time') {
        echo "✓ PASS: SEND_COST_TIME constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: SEND_COST_TIME constant is wrong\n";
        $testsFailed++;
    }
    
    $buckets = HistogramEnum::getBuckets(HistogramEnum::SEND_COST_TIME);
    if (count($buckets) === 7 && $buckets[0] === 1.0) {
        echo "✓ PASS: getBuckets() returns correct bucket configuration\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getBuckets() is incorrect\n";
        $testsFailed++;
    }
    
    $labels = HistogramEnum::getLabels(HistogramEnum::SEND_COST_TIME);
    if (in_array(MetricLabels::TOPIC, $labels) && in_array(MetricLabels::CLIENT_ID, $labels)) {
        echo "✓ PASS: getLabels() returns correct labels for SEND_COST_TIME\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getLabels() is incorrect\n";
        $testsFailed++;
    }
    
    $allHistograms = HistogramEnum::getAll();
    if (count($allHistograms) === 4) {
        echo "✓ PASS: getAll() returns all histogram names\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getAll() count is wrong\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 3: GaugeEnum
echo "Test 3: GaugeEnum\n";
try {
    if (GaugeEnum::CONSUMER_CACHED_MESSAGES === 'rocketmq_consumer_cached_messages') {
        echo "✓ PASS: CONSUMER_CACHED_MESSAGES constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: CONSUMER_CACHED_MESSAGES constant is wrong\n";
        $testsFailed++;
    }
    
    if (GaugeEnum::CONSUMER_CACHED_BYTES === 'rocketmq_consumer_cached_bytes') {
        echo "✓ PASS: CONSUMER_CACHED_BYTES constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: CONSUMER_CACHED_BYTES constant is wrong\n";
        $testsFailed++;
    }
    
    $labels = GaugeEnum::getLabels(GaugeEnum::CONSUMER_CACHED_MESSAGES);
    if (in_array(MetricLabels::CONSUMER_GROUP, $labels)) {
        echo "✓ PASS: getLabels() returns correct labels for CONSUMER_CACHED_MESSAGES\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getLabels() is incorrect\n";
        $testsFailed++;
    }
    
    $allGauges = GaugeEnum::getAll();
    if (count($allGauges) === 2) {
        echo "✓ PASS: getAll() returns all gauge names\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getAll() count is wrong\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 4: InvocationStatus
echo "Test 4: InvocationStatus\n";
try {
    if (InvocationStatus::SUCCESS === 'success') {
        echo "✓ PASS: SUCCESS constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: SUCCESS constant is wrong\n";
        $testsFailed++;
    }
    
    if (InvocationStatus::FAILURE === 'failure') {
        echo "✓ PASS: FAILURE constant is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: FAILURE constant is wrong\n";
        $testsFailed++;
    }
    
    if (InvocationStatus::isValid('success') && InvocationStatus::isValid('failure')) {
        echo "✓ PASS: isValid() validates correctly\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: isValid() is incorrect\n";
        $testsFailed++;
    }
    
    if (!InvocationStatus::isValid('invalid')) {
        echo "✓ PASS: isValid() rejects invalid status\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: isValid() should reject invalid status\n";
        $testsFailed++;
    }
    
    $allStatuses = InvocationStatus::getAll();
    if (count($allStatuses) === 2) {
        echo "✓ PASS: getAll() returns all status values\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: getAll() count is wrong\n";
        $testsFailed++;
    }
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
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
