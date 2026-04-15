<?php
/**
 * Test script for ClientMeterManager
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\HistogramEnum;
use Apache\Rocketmq\GaugeEnum;
use Apache\Rocketmq\MetricLabels;
use Apache\Rocketmq\InvocationStatus;

echo "=== ClientMeterManager Tests ===\n\n";

$testsPassed = 0;
$testsFailed = 0;

// Test 1: Initialization
echo "Test 1: Initialization\n";
try {
    $clientId = 'test-client-1';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    
    if (!$meterManager->isEnabled()) {
        echo "✓ PASS: Meter manager is disabled by default\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Meter manager should be disabled by default\n";
        $testsFailed++;
    }
    
    if ($meterManager->getClientId() === $clientId) {
        echo "✓ PASS: Client ID is correct\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Client ID is incorrect\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 2: Enable/Disable Metrics
echo "Test 2: Enable/Disable Metrics\n";
try {
    $clientId = 'test-client-2';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    
    // Enable metrics
    $meterManager->enable(null, 60);
    
    if ($meterManager->isEnabled()) {
        echo "✓ PASS: Metrics enabled successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Metrics should be enabled\n";
        $testsFailed++;
    }
    
    // Disable metrics
    $meterManager->disable();
    
    if (!$meterManager->isEnabled()) {
        echo "✓ PASS: Metrics disabled successfully\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Metrics should be disabled\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 3: Record Histogram Metrics
echo "Test 3: Record Histogram Metrics\n";
try {
    $clientId = 'test-client-3';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    
    // Enable metrics
    $meterManager->enable();
    
    // Record histogram
    $meterManager->record(
        HistogramEnum::SEND_COST_TIME,
        [
            MetricLabels::TOPIC => 'test-topic',
            MetricLabels::INVOCATION_STATUS => InvocationStatus::SUCCESS,
        ],
        123.45
    );
    
    // Check metrics were collected
    $metrics = $metricsCollector->getAllMetrics();
    $histogramMetrics = array_filter($metrics, function($m) {
        return $m['name'] === HistogramEnum::SEND_COST_TIME;
    });
    
    if (count($histogramMetrics) > 0) {
        echo "✓ PASS: Histogram metric recorded\n";
        $testsPassed++;
        
        // Verify client_id label was added
        $firstMetric = reset($histogramMetrics);
        if (isset($firstMetric['labels'][MetricLabels::CLIENT_ID]) &&
            $firstMetric['labels'][MetricLabels::CLIENT_ID] === $clientId) {
            echo "✓ PASS: Client ID label auto-added\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: Client ID label not added\n";
            $testsFailed++;
        }
    } else {
        echo "✗ FAIL: Histogram metric not recorded\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 4: Disabled Metrics Should Not Record
echo "Test 4: Disabled Metrics Should Not Record\n";
try {
    $clientId = 'test-client-4';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    
    // Keep metrics disabled
    
    // Try to record (should be ignored)
    $meterManager->record(
        HistogramEnum::SEND_COST_TIME,
        [MetricLabels::TOPIC => 'test-topic'],
        100.0
    );
    
    $metrics = $metricsCollector->getAllMetrics();
    
    if (count($metrics) === 0) {
        echo "✓ PASS: Disabled metrics not recorded\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Metrics should not be recorded when disabled\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 5: Register and Update Gauges
echo "Test 5: Register and Update Gauges\n";
try {
    $clientId = 'test-client-5';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    $meterManager->enable();
    
    // Register a gauge
    $counter = 0;
    $meterManager->registerGauge(
        GaugeEnum::CONSUMER_CACHED_MESSAGES,
        function() use (&$counter) {
            return floatval($counter);
        },
        [
            MetricLabels::TOPIC => 'test-topic',
            MetricLabels::CONSUMER_GROUP => 'test-group',
        ]
    );
    
    // Update counter
    $counter = 42;
    
    // Update gauges
    $meterManager->updateGauges();
    
    // Check gauge was updated
    $metrics = $metricsCollector->getAllMetrics();
    $gaugeMetrics = array_filter($metrics, function($m) {
        return $m['name'] === GaugeEnum::CONSUMER_CACHED_MESSAGES;
    });
    
    if (count($gaugeMetrics) > 0) {
        echo "✓ PASS: Gauge registered and updated\n";
        $testsPassed++;
        
        $firstGauge = reset($gaugeMetrics);
        if ($firstGauge['value'] == 42.0) {
            echo "✓ PASS: Gauge value is correct (42.0)\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: Gauge value is incorrect: " . $firstGauge['value'] . "\n";
            $testsFailed++;
        }
    } else {
        echo "✗ FAIL: Gauge not updated\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 6: Set Gauge Observer
echo "Test 6: Set Gauge Observer\n";
try {
    $clientId = 'test-client-6';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    $meterManager->enable();
    
    // Set gauge observer
    $meterManager->setGaugeObserver(function() {
        return [
            [
                'name' => GaugeEnum::CONSUMER_CACHED_BYTES,
                'value' => 1024.0,
                'labels' => [
                    MetricLabels::TOPIC => 'test-topic',
                    MetricLabels::CONSUMER_GROUP => 'test-group',
                ],
            ],
        ];
    });
    
    // Update gauges (should call observer)
    $meterManager->updateGauges();
    
    // Check observer was called
    $metrics = $metricsCollector->getAllMetrics();
    $observerMetrics = array_filter($metrics, function($m) {
        return $m['name'] === GaugeEnum::CONSUMER_CACHED_BYTES;
    });
    
    if (count($observerMetrics) > 0) {
        echo "✓ PASS: Gauge observer called successfully\n";
        $testsPassed++;
        
        $firstMetric = reset($observerMetrics);
        if ($firstMetric['value'] == 1024.0) {
            echo "✓ PASS: Observer returned correct value\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: Observer value incorrect\n";
            $testsFailed++;
        }
    } else {
        echo "✗ FAIL: Gauge observer not called\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 7: Increment Counter
echo "Test 7: Increment Counter\n";
try {
    $clientId = 'test-client-7';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    $meterManager->enable();
    
    // Increment counter
    $meterManager->incrementCounter(
        'test_counter',
        [MetricLabels::TOPIC => 'test-topic'],
        5
    );
    
    $metrics = $metricsCollector->getAllMetrics();
    $counterMetrics = array_filter($metrics, function($m) {
        return $m['name'] === 'test_counter';
    });
    
    if (count($counterMetrics) > 0) {
        echo "✓ PASS: Counter incremented\n";
        $testsPassed++;
        
        $firstCounter = reset($counterMetrics);
        if ($firstCounter['value'] == 5.0) {
            echo "✓ PASS: Counter value is correct (5.0)\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: Counter value incorrect: " . $firstCounter['value'] . "\n";
            $testsFailed++;
        }
    } else {
        echo "✗ FAIL: Counter not incremented\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 8: Export Metrics
echo "Test 8: Export Metrics\n";
try {
    $clientId = 'test-client-8';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    $meterManager->enable();
    
    // Record some metrics
    $meterManager->record(
        HistogramEnum::SEND_COST_TIME,
        [MetricLabels::TOPIC => 'test-topic'],
        100.0
    );
    
    // Export metrics
    $result = $meterManager->exportMetrics();
    
    if ($result['success']) {
        echo "✓ PASS: Metrics exported successfully\n";
        $testsPassed++;
        
        if (isset($result['count']) && $result['count'] > 0) {
            echo "✓ PASS: Exported metrics count: {$result['count']}\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: No metrics in export\n";
            $testsFailed++;
        }
        
        if (isset($result['clientId']) && $result['clientId'] === $clientId) {
            echo "✓ PASS: Export includes correct client ID\n";
            $testsPassed++;
        } else {
            echo "✗ FAIL: Export missing or incorrect client ID\n";
            $testsFailed++;
        }
    } else {
        echo "✗ FAIL: Export failed: " . ($result['message'] ?? 'Unknown') . "\n";
        $testsFailed++;
    }
    
} catch (\Exception $e) {
    echo "✗ FAIL: Exception - " . $e->getMessage() . "\n";
    $testsFailed++;
}

echo "\n";

// Test 9: Shutdown
echo "Test 9: Shutdown\n";
try {
    $clientId = 'test-client-9';
    $metricsCollector = new MetricsCollector($clientId);
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    $meterManager->enable();
    
    // Shutdown
    $meterManager->shutdown();
    
    if (!$meterManager->isEnabled()) {
        echo "✓ PASS: Meter manager disabled after shutdown\n";
        $testsPassed++;
    } else {
        echo "✗ FAIL: Meter manager should be disabled after shutdown\n";
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
