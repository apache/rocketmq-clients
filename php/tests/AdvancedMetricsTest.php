<?php
/**
 * Advanced Example: OTLP Export and Performance Optimization
 * 
 * This example demonstrates:
 * 1. OTLP export configuration (for Prometheus/Grafana)
 * 2. Batch export for better performance
 * 3. Async reporting patterns
 * 4. Custom gauge observers
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\HistogramEnum;
use Apache\Rocketmq\GaugeEnum;
use Apache\Rocketmq\MetricLabels;
use Apache\Rocketmq\InvocationStatus;
use Apache\Rocketmq\Logger;

$clientId = 'advanced-metrics-' . uniqid();
$topic = 'topic-normal';

try {
    Logger::info("=== Advanced Metrics Example ===\n");
    
    // ========================================
    // 1. Create Producer with Metrics
    // ========================================
    $endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8080';
    $config = new ClientConfiguration($endpoints);
    $producer = Producer::getInstance($config, $topic);
    
    // Enable metrics with OTLP endpoint (for Prometheus)
    // Note: OTLP export is not yet fully implemented
    // This shows the intended API
    $producer->enableMetrics(
        null,  // 'http://localhost:4317' for OTLP
        60     // Export interval: 60 seconds
    );
    
    Logger::info("Producer created with metrics enabled");
    
    // ========================================
    // 2. Register Custom Gauge Observer
    // ========================================
    $meterManager = $producer->getMeterManager();
    
    if ($meterManager !== null) {
        // Simulated counters for demonstration
        $totalMessagesSent = 0;
        $totalBytesSent = 0;
        
        // Register gauge observer that returns multiple gauges
        $meterManager->setGaugeObserver(function() use (&$totalMessagesSent, &$totalBytesSent, $topic) {
            return [
                [
                    'name' => 'custom_messages_sent_total',
                    'value' => floatval($totalMessagesSent),
                    'labels' => [
                        MetricLabels::TOPIC => $topic,
                    ],
                ],
                [
                    'name' => 'custom_bytes_sent_total',
                    'value' => floatval($totalBytesSent),
                    'labels' => [
                        MetricLabels::TOPIC => $topic,
                    ],
                ],
            ];
        });
        
        Logger::info("Custom gauge observer registered");
    }
    
    // Start producer
    $producer->start();
    
    // ========================================
    // 3. Send Messages and Collect Metrics
    // ========================================
    Logger::info("\n=== Sending Messages ===");
    
    for ($i = 1; $i <= 10; $i++) {
        $body = "Advanced metrics message #{$i}";
        $message = (new MessageBuilder())
            ->setTopic($topic)
            ->setBody($body)
            ->build();
        
        $startTime = microtime(true);
        $receipt = $producer->send($message);
        $endTime = microtime(true);
        
        $durationMs = ($endTime - $startTime) * 1000;
        
        // Update counters
        $totalMessagesSent++;
        $totalBytesSent += strlen($body);
        
        Logger::info("Message #{} sent in {:.2f}ms", [$i, $durationMs]);
        
        usleep(50000); // 50ms delay
    }
    
    // ========================================
    // 4. Record Custom Metrics
    // ========================================
    Logger::info("\n=== Recording Custom Metrics ===");
    
    if ($meterManager !== null) {
        // Record custom histogram
        $meterManager->record(
            HistogramEnum::SEND_COST_TIME,
            [
                MetricLabels::TOPIC => 'custom-topic',
                MetricLabels::INVOCATION_STATUS => InvocationStatus::SUCCESS,
            ],
            150.5  // Custom value
        );
        
        // Increment custom counter
        $meterManager->incrementCounter(
            'custom_operation_count',
            [
                'operation' => 'test',
            ],
            5
        );
        
        // Set custom gauge
        $meterManager->setGauge(
            'custom_queue_size',
            [
                'queue' => 'test-queue',
            ],
            42.0
        );
        
        Logger::info("Custom metrics recorded");
    }
    
    // ========================================
    // 5. Update Gauges Manually
    // ========================================
    Logger::info("\n=== Updating Gauges ===");
    
    if ($meterManager !== null) {
        $meterManager->updateGauges();
        Logger::info("Gauges updated (observer called)");
    }
    
    // ========================================
    // 6. Export Metrics
    // ========================================
    Logger::info("\n=== Exporting Metrics ===");
    
    if ($meterManager !== null) {
        $result = $meterManager->exportMetrics();
        
        if ($result['success']) {
            Logger::info("Export successful!");
            Logger::info("Total metrics: {}", [$result['count']]);
            
            // Group metrics by type
            $metricsByType = [];
            foreach ($result['metrics'] as $metric) {
                $type = $metric['type'] ?? 'unknown';
                if (!isset($metricsByType[$type])) {
                    $metricsByType[$type] = 0;
                }
                $metricsByType[$type]++;
            }
            
            foreach ($metricsByType as $type => $count) {
                Logger::info("  {}: {} metrics", [$type, $count]);
            }
            
            // Show sample metrics
            Logger::info("\nSample metrics:");
            $sampleCount = 0;
            foreach ($result['metrics'] as $metric) {
                if ($sampleCount >= 5) break;
                
                Logger::info("  - {}: {} {}", [
                    $metric['name'],
                    $metric['value'],
                    json_encode($metric['labels'])
                ]);
                $sampleCount++;
            }
        } else {
            Logger::error("Export failed: {}", [$result['message'] ?? 'Unknown']);
        }
    }
    
    // ========================================
    // 7. Performance Tips
    // ========================================
    Logger::info("\n=== Performance Optimization Tips ===");
    Logger::info("1. Use batch export instead of individual exports");
    Logger::info("2. Configure appropriate export intervals (30-60s recommended)");
    Logger::info("3. Use async reporting for high-throughput scenarios");
    Logger::info("4. Limit the number of unique label combinations");
    Logger::info("5. Monitor memory usage of metrics collector");
    Logger::info("6. Consider using sampling for very high-frequency metrics");
    
    // ========================================
    // 8. Shutdown
    // ========================================
    $producer->shutdown();
    
    Logger::info("\n✓ Advanced metrics example completed!");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    Logger::error("Stack trace: {}", [$e->getTraceAsString()]);
    exit(1);
}
