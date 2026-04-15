<?php
/**
 * Example: Using ClientMeterManager for Complete Metrics Management
 * 
 * This example demonstrates how to use ClientMeterManager to:
 * - Enable/disable metrics collection
 * - Register custom gauges
 * - Record histogram metrics
 * - Export metrics data
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\CompositedMessageInterceptor;
use Apache\Rocketmq\MessageMeterInterceptor;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\HistogramEnum;
use Apache\Rocketmq\GaugeEnum;
use Apache\Rocketmq\MetricLabels;
use Apache\Rocketmq\Logger;

// Configure endpoints
$endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);

$topic = 'topic-normal';
$clientId = 'meter-manager-example-' . uniqid();

try {
    // Step 1: Create metrics collector
    $metricsCollector = new MetricsCollector($clientId);
    
    // Step 2: Create client meter manager
    $meterManager = new ClientMeterManager($clientId, $metricsCollector);
    
    // Step 3: Enable metrics (optionally configure export endpoint)
    $meterManager->enable(
        null,  // No remote export endpoint (local only)
        60     // Export interval: 60 seconds
    );
    
    Logger::info("ClientMeterManager enabled, clientId={}", [$clientId]);
    
    // Step 4: Register custom gauges
    $messageCount = 0;
    $totalBytes = 0;
    
    $meterManager->registerGauge(
        GaugeEnum::CONSUMER_CACHED_MESSAGES,
        function() use (&$messageCount) {
            return floatval($messageCount);
        },
        [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CONSUMER_GROUP => 'test-group',
        ]
    );
    
    $meterManager->registerGauge(
        GaugeEnum::CONSUMER_CACHED_BYTES,
        function() use (&$totalBytes) {
            return floatval($totalBytes);
        },
        [
            MetricLabels::TOPIC => $topic,
            MetricLabels::CONSUMER_GROUP => 'test-group',
        ]
    );
    
    Logger::info("Custom gauges registered");
    
    // Step 5: Create producer with message meter interceptor
    $producer = Producer::getInstance($config, $topic);
    
    $interceptorChain = new CompositedMessageInterceptor();
    
    // Add message meter interceptor
    $meterInterceptor = new MessageMeterInterceptor(
        $metricsCollector,
        $clientId
    );
    $interceptorChain->addInterceptor($meterInterceptor);
    
    $producer->addInterceptor($interceptorChain);
    
    // Start producer
    $producer->start();
    Logger::info("Producer started with metrics enabled");
    
    // Step 6: Send messages and collect metrics automatically
    for ($i = 1; $i <= 5; $i++) {
        $body = "Message #{$i} with meter manager";
        $message = (new MessageBuilder())
            ->setTopic($topic)
            ->setBody($body)
            ->setKeys(["meter-manager-test", "message-{$i}"])
            ->build();
        
        try {
            $receipt = $producer->send($message);
            
            // Update custom gauges
            $messageCount++;
            $totalBytes += strlen($body);
            
            Logger::info("Message #{} sent, messageId={}, totalMessages={}, totalBytes={}", [
                $i,
                $receipt->getMessageId(),
                $messageCount,
                $totalBytes
            ]);
            
            // Simulate some delay
            usleep(100000); // 100ms
            
        } catch (\Exception $e) {
            Logger::error("Failed to send message #{}: {}", [$i, $e->getMessage()]);
        }
    }
    
    // Step 7: Manually record custom metrics
    Logger::info("\n=== Recording Custom Metrics ===");
    
    // Record a custom histogram
    $meterManager->record(
        HistogramEnum::SEND_COST_TIME,
        [
            MetricLabels::TOPIC => 'custom-topic',
            MetricLabels::INVOCATION_STATUS => \Apache\Rocketmq\InvocationStatus::SUCCESS,
        ],
        123.45  // Custom value in milliseconds
    );
    
    // Increment a custom counter
    $meterManager->incrementCounter(
        'custom_message_count',
        [
            MetricLabels::TOPIC => $topic,
        ],
        10
    );
    
    // Step 8: Update gauges manually
    Logger::info("Updating gauges...");
    $meterManager->updateGauges();
    
    // Step 9: Export metrics
    Logger::info("\n=== Exporting Metrics ===");
    $exportResult = $meterManager->exportMetrics();
    
    if ($exportResult['success']) {
        Logger::info("Export successful!");
        Logger::info("Total metrics: {}", [$exportResult['count']]);
        
        // Display metrics summary
        if (isset($exportResult['metrics'])) {
            foreach ($exportResult['metrics'] as $metric) {
                Logger::info("  - {}: {} (labels: {})", [
                    $metric['name'],
                    $metric['value'],
                    json_encode($metric['labels'])
                ]);
            }
        }
    } else {
        Logger::error("Export failed: {}", [$exportResult['message'] ?? 'Unknown error']);
    }
    
    // Step 10: Export to JSON format
    Logger::info("\n=== Metrics in JSON Format ===");
    $jsonMetrics = $metricsCollector->exportMetrics();
    echo $jsonMetrics . "\n";
    
    // Step 11: Shutdown
    $producer->shutdown();
    $meterManager->shutdown();
    
    Logger::info("\n✓ Example completed successfully!");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    Logger::error("Stack trace: {}", [$e->getTraceAsString()]);
    exit(1);
}
