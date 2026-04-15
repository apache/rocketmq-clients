<?php
/**
 * Example: Using Message Meter Interceptor for Automatic Metrics Collection
 * 
 * This example demonstrates how to use the MessageMeterInterceptor to automatically
 * collect metrics for message send and consume operations.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\CompositedMessageInterceptor;
use Apache\Rocketmq\MessageMeterInterceptor;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\Logger;

// Configure endpoints
$endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);

$topic = 'topic-normal';
$clientId = 'producer-example-' . uniqid();

try {
    // Create metrics collector
    $metricsCollector = new MetricsCollector($clientId);
    
    // Create producer
    $producer = Producer::getInstance($config, $topic);
    
    // Create composited interceptor
    $interceptorChain = new CompositedMessageInterceptor();
    
    // Add message meter interceptor (for automatic metrics collection)
    $meterInterceptor = new MessageMeterInterceptor(
        $metricsCollector,
        $clientId
    );
    $interceptorChain->addInterceptor($meterInterceptor);
    
    // You can also add logging interceptor
    // $loggingInterceptor = new LoggingMessageInterceptor();
    // $interceptorChain->addInterceptor($loggingInterceptor);
    
    // Add interceptor chain to producer
    $producer->addInterceptor($interceptorChain);
    
    Logger::info("Producer configured with {} interceptor(s)", [
        $interceptorChain->getInterceptorCount()
    ]);
    
    // Start producer
    $producer->start();
    Logger::info("Producer started, clientId={}", [$clientId]);
    
    // Send messages (metrics will be collected automatically)
    for ($i = 1; $i <= 5; $i++) {
        $message = (new MessageBuilder())
            ->setTopic($topic)
            ->setBody("Message #{$i} with automatic metrics")
            ->setKeys(["meter-test", "message-{$i}"])
            ->build();
        
        try {
            $receipt = $producer->send($message);
            Logger::info("Message #{} sent successfully, messageId={}", [
                $i,
                $receipt->getMessageId()
            ]);
            
            // Simulate some delay
            usleep(100000); // 100ms
            
        } catch (\Exception $e) {
            Logger::error("Failed to send message #{}: {}", [$i, $e->getMessage()]);
        }
    }
    
    // Export metrics
    Logger::info("\n=== Collected Metrics ===");
    $metrics = $metricsCollector->getAllMetrics();
    
    foreach ($metrics as $key => $metric) {
        Logger::info("Metric: {} = {} (labels: {})", [
            $metric['name'],
            $metric['value'],
            json_encode($metric['labels'])
        ]);
    }
    
    // Export to JSON format
    $jsonMetrics = $metricsCollector->exportMetrics();
    Logger::info("\nMetrics in JSON format:\n{}", [$jsonMetrics]);
    
    // Shutdown producer
    $producer->shutdown();
    Logger::info("Producer shutdown successfully");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    Logger::error("Stack trace: {}", [$e->getTraceAsString()]);
    exit(1);
}
