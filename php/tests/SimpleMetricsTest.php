<?php
/**
 * Simple Example: Enable Metrics with One Line
 * 
 * This example shows how easy it is to enable metrics collection
 * by calling just one method: enableMetrics()
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\Logger;

// Configure endpoints
$endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);

$topic = 'topic-normal';

try {
    // Create producer
    $producer = Producer::getInstance($config, $topic);
    
    // ✨ ONE LINE to enable metrics! ✨
    $producer->enableMetrics();
    
    Logger::info("Producer created with metrics enabled");
    
    // Start producer
    $producer->start();
    
    // Send messages - metrics are collected automatically!
    for ($i = 1; $i <= 3; $i++) {
        $message = (new MessageBuilder())
            ->setTopic($topic)
            ->setBody("Simple metrics example message #{$i}")
            ->build();
        
        $receipt = $producer->send($message);
        Logger::info("Message #{} sent, messageId={}", [$i, $receipt->getMessageId()]);
        
        usleep(50000); // 50ms delay
    }
    
    // Get meter manager for advanced operations
    $meterManager = $producer->getMeterManager();
    
    if ($meterManager !== null) {
        // Export metrics
        $result = $meterManager->exportMetrics();
        
        Logger::info("\n=== Collected {} metrics ===", [$result['count']]);
        
        foreach ($result['metrics'] as $metric) {
            Logger::info("  {}: {} {}", [
                $metric['name'],
                $metric['value'],
                json_encode($metric['labels'])
            ]);
        }
    }
    
    // Shutdown (automatically shuts down meter manager)
    $producer->shutdown();
    
    Logger::info("\n✓ Example completed!");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    exit(1);
}
