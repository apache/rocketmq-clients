<?php
/**
 * Example: Using Metrics with Consumer
 * 
 * This example demonstrates how to enable metrics for consumers
 * using the MetricsSupportTrait.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Consumer\FilterExpression;
use Apache\Rocketmq\Consumer\MessageListener;
use Apache\Rocketmq\Consumer\ConsumeResult;
use Apache\Rocketmq\MetricsSupportTrait;
use Apache\Rocketmq\Logger;

// Example consumer class that uses MetricsSupportTrait
class ExampleConsumer {
    use MetricsSupportTrait;
    
    private $consumerGroup;
    private $clientId;
    
    public function __construct(string $clientId, string $consumerGroup) {
        $this->clientId = $clientId;
        $this->consumerGroup = $consumerGroup;
        
        // Initialize metrics support
        $this->initMetrics($clientId);
        
        Logger::info("ExampleConsumer created, clientId={}, group={}", [
            $clientId,
            $consumerGroup
        ]);
    }
    
    public function getClientId(): string {
        return $this->clientId;
    }
    
    public function getConsumerGroup(): string {
        return $this->consumerGroup;
    }
    
    /**
     * Simulate consuming messages
     */
    public function consumeMessages(int $count = 5): void {
        Logger::info("Simulating consumption of {} messages", [$count]);
        
        for ($i = 1; $i <= $count; $i++) {
            // Simulate message processing
            usleep(rand(10000, 50000)); // 10-50ms
            
            Logger::info("Consumed message #{}", [$i]);
        }
        
        // Update custom gauge
        if ($this->meterManager !== null) {
            $this->meterManager->setGauge(
                \Apache\Rocketmq\GaugeEnum::CONSUMER_CACHED_MESSAGES,
                [
                    \Apache\Rocketmq\MetricLabels::TOPIC => 'test-topic',
                    \Apache\Rocketmq\MetricLabels::CONSUMER_GROUP => $this->consumerGroup,
                ],
                floatval($count)
            );
        }
    }
    
    /**
     * Shutdown consumer
     */
    public function shutdown(): void {
        Logger::info("Shutting down consumer...");
        $this->shutdownMetrics();
        Logger::info("Consumer shutdown complete");
    }
}

// Main example
$clientId = 'consumer-example-' . uniqid();
$consumerGroup = 'test-consumer-group';

try {
    // Create consumer
    $consumer = new ExampleConsumer($clientId, $consumerGroup);
    
    // ✨ Enable metrics with one line!
    $consumer->enableMetrics();
    
    Logger::info("Consumer started with metrics enabled");
    
    // Simulate message consumption
    $consumer->consumeMessages(10);
    
    // Get meter manager for advanced operations
    $meterManager = $consumer->getMeterManager();
    
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
    
    // Shutdown
    $consumer->shutdown();
    
    Logger::info("\n✓ Consumer example completed!");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    Logger::error("Stack trace: {}", [$e->getTraceAsString()]);
    exit(1);
}
