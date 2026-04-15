<?php
/**
 * Example: Using the Interceptor Framework
 * 
 * This example demonstrates how to use the message interceptor framework
 * for logging, monitoring, and custom processing.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Builder\MessageBuilder;
use Apache\Rocketmq\Producer;
use Apache\Rocketmq\CompositedMessageInterceptor;
use Apache\Rocketmq\LoggingMessageInterceptor;
use Apache\Rocketmq\Logger;

// Configure endpoints
$endpoints = getenv('ROCKETMQ_ENDPOINTS') ?: '127.0.0.1:8080';
$config = new ClientConfiguration($endpoints);

$topic = 'topic-normal';

try {
    // Create producer
    $producer = Producer::getInstance($config, $topic);
    
    // Create composited interceptor
    $interceptorChain = new CompositedMessageInterceptor();
    
    // Add logging interceptor
    $loggingInterceptor = new LoggingMessageInterceptor();
    $interceptorChain->addInterceptor($loggingInterceptor);
    
    // You can add more custom interceptors here
    // $customInterceptor = new MyCustomInterceptor();
    // $interceptorChain->addInterceptor($customInterceptor);
    
    // Add interceptor chain to producer
    $producer->addInterceptor($interceptorChain);
    
    Logger::info("Producer configured with {} interceptor(s)", [
        $interceptorChain->getInterceptorCount()
    ]);
    
    // Start producer
    $producer->start();
    Logger::info("Producer started, clientId={}", [$producer->getClientId()]);
    
    // Send a message (interceptors will be invoked automatically)
    $message = (new MessageBuilder())
        ->setTopic($topic)
        ->setBody("Hello from interceptor example!")
        ->setKeys(["interceptor-test"])
        ->build();
    
    $receipt = $producer->send($message);
    Logger::info("Message sent successfully, messageId={}", [$receipt->getMessageId()]);
    
    // Shutdown producer
    $producer->shutdown();
    Logger::info("Producer shutdown successfully");
    
} catch (\Exception $e) {
    Logger::error("Error: {}", [$e->getMessage()]);
    exit(1);
}
