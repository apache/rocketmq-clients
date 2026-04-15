<?php
/**
 * Route Module Usage Example
 * 
 * This example demonstrates how to use the Route module for managing topic routing information.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\Route\Address;
use Apache\Rocketmq\Route\AddressScheme;
use Apache\Rocketmq\Route\Endpoints;
use Apache\Rocketmq\Route\Broker;
use Apache\Rocketmq\Route\MessageQueue;
use Apache\Rocketmq\Route\TopicRouteData;
use Apache\Rocketmq\Route\RouteManager;
use Apache\Rocketmq\Route\SubscriptionLoadBalancer;
use Apache\Rocketmq\ClientConfiguration;

echo "=== Route Module Usage Example ===\n\n";

// 1. Create Address
echo "1. Creating Address...\n";
$address = new Address('127.0.0.1', 8080);
echo "   Address: {$address}\n";
echo "   Host: {$address->getHost()}, Port: {$address->getPort()}\n\n";

// 2. Create Endpoints from string
echo "2. Creating Endpoints from string...\n";
$endpoints = Endpoints::fromString('127.0.0.1:8080');
echo "   Endpoints: {$endpoints}\n";
echo "   Scheme: {$endpoints->getScheme()}\n";
echo "   gRPC Target: {$endpoints->getGrpcTarget()}\n\n";

// 3. Create Endpoints from domain name
echo "3. Creating Endpoints from domain name...\n";
$domainEndpoints = Endpoints::fromString('dns:rocketmq.example.com:8080');
echo "   Endpoints: {$domainEndpoints}\n";
echo "   Scheme: {$domainEndpoints->getScheme()}\n";
echo "   gRPC Target: {$domainEndpoints->getGrpcTarget()}\n\n";

// 4. Create RouteManager
echo "4. Creating RouteManager...\n";
$routeManager = RouteManager::getInstance();
$routeManager->setCacheTtl(30); // 30 seconds
$routeManager->setEnableCache(true);
echo "   RouteManager created with TTL=30s, cache enabled\n\n";

// 5. Example: Query route data (would need actual broker connection)
echo "5. Query route data example:\n";
echo "   In production, you would call:\n";
echo "   \$routeData = \$routeManager->getRouteData('your-topic');\n";
echo "   This will:\n";
echo "   - Check cache first\n";
echo "   - If cache expired, query from broker\n";
echo "   - Return TopicRouteData with message queues\n\n";

// 6. TopicRouteData features
echo "6. TopicRouteData features:\n";
echo "   - getTotalEndpoints(): Get all unique broker endpoints\n";
echo "   - getMessageQueues(): Get all message queues\n";
echo "   - pickEndpointsToQueryAssignments(): Round-robin endpoint selection\n";
echo "   - getMessageQueue(topic, queueId): Get specific queue\n";
echo "   - getQueueCount(): Get total queue count\n\n";

// 7. MessageQueue features
echo "7. MessageQueue features:\n";
echo "   - getTopic(): Get topic name\n";
echo "   - getBroker(): Get broker information\n";
echo "   - getQueueId(): Get queue ID\n";
echo "   - getPermission(): Get permission (NONE/READ/WRITE/READ_WRITE)\n";
echo "   - isReadable(): Check if readable\n";
echo "   - isWritable(): Check if writable\n";
echo "   - getKey(): Get unique key (topic@broker:queueId)\n\n";

// 8. SubscriptionLoadBalancer
echo "8. SubscriptionLoadBalancer features:\n";
echo "   - pickMessageQueue(): Consistent hashing queue selection\n";
echo "   - getMessageQueues(): Get all queues\n";
echo "   - getQueueCount(): Get queue count\n\n";

// 9. Cache management
echo "9. Cache management:\n";
echo "   - invalidateCache(topic): Invalidate specific topic cache\n";
echo "   - invalidateAllCache(): Clear all cache\n";
echo "   - getCacheStats(): Get cache statistics\n\n";

echo "=== Example Complete ===\n";
echo "\nNote: This example shows the API usage.\n";
echo "To query actual route data, you need a running RocketMQ broker.\n";
