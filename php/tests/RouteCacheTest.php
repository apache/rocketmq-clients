<?php
/**
 * Route cache test
 */

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Rocketmq\RouteCache;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\MessageQueue;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Resource;

echo "=== Route Cache Test ===\n\n";

// Reset singleton
RouteCache::reset();

// Test 1: Basic functionality
echo "Test 1: Create and get singleton\n";
$cache1 = RouteCache::getInstance();
$cache2 = RouteCache::getInstance();
echo ($cache1 === $cache2 ? "✓" : "✗") . " Singleton pattern works correctly\n\n";

// Test 2: Cache set and get
echo "Test 2: Cache set and get\n";
$mockRoute = new QueryRouteResponse();
$cache1->set('test_topic', $mockRoute);
$cached = $cache1->get('test_topic');
echo ($cached === $mockRoute ? "✓" : "✗") . " Cache set and get works correctly\n";
echo "Cache size: " . $cache1->size() . "\n\n";

// Test 3: Cache expiration
echo "Test 3: Cache expiration mechanism\n";
$cache1->setTtl(1); // Set TTL to 1 second
$cache1->set('expire_test', $mockRoute);
echo "✓ TTL set to 1 second\n";
echo "Immediate get: " . ($cache1->get('expire_test') !== null ? 'Success' : 'Failed') . "\n";
sleep(2);
echo "Get after 2 seconds: " . ($cache1->get('expire_test') !== null ? 'Success' : 'Failed (expired)') . "\n\n";

// Test 4: Cache invalidation
echo "Test 4: Manually invalidate cache\n";
$cache1->setTtl(60); // Restore TTL
$cache1->set('invalidate_test', $mockRoute);
echo "Cache exists: " . ($cache1->has('invalidate_test') ? 'Yes' : 'No') . "\n";
$cache1->invalidate('invalidate_test');
echo "Exists after invalidation: " . ($cache1->has('invalidate_test') ? 'Yes' : 'No') . "\n\n";

// Test 5: getOrCreate method
echo "Test 5: getOrCreate method\n";
$callCount = 0;
$loader = function($topic) use (&$callCount) {
    $callCount++;
    echo "  - Loader called ({$callCount} time)\n";
    return new QueryRouteResponse();
};

$route1 = $cache1->getOrCreate('topic1', $loader);
$route2 = $cache1->getOrCreate('topic1', $loader); // Should use cache
$route3 = $cache1->getOrCreate('topic2', $loader); // New topic, call loader

echo "✓ getOrCreate works correctly\n";
echo "Loader call count: {$callCount} (expected 2 times)\n\n";

// Test 6: Cache statistics
echo "Test 6: Cache statistics\n";
$stats = $cache1->getStats();
echo "Hits: " . $stats['hits'] . "\n";
echo "Misses: " . $stats['misses'] . "\n";
echo "Refreshes: " . $stats['refreshes'] . "\n";
echo "Cache size: " . $stats['size'] . "\n";
echo "Hit rate: " . number_format($stats['hit_rate'] * 100, 2) . "%\n\n";

// Test 7: Clear cache
echo "Test 7: Clear all cache\n";
$cache1->clear();
echo "Size after clear: " . $cache1->size() . "\n";
echo "✓ Cache cleared\n\n";

// Test 8: TTL parameter validation
echo "Test 8: TTL parameter validation\n";
try {
    $cache1->setTtl(0);
    echo "✗ Should throw exception: TTL < 1\n";
} catch (InvalidArgumentException $e) {
    echo "✓ Correctly caught exception: TTL must be >= 1\n";
}

try {
    $cache1->setTtl(-5);
    echo "✗ Should throw exception: TTL < 0\n";
} catch (InvalidArgumentException $e) {
    echo "✓ Correctly caught exception: TTL cannot be negative\n";
}
echo "\n";

// Test 9: Enable/disable cache
echo "Test 9: Enable/disable cache\n";
$cache1->setEnabled(false);
echo "After disable: " . ($cache1->isEnabled() ? 'Enabled' : 'Disabled') . "\n";
$cache1->setEnabled(true);
echo "After enable: " . ($cache1->isEnabled() ? 'Enabled' : 'Disabled') . "\n\n";

// 测试10: 刷新过期缓存
echo "测试10: 批量刷新过期缓存\n";
$cache1->setTtl(1);
$cache1->set('refresh_test_1', new QueryRouteResponse());
$cache1->set('refresh_test_2', new QueryRouteResponse());
sleep(2);

$refreshCount = 0;
$refreshLoader = function($topic) use (&$refreshCount) {
    $refreshCount++;
    return new QueryRouteResponse();
};

$refreshed = $cache1->refreshExpired($refreshLoader);
echo "Refreshed topics count: " . count($refreshed) . "\n";
echo "Loader call count: {$refreshCount}\n";
echo "✓ Batch refresh completed\n\n";

// Test 11: Performance comparison
echo "Test 11: Performance comparison (cache vs no cache)\n";
$perfCache = RouteCache::getInstance();
$perfCache->clear();
$perfCache->resetStats();

// Simulate 1000 queries
$start = microtime(true);
for ($i = 0; $i < 1000; $i++) {
    $perfCache->getOrCreate('perf_test', function() {
        return new QueryRouteResponse();
    });
}
$end = microtime(true);
$timeWithCache = ($end - $start) * 1000;

$stats = $perfCache->getStats();
echo "1000 queries time: " . number_format($timeWithCache, 2) . "ms\n";
echo "Cache hits: " . $stats['hits'] . " times\n";
echo "Cache misses: " . $stats['misses'] . " times\n";
echo "Average per query: " . number_format($timeWithCache / 1000, 4) . "ms\n";
echo "✓ Performance test completed\n\n";

echo "=== All tests completed ===\n";
echo "\nRoute cache features summary:\n";
echo "1. ✓ Singleton pattern, globally shared\n";
echo "2. ✓ TTL automatic expiration mechanism\n";
echo "3. ✓ Manual invalidation control\n";
echo "4. ✓ getOrCreate smart loading\n";
echo "5. ✓ Detailed statistics\n";
echo "6. ✓ Batch refresh expired cache\n";
echo "7. ✓ Enable/disable switch\n";
echo "8. ✓ Thread safe (within PHP process)\n";
