<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Rocketmq\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\RouteCache;
use Apache\Rocketmq\V2\QueryRouteResponse;

/**
 * Route cache test class
 */
class RouteCacheTest extends TestCase
{
    protected function setUp(): void
    {
        // Reset singleton before each test
        RouteCache::reset();
    }
    
    /**
     * Test singleton pattern
     */
    public function testSingletonPattern()
    {
        $cache1 = RouteCache::getInstance();
        $cache2 = RouteCache::getInstance();
        
        $this->assertSame($cache1, $cache2);
    }
    
    /**
     * Test cache set and get
     */
    public function testCacheSetAndGet()
    {
        $cache = RouteCache::getInstance();
        $mockRoute = new QueryRouteResponse();
        
        $cache->set('test_topic', $mockRoute);
        $cached = $cache->get('test_topic');
        
        $this->assertSame($mockRoute, $cached);
        $this->assertEquals(1, $cache->size());
    }
    
    /**
     * Test cache expiration
     */
    public function testCacheExpiration()
    {
        $cache = RouteCache::getInstance();
        $cache->setTtl(1); // Set TTL to 1 second
        
        $mockRoute = new QueryRouteResponse();
        $cache->set('expire_test', $mockRoute);
        
        // Should exist immediately
        $this->assertNotNull($cache->get('expire_test'));
        
        // Wait for expiration
        sleep(2);
        
        // Should be expired
        $this->assertNull($cache->get('expire_test'));
    }
    
    /**
     * Test cache invalidation
     */
    public function testCacheInvalidation()
    {
        $cache = RouteCache::getInstance();
        $mockRoute = new QueryRouteResponse();
        
        $cache->set('invalidate_test', $mockRoute);
        $this->assertTrue($cache->has('invalidate_test'));
        
        $cache->invalidate('invalidate_test');
        $this->assertFalse($cache->has('invalidate_test'));
    }
    
    /**
     * Test getOrCreate method
     */
    public function testGetOrCreate()
    {
        $cache = RouteCache::getInstance();
        $callCount = 0;
        
        $loader = function($topic) use (&$callCount) {
            $callCount++;
            return new QueryRouteResponse();
        };
        
        // First call should invoke loader
        $route1 = $cache->getOrCreate('topic1', $loader);
        $this->assertEquals(1, $callCount);
        
        // Second call should use cache
        $route2 = $cache->getOrCreate('topic1', $loader);
        $this->assertEquals(1, $callCount); // Loader not called again
        
        // Different topic should call loader
        $route3 = $cache->getOrCreate('topic2', $loader);
        $this->assertEquals(2, $callCount);
    }
    
    /**
     * Test cache statistics
     */
    public function testCacheStatistics()
    {
        $cache = RouteCache::getInstance();
        $cache->clear();
        $cache->resetStats();
        
        $mockRoute = new QueryRouteResponse();
        
        $cache->set('stat_test', $mockRoute);
        $cache->get('stat_test'); // Hit
        $cache->get('nonexistent'); // Miss
        
        $stats = $cache->getStats();
        
        $this->assertGreaterThanOrEqual(1, $stats['hits']);
        $this->assertGreaterThanOrEqual(1, $stats['misses']);
        $this->assertEquals(1, $stats['size']);
        $this->assertIsFloat($stats['hit_rate']);
    }
    
    /**
     * Test clear cache
     */
    public function testClearCache()
    {
        $cache = RouteCache::getInstance();
        
        $cache->set('test1', new QueryRouteResponse());
        $cache->set('test2', new QueryRouteResponse());
        $this->assertEquals(2, $cache->size());
        
        $cache->clear();
        $this->assertEquals(0, $cache->size());
    }
    
    /**
     * Test TTL parameter validation
     */
    public function testTtlValidation()
    {
        $cache = RouteCache::getInstance();
        
        $this->expectException(\InvalidArgumentException::class);
        $cache->setTtl(0);
    }
    
    /**
     * Test negative TTL validation
     */
    public function testNegativeTtlValidation()
    {
        $cache = RouteCache::getInstance();
        
        $this->expectException(\InvalidArgumentException::class);
        $cache->setTtl(-5);
    }
    
    /**
     * Test enable/disable cache
     */
    public function testEnableDisableCache()
    {
        $cache = RouteCache::getInstance();
        
        $cache->setEnabled(false);
        $this->assertFalse($cache->isEnabled());
        
        $cache->setEnabled(true);
        $this->assertTrue($cache->isEnabled());
    }
    
    /**
     * Test refresh expired cache
     */
    public function testRefreshExpired()
    {
        $cache = RouteCache::getInstance();
        $cache->setTtl(1);
        
        $cache->set('refresh_test_1', new QueryRouteResponse());
        $cache->set('refresh_test_2', new QueryRouteResponse());
        
        // Wait for expiration
        sleep(2);
        
        $refreshCount = 0;
        $refreshLoader = function($topic) use (&$refreshCount) {
            $refreshCount++;
            return new QueryRouteResponse();
        };
        
        $refreshed = $cache->refreshExpired($refreshLoader);
        
        $this->assertGreaterThanOrEqual(2, count($refreshed));
        $this->assertGreaterThanOrEqual(2, $refreshCount);
    }
    
    /**
     * Test performance with cache
     */
    public function testPerformanceWithCache()
    {
        $cache = RouteCache::getInstance();
        $cache->clear();
        $cache->resetStats();
        
        $start = microtime(true);
        for ($i = 0; $i < 100; $i++) {
            $cache->getOrCreate('perf_test', function() {
                return new QueryRouteResponse();
            });
        }
        $end = microtime(true);
        
        $timeMs = ($end - $start) * 1000;
        
        // Should be very fast with cache (< 100ms for 100 iterations)
        $this->assertLessThan(100, $timeMs);
        
        $stats = $cache->getStats();
        $this->assertEquals(99, $stats['hits']); // First call is miss, rest are hits
    }
}
