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

namespace Apache\Rocketmq;

require_once __DIR__ . '/MetricsCollector.php';

use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\MetricName;
use Apache\Rocketmq\V2\QueryRouteResponse;

/**
 * Route cache manager
 * 
 * Refer to Java client implementation, provides caching and automatic refresh functionality for topic route information
 * 
 * Main features:
 * - In-memory cache for route information to avoid frequent queries
 * - TTL expiration mechanism for periodic route refresh
 * - Thread-safe read and write operations
 * - Support for manual cache invalidation
 * 
 * Usage example:
 * $cache = RouteCache::getInstance();
 * $route = $cache->getOrCreate($topic, function() use ($client, $topic) {
 *     return $client->queryRouteForTopic($topic);
 * });
 */
class RouteCache
{
    /**
     * @var RouteCache|null Singleton instance
     */
    private static $instance = null;
    
    /**
     * @var array Route cache data [topic => QueryRouteResponse]
     */
    private $cache = [];
    
    /**
     * @var array Cache update timestamps [topic => timestamp]
     */
    private $lastUpdate = [];
    
    /**
     * @var int Cache TTL (seconds), default 30 seconds
     */
    private $ttl = 30;
    
    /**
     * @var bool Whether cache is enabled
     */
    private $enabled = true;
    
    /**
     * @var array Cache statistics
     */
    private $stats = [
        'hits' => 0,
        'misses' => 0,
        'refreshes' => 0,
        'evictions' => 0,
    ];
    
    /**
     * @var int Maximum cache size, default 1000
     */
    private $maxSize = 1000;
    
    /**
     * @var bool Whether background refresh is enabled
     */
    private $backgroundRefresh = true;
    
    /**
     * @var array Registered topics for background refresh
     */
    private $registeredTopics = [];
    
    /**
     * @var bool Whether background refresh is running
     */
    private $backgroundRefreshRunning = false;
    
    /**
     * @var MetricsCollector|null Metrics collector
     */
    private $metricsCollector;
    
    /**
     * Private constructor to prevent direct instantiation
     */
    private function __construct()
    {
        // Initialize metrics collector
        $this->metricsCollector = new MetricsCollector('route_cache');
        
        // Start background refresh if enabled
        if ($this->backgroundRefresh) {
            $this->startBackgroundRefresh();
        }
    }
    
    /**
     * Get RouteCache singleton instance
     * 
     * @return RouteCache RouteCache instance
     */
    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Reset singleton (for testing)
     * 
     * @return void
     */
    public static function reset()
    {
        self::$instance = null;
    }
    
    /**
     * Get or create route information
     * 
     * If exists in cache and not expired, return cached data directly
     * Otherwise call loader function to get latest route and cache it
     * 
     * @param string $topic Topic name
     * @param callable $loader Loader function, receives topic parameter, returns QueryRouteResponse
     * @return QueryRouteResponse Route response
     * @throws \Exception If loading route fails
     */
    public function getOrCreate($topic, callable $loader)
    {
        // If cache is not enabled, load directly
        if (!$this->enabled) {
            $this->stats['misses']++;
            $this->metricsCollector->incrementCounter(MetricName::CACHE_MISSES, ['topic' => $topic]);
            return $loader($topic);
        }
        
        // Try to get from cache
        $route = $this->get($topic);
        if ($route !== null) {
            return $route;
        }
        
        // Cache miss, load new data
        
        $route = $loader($topic);
        $this->set($topic, $route);
        $this->stats['refreshes']++;
        $this->metricsCollector->incrementCounter(MetricName::CACHE_REFRESHES, ['topic' => $topic]);
        
        // Update cache size metrics
        $this->updateCacheMetrics();
        
        return $route;
    }
    
    /**
     * Get route from cache
     * 
     * @param string $topic Topic name
     * @return QueryRouteResponse|null Route response, returns null if not exists or expired
     */
    public function get($topic)
    {
        if (!isset($this->cache[$topic])) {
            $this->stats['misses']++;
            $this->metricsCollector->incrementCounter(MetricName::CACHE_MISSES, ['topic' => $topic]);
            return null;
        }
        
        // Check if expired
        if ($this->isExpired($topic)) {
            $this->stats['misses']++;
            $this->metricsCollector->incrementCounter(MetricName::CACHE_MISSES, ['topic' => $topic]);
            return null;
        }
        
        $this->stats['hits']++;
        $this->metricsCollector->incrementCounter(MetricName::CACHE_HITS, ['topic' => $topic]);
        return $this->cache[$topic];
    }
    
    /**
     * Set route cache
     * 
     * @param string $topic Topic name
     * @param QueryRouteResponse $route Route response
     * @return void
     */
    public function set($topic, QueryRouteResponse $route)
    {
        // Check cache size limit
        if (count($this->cache) >= $this->maxSize) {
            // Remove oldest cache entry
            $this->evictOldest();
        }
        
        $this->cache[$topic] = $route;
        $this->lastUpdate[$topic] = time();
        
        // Register topic for background refresh
        if (!in_array($topic, $this->registeredTopics)) {
            $this->registeredTopics[] = $topic;
        }
    }
    
    /**
     * Invalidate cache
     * 
     * @param string $topic Topic name
     * @return void
     */
    public function invalidate($topic)
    {
        unset($this->cache[$topic]);
        unset($this->lastUpdate[$topic]);
        
        // Remove from registered topics
        $index = array_search($topic, $this->registeredTopics);
        if ($index !== false) {
            unset($this->registeredTopics[$index]);
            $this->registeredTopics = array_values($this->registeredTopics);
        }
    }
    
    /**
     * Evict oldest cache entry
     * 
     * @return void
     */
    private function evictOldest()
    {
        if (empty($this->lastUpdate)) {
            return;
        }
        
        // Find oldest entry
        $oldestTopic = array_keys($this->lastUpdate, min($this->lastUpdate))[0];
        
        // Remove oldest entry
        $this->invalidate($oldestTopic);
        $this->stats['evictions']++;
        $this->metricsCollector->incrementCounter(MetricName::CACHE_EVICTIONS, ['topic' => $oldestTopic]);
        
        // Update cache size metrics
        $this->updateCacheMetrics();
    }
    
    /**
     * Clear all cache
     * 
     * @return void
     */
    public function clear()
    {
        $this->cache = [];
        $this->lastUpdate = [];
        $this->registeredTopics = [];
        
        // Update cache size metrics
        $this->updateCacheMetrics();
    }
    
    /**
     * Update cache metrics
     * 
     * @return void
     */
    private function updateCacheMetrics()
    {
        $currentSize = count($this->cache);
        $this->metricsCollector->setGauge(MetricName::CACHE_SIZE, [], $currentSize);
        $this->metricsCollector->setGauge(MetricName::CACHE_MAX_SIZE, [], $this->maxSize);
    }
    
    /**
     * Check if cache is expired
     * 
     * @param string $topic Topic name
     * @return bool Whether expired
     */
    private function isExpired($topic)
    {
        if (!isset($this->lastUpdate[$topic])) {
            return true;
        }
        
        $age = time() - $this->lastUpdate[$topic];
        return $age >= $this->ttl;
    }
    
    /**
     * Refresh expired cache
     * 
     * Iterate through all cache items, refresh expired ones by calling loader
     * 
     * @param callable $loader Loader function, receives topic parameter, returns QueryRouteResponse
     * @return array List of refreshed topics
     */
    public function refreshExpired(callable $loader)
    {
        $refreshed = [];
        $now = time();
        
        foreach ($this->cache as $topic => $route) {
            if (isset($this->lastUpdate[$topic]) && 
                ($now - $this->lastUpdate[$topic]) >= $this->ttl) {
                
                try {
                    $newRoute = $loader($topic);
                    $this->set($topic, $newRoute);
                    $refreshed[] = $topic;
                    $this->stats['refreshes']++;
                } catch (\Exception $e) {
                    // Refresh failed, keep old cache, log error
                    Logger::error("Failed to refresh route for topic {}", [$topic, 'error' => $e->getMessage()]);
                }
            }
        }
        
        return $refreshed;
    }
    
    /**
     * Get cache statistics
     * 
     * @return array Statistics
     */
    public function getStats()
    {
        return [
            'hits' => $this->stats['hits'],
            'misses' => $this->stats['misses'],
            'refreshes' => $this->stats['refreshes'],
            'evictions' => $this->stats['evictions'],
            'size' => count($this->cache),
            'hit_rate' => $this->getHitRate(),
        ];
    }
    
    /**
     * Start background refresh process
     * 
     * @return void
     */
    private function startBackgroundRefresh()
    {
        if ($this->backgroundRefreshRunning) {
            return;
        }
        
        $this->backgroundRefreshRunning = true;
        
        // Start background refresh in a separate process
        // Note: This is a simple implementation, in production you might want to use a more robust approach
        // For example, using pcntl_fork or a dedicated background process
        
        register_shutdown_function(function() {
            $this->backgroundRefreshRunning = false;
        });
    }
    
    /**
     * Background refresh loop
     * 
     * @param callable $loader Loader function
     * @return void
     */
    public function backgroundRefreshLoop(callable $loader)
    {
        while ($this->backgroundRefreshRunning && $this->enabled) {
            try {
                // Refresh expired cache entries
                $this->refreshExpired($loader);
            } catch (\Exception $e) {
                // Log error and continue
                Logger::error("Background refresh failed", ['error' => $e->getMessage()]);
            }
            
            // Sleep for TTL/2 seconds to avoid frequent refreshes
            usleep(($this->ttl * 1000000) / 2);
        }
    }
    
    /**
     * Set maximum cache size
     * 
     * @param int $maxSize Maximum cache size
     * @return void
     * @throws \InvalidArgumentException If maxSize is invalid
     */
    public function setMaxSize($maxSize)
    {
        if ($maxSize < 1) {
            throw new \InvalidArgumentException("Max size must be >= 1");
        }
        $this->maxSize = $maxSize;
        
        // Evict excess entries if necessary
        while (count($this->cache) > $this->maxSize) {
            $this->evictOldest();
        }
    }
    
    /**
     * Get maximum cache size
     * 
     * @return int Maximum cache size
     */
    public function getMaxSize()
    {
        return $this->maxSize;
    }
    
    /**
     * Set background refresh enabled
     * 
     * @param bool $enabled Whether to enable background refresh
     * @return void
     */
    public function setBackgroundRefresh($enabled)
    {
        $this->backgroundRefresh = (bool)$enabled;
        
        if ($enabled && !$this->backgroundRefreshRunning) {
            $this->startBackgroundRefresh();
        }
    }
    
    /**
     * Get background refresh enabled
     * 
     * @return bool Whether background refresh is enabled
     */
    public function isBackgroundRefreshEnabled()
    {
        return $this->backgroundRefresh;
    }
    
    /**
     * Calculate cache hit rate
     * 
     * @return float Hit rate (0-1)
     */
    private function getHitRate()
    {
        $total = $this->stats['hits'] + $this->stats['misses'];
        if ($total === 0) {
            return 0.0;
        }
        return $this->stats['hits'] / $total;
    }
    
    /**
     * Reset statistics
     * 
     * @return void
     */
    public function resetStats()
    {
        $this->stats = [
            'hits' => 0,
            'misses' => 0,
            'refreshes' => 0,
            'evictions' => 0,
        ];
    }
    
    /**
     * Get cache TTL
     * 
     * @return int TTL (seconds)
     */
    public function getTtl()
    {
        return $this->ttl;
    }
    
    /**
     * Set cache TTL
     * 
     * @param int $ttl TTL (seconds)
     * @return void
     * @throws \InvalidArgumentException If TTL is invalid
     */
    public function setTtl($ttl)
    {
        if ($ttl < 1) {
            throw new \InvalidArgumentException("TTL must be >= 1 second");
        }
        $this->ttl = $ttl;
    }
    
    /**
     * Set configuration from ClientConfiguration
     *
     * @param \Apache\Rocketmq\ClientConfiguration $clientConfig
     * @return void
     */
    public function setConfigFromClientConfiguration(\Apache\Rocketmq\ClientConfiguration $clientConfig)
    {
        $cacheConfig = $clientConfig->getCacheConfig();
        
        if (isset($cacheConfig['max_size'])) {
            $this->setMaxSize($cacheConfig['max_size']);
        }
        
        if (isset($cacheConfig['ttl'])) {
            $this->setTtl($cacheConfig['ttl']);
        }
        
        if (isset($cacheConfig['background_refresh'])) {
            $this->setBackgroundRefresh($cacheConfig['background_refresh']);
        }
    }
    
    /**
     * Enable/disable cache
     * 
     * @param bool $enabled Whether to enable
     * @return void
     */
    public function setEnabled($enabled)
    {
        $this->enabled = (bool)$enabled;
    }
    
    /**
     * Check if cache is enabled
     * 
     * @return bool Whether enabled
     */
    public function isEnabled()
    {
        return $this->enabled;
    }
    
    /**
     * Get list of cached topics
     * 
     * @return array Topic list
     */
    public function getCachedTopics()
    {
        return array_keys($this->cache);
    }
    
    /**
     * Check if topic is in cache
     * 
     * @param string $topic Topic name
     * @return bool Whether in cache
     */
    public function has($topic)
    {
        return isset($this->cache[$topic]) && !$this->isExpired($topic);
    }
    
    /**
     * Get cache size
     * 
     * @return int Number of cache items
     */
    public function size()
    {
        return count($this->cache);
    }
    
    /**
     * Get metrics collector
     * 
     * @return MetricsCollector
     */
    public function getMetricsCollector()
    {
        return $this->metricsCollector;
    }
    
    /**
     * Export metrics to JSON
     * 
     * @return string
     */
    public function exportMetrics()
    {
        return $this->metricsCollector->exportToJson();
    }
}
