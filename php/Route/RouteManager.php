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

namespace Apache\Rocketmq\Route;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Connection\ConnectionPool;
use Apache\Rocketmq\Exception\ClientException;
use Apache\Rocketmq\Exception\NotFoundException;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\Resource;

/**
 * RouteManager manages topic route queries and caching
 * 
 * References Java route management implementation
 * 
 * Key features:
 * - Query route data from broker
 * - Cache route data with TTL
 * - Automatic route refresh
 * - Thread-safe operations
 */
class RouteManager
{
    /**
     * @var RouteManager Singleton instance
     */
    private static $instance = null;
    
    /**
     * @var ClientConfiguration Client configuration
     */
    private $config;
    
    /**
     * @var array Route cache [topic => TopicRouteData]
     */
    private $routeCache = [];
    
    /**
     * @var array Cache timestamps [topic => timestamp]
     */
    private $cacheTimestamps = [];
    
    /**
     * @var int Cache TTL in seconds (default 30s)
     */
    private $cacheTtl = 30;
    
    /**
     * @var bool Whether to enable cache
     */
    private $enableCache = true;
    
    /**
     * Private constructor for singleton
     */
    private function __construct()
    {
    }
    
    /**
     * Get singleton instance
     * 
     * @return RouteManager
     */
    public static function getInstance(): RouteManager
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Set client configuration
     * 
     * @param ClientConfiguration $config Client configuration
     * @return void
     */
    public function setConfig(ClientConfiguration $config): void
    {
        $this->config = $config;
    }
    
    /**
     * Set cache TTL
     * 
     * @param int $ttl Cache TTL in seconds
     * @return void
     */
    public function setCacheTtl(int $ttl): void
    {
        $this->cacheTtl = $ttl;
    }
    
    /**
     * Enable or disable cache
     * 
     * @param bool $enable Whether to enable cache
     * @return void
     */
    public function setEnableCache(bool $enable): void
    {
        $this->enableCache = $enable;
    }
    
    /**
     * Get route data for topic
     * 
     * Checks cache first, queries from broker if cache is expired or not found
     * 
     * @param string $topic Topic name
     * @param bool $forceRefresh Force refresh from broker
     * @return TopicRouteData Route data
     * @throws ClientException If query fails
     * @throws NotFoundException If route not found
     */
    public function getRouteData(string $topic, bool $forceRefresh = false): TopicRouteData
    {
        // Check cache
        if (!$forceRefresh && $this->enableCache) {
            $cached = $this->getCachedRouteData($topic);
            if ($cached !== null) {
                return $cached;
            }
        }
        
        // Query from broker
        $routeData = $this->queryRouteFromBroker($topic);
        
        // Update cache
        if ($this->enableCache) {
            $this->routeCache[$topic] = $routeData;
            $this->cacheTimestamps[$topic] = time();
        }
        
        return $routeData;
    }
    
    /**
     * Get cached route data if not expired
     * 
     * @param string $topic Topic name
     * @return TopicRouteData|null Cached route data or null
     */
    private function getCachedRouteData(string $topic): ?TopicRouteData
    {
        if (!isset($this->routeCache[$topic])) {
            return null;
        }
        
        if (!isset($this->cacheTimestamps[$topic])) {
            return null;
        }
        
        // Check TTL
        $elapsed = time() - $this->cacheTimestamps[$topic];
        if ($elapsed >= $this->cacheTtl) {
            // Cache expired
            unset($this->routeCache[$topic]);
            unset($this->cacheTimestamps[$topic]);
            return null;
        }
        
        return $this->routeCache[$topic];
    }
    
    /**
     * Query route data from broker
     * 
     * @param string $topic Topic name
     * @return TopicRouteData Route data
     * @throws ClientException If query fails
     * @throws NotFoundException If route not found
     */
    private function queryRouteFromBroker(string $topic): TopicRouteData
    {
        if ($this->config === null) {
            throw new ClientException("Client configuration not set");
        }
        
        // Build request
        $request = new QueryRouteRequest();
        $request->setInfo(Resource::create()->setName($topic));
        $request->setEndpoints($this->config->getEndpoints()->toProtobuf());
        
        // Get connection
        $pool = ConnectionPool::getInstance();
        $pool->setConfigFromClientConfiguration($this->config);
        $client = $pool->getConnection($this->config);
        
        try {
            // Execute query
            $call = $client->QueryRoute($request);
            $response = $call->wait();
            
            if (!($response instanceof QueryRouteResponse)) {
                throw new ClientException("Invalid response type");
            }
            
            $messageQueues = $response->getMessageQueuesList();
            
            if (empty($messageQueues)) {
                throw new NotFoundException("No route found for topic: {$topic}");
            }
            
            return new TopicRouteData($messageQueues);
            
        } catch (\Exception $e) {
            throw new ClientException("Failed to query route for topic {$topic}: " . $e->getMessage(), 0, $e);
        }
    }
    
    /**
     * Invalidate cache for a topic
     * 
     * @param string $topic Topic name
     * @return void
     */
    public function invalidateCache(string $topic): void
    {
        unset($this->routeCache[$topic]);
        unset($this->cacheTimestamps[$topic]);
    }
    
    /**
     * Invalidate all cache
     * 
     * @return void
     */
    public function invalidateAllCache(): void
    {
        $this->routeCache = [];
        $this->cacheTimestamps = [];
    }
    
    /**
     * Get cache statistics
     * 
     * @return array Cache statistics
     */
    public function getCacheStats(): array
    {
        return [
            'cached_topics' => count($this->routeCache),
            'cache_ttl' => $this->cacheTtl,
            'cache_enabled' => $this->enableCache,
            'topics' => array_keys($this->routeCache),
        ];
    }
    
    /**
     * Get cached route data without querying broker
     * 
     * @param string $topic Topic name
     * @return TopicRouteData|null Cached route data or null
     */
    public function getCachedRoute(string $topic): ?TopicRouteData
    {
        return $this->routeCache[$topic] ?? null;
    }
}
