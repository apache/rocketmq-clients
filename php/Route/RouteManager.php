<?php
declare(strict_types=1);
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
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\QueryRouteResponse;
use Apache\Rocketmq\V2\Resource;

/**
 * RouteManager manages topic route queries and caching
 * 
 * References Java route management implementation:
 * - Cache with TTL and max size limit (prevents unbounded memory growth)
 * - LRU eviction when cache exceeds max entries
 */
class RouteManager
{
    /**
     * Maximum number of cached topics (prevents unbounded memory growth)
     */
    private const MAX_CACHE_SIZE = 1000;

    private static ?RouteManager $instance = null;
    private ?ClientConfiguration $config = null;
    /** @var array<string, TopicRouteData> Route cache [topic => TopicRouteData] */
    private array $routeCache = [];
    /** @var array<string, int> Cache timestamps [topic => timestamp] */
    private array $cacheTimestamps = [];
    private int $cacheTtl = 30;
    private bool $enableCache = true;

    private function __construct()
    {
    }

    public static function getInstance(): RouteManager
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function setConfig(ClientConfiguration $config): void
    {
        $this->config = $config;
    }

    public function setCacheTtl(int $ttl): void
    {
        $this->cacheTtl = $ttl;
    }

    public function setEnableCache(bool $enable): void
    {
        $this->enableCache = $enable;
    }

    /**
     * Get route data for topic
     * 
     * @throws ClientException If query fails
     * @throws NotFoundException If route not found
     */
    public function getRouteData(string $topic, bool $forceRefresh = false): TopicRouteData
    {
        if (!$forceRefresh && $this->enableCache) {
            $cached = $this->getCachedRouteData($topic);
            if ($cached !== null) {
                return $cached;
            }
        }

        $routeData = $this->queryRouteFromBroker($topic);

        if ($this->enableCache) {
            $this->putCache($topic, $routeData);
        }

        return $routeData;
    }

    /**
     * Put route data into cache with LRU eviction
     */
    private function putCache(string $topic, TopicRouteData $routeData): void
    {
        // Evict oldest entries if cache exceeds max size
        while (count($this->routeCache) >= self::MAX_CACHE_SIZE && !isset($this->routeCache[$topic])) {
            $oldestTopic = null;
            $oldestTime = PHP_INT_MAX;
            foreach ($this->cacheTimestamps as $t => $ts) {
                if ($ts < $oldestTime) {
                    $oldestTime = $ts;
                    $oldestTopic = $t;
                }
            }
            if ($oldestTopic !== null) {
                Logger::debug("Route cache evicting oldest entry, topic={$oldestTopic}, cacheSize=" . count($this->routeCache));
                unset($this->routeCache[$oldestTopic]);
                unset($this->cacheTimestamps[$oldestTopic]);
            } else {
                break;
            }
        }

        $this->routeCache[$topic] = $routeData;
        $this->cacheTimestamps[$topic] = time();
    }

    private function getCachedRouteData(string $topic): ?TopicRouteData
    {
        if (!isset($this->routeCache[$topic]) || !isset($this->cacheTimestamps[$topic])) {
            return null;
        }

        $elapsed = time() - $this->cacheTimestamps[$topic];
        if ($elapsed >= $this->cacheTtl) {
            unset($this->routeCache[$topic]);
            unset($this->cacheTimestamps[$topic]);
            return null;
        }

        return $this->routeCache[$topic];
    }

    /**
     * @throws ClientException If query fails
     * @throws NotFoundException If route not found
     */
    private function queryRouteFromBroker(string $topic): TopicRouteData
    {
        if ($this->config === null) {
            throw new ClientException("Client configuration not set");
        }

        $request = new QueryRouteRequest();
        $topicResource = new Resource();
        $topicResource->setName($topic);
        $request->setInfo($topicResource);
        $request->setEndpoints($this->config->getEndpoints()->toProtobuf());

        $pool = ConnectionPool::getInstance();
        $pool->setConfigFromClientConfiguration($this->config);
        $client = $pool->getConnection($this->config);

        try {
            $call = $client->QueryRoute($request);
            $response = $call->wait();

            if (!($response instanceof QueryRouteResponse)) {
                throw new ClientException("Invalid response type");
            }

            $messageQueues = $response->getMessageQueuesList();

            if (empty($messageQueues)) {
                throw new NotFoundException("No route found for topic: {$topic}");
            }

            Logger::debug("Fetched route data from broker, topic={$topic}, queueCount=" . count($messageQueues));

            return new TopicRouteData($messageQueues);
        } catch (\Throwable $e) {
            if ($e instanceof ClientException || $e instanceof NotFoundException) {
                throw $e;
            }
            throw new ClientException("Failed to query route for topic {$topic}: " . $e->getMessage(), 0, $e);
        }
    }

    public function invalidateCache(string $topic): void
    {
        unset($this->routeCache[$topic]);
        unset($this->cacheTimestamps[$topic]);
    }

    public function invalidateAllCache(): void
    {
        $this->routeCache = [];
        $this->cacheTimestamps = [];
    }

    /**
     * @return array{cached_topics: int, cache_ttl: int, cache_enabled: bool, max_cache_size: int, topics: string[]}
     */
    public function getCacheStats(): array
    {
        return [
            'cached_topics' => count($this->routeCache),
            'cache_ttl' => $this->cacheTtl,
            'cache_enabled' => $this->enableCache,
            'max_cache_size' => self::MAX_CACHE_SIZE,
            'topics' => array_keys($this->routeCache),
        ];
    }

    public function getCachedRoute(string $topic): ?TopicRouteData
    {
        return $this->routeCache[$topic] ?? null;
    }
}
