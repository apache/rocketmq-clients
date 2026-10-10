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

use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\Resource;

/**
 * PublishingRouteManager — route cache, query, and endpoint isolation for Producer.
 *
 * Extracted from Producer. Owns:
 * - Route cache (PublishingLoadBalancer per topic)
 * - QueryRoute gRPC calls
 * - Endpoint isolation (failed broker tracking)
 */
class PublishingRouteManager
{
    private array $routeCache = [];
    private array $isolatedEndpoints = [];
    private readonly Logger $logger;

    public function __construct(
        private readonly MessagingServiceClient $client,
        private readonly string $endpoints,
        private readonly ClientTraitProvider $traitProvider,
    ) {
        $this->logger = Logger::getInstance('PublishingRouteManager');
    }

    // ==================== Route Cache ====================

    public function getRouteCache(): array
    {
        return $this->routeCache;
    }

    /**
     * Get or create a PublishingLoadBalancer for a topic.
     */
    public function getPublishingLoadBalancer(string $topic): PublishingLoadBalancer
    {
        if (!isset($this->routeCache[$topic])) {
            $routeData = $this->queryRoute($topic);
            $this->routeCache[$topic] = new PublishingLoadBalancer($routeData);
        }
        return $this->routeCache[$topic];
    }

    /**
     * Refresh route cache for all known topics.
     */
    public function refreshRouteCache(): void
    {
        foreach (array_keys($this->routeCache) as $topic) {
            try {
                $routeData = $this->queryRoute($topic);
                $existing = $this->routeCache[$topic] ?? null;
                $this->routeCache[$topic] = $existing !== null
                    ? $existing->update($routeData)
                    : new PublishingLoadBalancer($routeData);
                $this->logger->debug("Route refreshed for topic={$topic}");
            } catch (\Exception $e) {
                $this->logger->error("Failed to refresh route for topic={$topic}", ['exception' => $e]);
            }
        }
    }

    /**
     * Pre-populate route cache for initial topics during start().
     */
    public function warmUp(array $topics): void
    {
        foreach ($topics as $topic) {
            $this->getPublishingLoadBalancer($topic);
        }
    }

    // ==================== Endpoint Collection ====================

    /**
     * Get all unique route endpoints across all cached topics.
     *
     * @return Endpoints[]
     */
    public function getTotalRouteEndpoints(): array
    {
        $endpointMap = [];
        foreach ($this->routeCache as $loadBalancer) {
            foreach ($loadBalancer->getMessageQueues() as $messageQueue) {
                $ep = $this->extractMessageQueueEndpoint($messageQueue);
                if ($ep !== null) {
                    $endpointMap[$this->endpointsKey($ep)] = $ep;
                }
            }
        }
        return array_values($endpointMap);
    }

    // ==================== Endpoint Isolation ====================

    public function isolateEndpoints(Endpoints $endpoints): void
    {
        foreach ($endpoints->getAddresses() as $address) {
            $key = $address->getHost() . ':' . $address->getPort();
            $this->isolatedEndpoints[$key] = $endpoints;
        }
    }

    public function clearIsolatedEndpoints(): void
    {
        $this->isolatedEndpoints = [];
    }

    public function getIsolatedEndpoints(): array
    {
        return $this->isolatedEndpoints;
    }

    /**
     * Get broker names that are currently isolated.
     */
    public function getIsolatedBrokerNames(): array
    {
        $brokerNames = [];
        foreach ($this->routeCache as $loadBalancer) {
            foreach ($loadBalancer->getMessageQueues() as $messageQueue) {
                $ep = $this->extractMessageQueueEndpoint($messageQueue);
                if ($ep !== null) {
                    $key = $this->endpointsKey($ep);
                    if (isset($this->isolatedEndpoints[$key])) {
                        $brokerNames[] = $messageQueue->getBroker()->getName();
                    }
                }
            }
        }
        return array_unique($brokerNames);
    }

    // ==================== Helpers ====================

    public function endpointsKey(Endpoints $endpoints): string
    {
        $addresses = $endpoints->getAddresses();
        if (!empty($addresses) && $addresses[0] !== null) {
            return $addresses[0]->getHost() . ':' . $addresses[0]->getPort();
        }
        return spl_object_hash($endpoints);
    }

    public static function extractMessageQueueEndpoint($messageQueue): ?Endpoints
    {
        $broker = $messageQueue->getBroker();
        if ($broker && $broker->hasEndpoints()) {
            return $broker->getEndpoints();
        }
        return null;
    }

    // ==================== Private ====================

    private function queryRoute(string $topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->traitProvider->parseEndpoints($this->endpoints));

        $timeoutMs = (int)($this->traitProvider->getOperationTimeout('QUERY_ROUTE') / 1000);
        $metadata = $this->traitProvider->buildMetadata($timeoutMs);
        $callOptions = ['timeout' => $this->traitProvider->getOperationTimeout('QUERY_ROUTE')];

        list($response, $status) = $this->client->QueryRoute($request, $metadata, $callOptions)->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }
        return $response;
    }
}
