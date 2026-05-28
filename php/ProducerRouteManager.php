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

use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\QueryRouteRequest;
use Apache\Rocketmq\V2\Resource;

/**
 * ProducerRouteManager manages route querying and caching for Producer.
 *
 * Responsibilities:
 * - Query route data from server for given topics
 * - Cache PublishingLoadBalancer instances per topic
 * - Provide load balancer access with lazy initialization
 *
 * This class is internal to Producer and should not be used directly.
 */
class ProducerRouteManager
{
    /**
     * @var MessagingServiceClient gRPC client for route queries
     */
    private MessagingServiceClient $client;

    /**
     * @var string[] Endpoints for route queries
     */
    private array $endpoints;

    /**
     * @var PublishingLoadBalancer[] Cache of load balancers keyed by topic
     */
    private array $publishingRouteDataCache = [];

    /**
     * Constructor
     *
     * @param MessagingServiceClient $client gRPC client
     * @param string[] $endpoints Server endpoints
     */
    public function __construct(MessagingServiceClient $client, array $endpoints)
    {
        $this->client = $client;
        $this->endpoints = $endpoints;
    }

    /**
     * Get PublishingLoadBalancer for a topic (with lazy initialization).
     *
     * If the load balancer doesn't exist for the topic, it will query route
     * data from the server and create a new load balancer.
     *
     * @param string $topic Topic name
     * @return PublishingLoadBalancer Load balancer for the topic
     * @throws \RuntimeException if route query fails
     */
    public function getPublishingLoadBalancer(string $topic): PublishingLoadBalancer
    {
        if (!isset($this->publishingRouteDataCache[$topic])) {
            $routeData = $this->queryRoute($topic);
            $this->publishingRouteDataCache[$topic] = new PublishingLoadBalancer($routeData);
        }

        return $this->publishingRouteDataCache[$topic];
    }

    /**
     * Query route data from server for a topic.
     *
     * @param string $topic Topic name
     * @return object Route response from server
     * @throws \RuntimeException if query fails
     */
    private function queryRoute(string $topic)
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new QueryRouteRequest();
        $request->setTopic($topicResource);
        $request->setEndpoints($this->parseEndpoints($this->endpoints));

        $metadata = $this->buildMetadata();

        list($response, $status) = $this->client->QueryRoute($request, $metadata, $this->getCallOptions())->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Query route failed: " . $status->details);
        }

        return $response;
    }

    /**
     * Parse endpoints string into protobuf format.
     *
     * @param string[] $endpoints Endpoint strings
     * @return array Parsed endpoints
     */
    private function parseEndpoints(array $endpoints): array
    {
        $parsed = [];
        foreach ($endpoints as $endpoint) {
            $parts = explode(':', $endpoint, 2);
            $host = $parts[0];
            $port = isset($parts[1]) ? (int)$parts[1] : 8080;

            $address = new \Apache\Rocketmq\V2\Address();
            $address->setHost($host);
            $address->setPort($port);

            $parsed[] = $address;
        }
        return $parsed;
    }

    /**
     * Build metadata for gRPC calls.
     *
     * @return array Metadata array
     */
    private function buildMetadata(): array
    {
        return [
            'authorization' => ['Bearer ' . $this->getAccessToken()],
        ];
    }

    /**
     * Get call options for gRPC calls.
     *
     * @return array Call options
     */
    private function getCallOptions(): array
    {
        return [];
    }

    /**
     * Get access token (placeholder - should be implemented based on auth mechanism).
     *
     * @return string Access token
     */
    private function getAccessToken(): string
    {
        // TODO: Implement proper authentication
        return '';
    }

    /**
     * Clear all cached route data.
     *
     * Useful for testing or forcing route refresh.
     */
    public function clearCache(): void
    {
        $this->publishingRouteDataCache = [];
    }

    /**
     * Get count of cached routes.
     *
     * @return int Number of cached topic routes
     */
    public function getCacheCount(): int
    {
        return count($this->publishingRouteDataCache);
    }
}
