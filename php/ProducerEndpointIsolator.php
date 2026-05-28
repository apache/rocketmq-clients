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

/**
 * ProducerEndpointIsolator manages endpoint isolation for fault tolerance.
 *
 * Responsibilities:
 * - Track isolated (unhealthy) broker endpoints
 * - Provide filtered broker lists excluding isolated endpoints
 * - Automatic isolation timeout management
 *
 * This class is internal to Producer and should not be used directly.
 */
class ProducerEndpointIsolator
{
    /**
     * @var array Isolated endpoints keyed by broker name with isolation timestamp
     */
    private array $isolatedEndpoints = [];

    /**
     * @var int Isolation timeout in seconds (default: 30s)
     */
    private int $isolationTimeoutSeconds = 30;

    /**
     * Isolate a list of endpoints due to failures.
     *
     * @param array $endpoints Array of endpoint strings to isolate
     */
    public function isolateEndpoints(array $endpoints): void
    {
        $now = time();
        foreach ($endpoints as $endpoint) {
            $this->isolatedEndpoints[$endpoint] = $now;
        }
        
        // Clean up expired isolations
        $this->cleanupExpiredIsolations($now);
    }

    /**
     * Get list of broker names excluding isolated ones.
     *
     * @param array $publishingRouteDataCache Route cache from Producer
     * @return string[] Array of non-isolated broker names
     */
    public function getIsolatedBrokerNames(array $publishingRouteDataCache): array
    {
        $allBrokers = [];
        foreach ($publishingRouteDataCache as $loadBalancer) {
            if ($loadBalancer instanceof PublishingLoadBalancer) {
                $brokers = $loadBalancer->getAllBrokerNames();
                foreach ($brokers as $broker) {
                    $allBrokers[$broker] = true;
                }
            }
        }

        // Filter out isolated brokers
        $activeBrokers = [];
        $now = time();
        foreach (array_keys($allBrokers) as $broker) {
            if (!isset($this->isolatedEndpoints[$broker]) || 
                ($now - $this->isolatedEndpoints[$broker]) > $this->isolationTimeoutSeconds) {
                $activeBrokers[] = $broker;
            }
        }

        return $activeBrokers;
    }

    /**
     * Get array keys of isolated endpoints (for backward compatibility).
     *
     * @return string[] Array of isolated endpoint keys
     */
    public function getIsolatedEndpointKeys(): array
    {
        return array_keys($this->isolatedEndpoints);
    }

    /**
     * Get count of currently isolated endpoints.
     *
     * @return int Number of isolated endpoints
     */
    public function getIsolatedCount(): int
    {
        return count($this->isolatedEndpoints);
    }

    /**
     * Clear all isolated endpoints.
     */
    public function clearIsolations(): void
    {
        $this->isolatedEndpoints = [];
    }

    /**
     * Set isolation timeout in seconds.
     *
     * @param int $seconds Timeout value
     */
    public function setIsolationTimeout(int $seconds): void
    {
        $this->isolationTimeoutSeconds = max(1, $seconds);
    }

    /**
     * Clean up expired isolations.
     *
     * @param int $currentTime Current timestamp
     */
    private function cleanupExpiredIsolations(int $currentTime): void
    {
        foreach ($this->isolatedEndpoints as $endpoint => $timestamp) {
            if (($currentTime - $timestamp) > $this->isolationTimeoutSeconds) {
                unset($this->isolatedEndpoints[$endpoint]);
            }
        }
    }
}
