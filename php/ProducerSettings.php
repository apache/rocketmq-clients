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
 * ProducerSettings - Immutable configuration holder for Producer.
 *
 * Extracted from Producer constructor to encapsulate all configuration:
 * - Connection: endpoints, credentials, TLS
 * - Retry: maxAttempts, retryPolicy
 * - Timeouts: requestTimeout
 * - Topics: pre-warmed topic list
 * - Namespace: resource scoping
 *
 * Retry policy is mutable (updated by server settings via applyServerBackoffPolicy).
 */
class ProducerSettings
{
    private readonly string $clientId;
    private readonly int $maxAttempts;
    private readonly int $requestTimeout;
    private readonly array $topics;
    private readonly string $namespace;
    private readonly ?SessionCredentials $credentials;
    private readonly ?TlsCredentials $tlsCredentials;
    private readonly bool $sslEnabled;
    private ExponentialBackoffRetryPolicy|CustomizedBackoffRetryPolicy $retryPolicy;

    /**
     * @param string $endpoints gRPC server endpoint
     * @param array $options Configuration options
     */
    public function __construct(
        private readonly string $endpoints,
        array $options = []
    ) {
        if (empty($endpoints)) {
            throw new \InvalidArgumentException("Endpoints cannot be empty");
        }

        $this->clientId = $options['clientId'] ?? ('php-producer-' . getmypid() . '-' . time());
        $this->maxAttempts = $options['maxAttempts'] ?? 3;
        $this->requestTimeout = $options['requestTimeout'] ?? 3000;
        $this->topics = $options['topics'] ?? [];
        $this->namespace = $options['namespace'] ?? '';
        $this->tlsCredentials = $options['tlsCredentials'] ?? null;
        $this->sslEnabled = $options['sslEnabled'] ?? true;

        if (isset($options['credentials']) && $options['credentials'] instanceof SessionCredentials) {
            $this->credentials = $options['credentials'];
        } else {
            $this->credentials = null;
        }

        $this->retryPolicy = new ExponentialBackoffRetryPolicy($this->maxAttempts, 1000, 30000, 2.0);
    }

    /**
     * Apply server-side backoff policy settings.
     *
     * @param object $settings Server settings protobuf object
     * @param Logger $logger Logger for diagnostics
     * @return void
     */
    public function applyServerBackoffPolicy(object $settings, Logger $logger): void
    {
        if (!$settings->hasBackoffPolicy()) {
            return;
        }

        $serverPolicy = $settings->getBackoffPolicy();
        $logger->info("Received backoff policy from server");

        if ($serverPolicy->hasCustomizedBackoff()) {
            $customizedBackoff = $serverPolicy->getCustomizedBackoff();
            if ($customizedBackoff !== null && !ProtobufUtil::isRepeatedFieldEmpty($customizedBackoff->getNext())) {
                $this->retryPolicy = CustomizedBackoffRetryPolicy::fromProtobuf($serverPolicy);
                $logger->info("Updated retry policy from server backoff");
            }
        }
    }

    // ==================== Getters ====================

    public function getEndpoints(): string { return $this->endpoints; }
    public function getClientId(): string { return $this->clientId; }
    public function getMaxAttempts(): int { return $this->maxAttempts; }
    public function getRequestTimeout(): int { return $this->requestTimeout; }
    public function getTopics(): array { return $this->topics; }
    public function getNamespace(): string { return $this->namespace; }
    public function getCredentials(): ?SessionCredentials { return $this->credentials; }
    public function getTlsCredentials(): ?TlsCredentials { return $this->tlsCredentials; }
    public function isSslEnabled(): bool { return $this->sslEnabled; }

    /**
     * Get the current retry policy.
     *
     * @return ExponentialBackoffRetryPolicy|CustomizedBackoffRetryPolicy
     */
    public function getRetryPolicy(): ExponentialBackoffRetryPolicy|CustomizedBackoffRetryPolicy
    {
        return $this->retryPolicy;
    }

    /**
     * Replace the retry policy (used by server settings update).
     *
     * @param ExponentialBackoffRetryPolicy|CustomizedBackoffRetryPolicy $policy
     */
    public function setRetryPolicy(ExponentialBackoffRetryPolicy|CustomizedBackoffRetryPolicy $policy): void
    {
        $this->retryPolicy = $policy;
    }
}
