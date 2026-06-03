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
use Grpc\ChannelCredentials;

class RpcClientManager
{
    private static ?self $instance = null;

    private array $clients = [];
    private array $mocks = [];
    private array $clientLastUsedTime = [];
    private int $idleTimeoutSeconds = 1800; // 30 minutes
    private int $checkIntervalSeconds = 60; // 1 minute
    private int $lastCheckTime = 0;
    private Logger $logger;

    /**
     * Initialize client manager with logger and timestamp.
     */
    private function __construct()
    {
        $this->logger = Logger::getInstance('RpcClientManager');
        $this->lastCheckTime = time();
    }

    /**
     * Get the singleton instance, creating it if necessary.
     *
     * @return self
     */
    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * Reset the singleton instance (primarily for testing).
     *
     * @return void
     */
    public static function reset(): void
    {
        self::$instance = null;
    }

    /**
     * Get or create a MessagingServiceClient for the given endpoints.
     *
     * Clients are cached and reused based on endpoint + TLS configuration.
     * Idle clients are automatically cleaned up every 60 seconds if unused for 30 minutes.
     *
     * @param string $endpoints Server endpoint in format "host:port"
     * @param array $options Optional configuration:
     *                       - 'tlsCredentials': TlsCredentials instance for TLS/mTLS
     *                       - 'credentials': Pre-created ChannelCredentials
     * @return MessagingServiceClient gRPC client instance
     */
    public function getClient(string $endpoints, array $options = []): MessagingServiceClient
    {
        $credentials = $this->resolveCredentials($options);
        $key = $this->makeKey($endpoints, $options);

        // Check mock registry first
        if (isset($this->mocks[$key])) {
            $this->clientLastUsedTime[$key] = time();
            return $this->mocks[$key];
        }

        if (!isset($this->clients[$key])) {
            $this->logger->info("Creating new RPC client for: {$endpoints}");
            $opts = ['credentials' => $credentials];

            // Merge channel args from TlsCredentials (e.g., SSL target name override for dev)
            if (isset($options['tlsCredentials']) && $options['tlsCredentials'] instanceof TlsCredentials) {
                $opts = array_merge($opts, $options['tlsCredentials']->getChannelArgs());
            }

            $this->clients[$key] = new MessagingServiceClient($endpoints, $opts);
        }

        $this->clientLastUsedTime[$key] = time();

        // Periodically clean up idle connections
        $now = time();
        if ($now - $this->lastCheckTime >= $this->checkIntervalSeconds) {
            $this->cleanupIdleClients();
            $this->lastCheckTime = $now;
        }

        return $this->clients[$key];
    }

    /**
     * Register a mock MessagingServiceClient for the given endpoints.
     * Subsequent calls to getClient() with matching endpoints will return this mock.
     *
     * @param string $endpoints Server endpoint in format "host:port"
     * @param MessagingServiceClient $mock The mock client to return
     * @return void
     */
    public function registerMock(string $endpoints, MessagingServiceClient $mock): void
    {
        $key = $this->makeKey($endpoints, []);
        $this->mocks[$key] = $mock;
        $this->logger->info("Registered mock client for: {$key}");
    }

    /**
     * Remove all registered mocks.
     *
     * @return void
     */
    public function clearMocks(): void
    {
        $this->mocks = [];
    }

    /**
     * Release a specific client connection by endpoint prefix.
     *
     * All clients whose key starts with the given endpoint will be removed.
     *
     * @param string $endpoints Endpoint prefix to match (e.g., "localhost:8080")
     * @return void
     */
    public function releaseClient(string $endpoints): void
    {
        $keysToRemove = [];
        foreach ($this->clients as $key => $client) {
            if (strpos($key, $endpoints) === 0) {
                $keysToRemove[] = $key;
            }
        }

        foreach ($keysToRemove as $key) {
            unset($this->clients[$key]);
            unset($this->clientLastUsedTime[$key]);
            $this->logger->info("Released RPC client: {$key}");
        }
    }

    /**
     * Release all client connections and clear the cache.
     *
     * @return void
     */
    public function releaseAll(): void
    {
        $count = count($this->clients);
        $this->clients = [];
        $this->clientLastUsedTime = [];
        $this->logger->info("Released all {$count} RPC clients");
    }

    /**
     * Get the number of active connections in the pool.
     *
     * @return int Number of cached client connections
     */
    public function getConnectionCount(): int
    {
        return count($this->clients);
    }

    /**
     * Clean up idle client connections that haven't been used for more than idleTimeoutSeconds.
     *
     * This method is called automatically every checkIntervalSeconds (60s) when getClient() is invoked.
     *
     * @return void
     */
    private function cleanupIdleClients(): void
    {
        $now = time();
        $keysToRemove = [];

        foreach ($this->clientLastUsedTime as $key => $lastUsed) {
            if ($now - $lastUsed > $this->idleTimeoutSeconds) {
                $keysToRemove[] = $key;
            }
        }

        foreach ($keysToRemove as $key) {
            unset($this->clients[$key]);
            unset($this->clientLastUsedTime[$key]);
            $this->logger->info("Cleaned up idle RPC client: {$key}");
        }
    }

    /**
     * Generate a unique cache key based on endpoint and TLS configuration.
     *
     * Key format: "{endpoint}:{tlsFingerprint}"
     * Examples:
     *   - "localhost:8080:insecure"
     *   - "localhost:8080:tls|ca:/path/to/ca.pem"
     *   - "localhost:8080:mtls:/path/to/client.pem|no-verify"
     *
     * @param string $endpoints Server endpoint
     * @param array $options Client options containing TLS credentials
     * @return string Unique cache key
     */
    private function makeKey(string $endpoints, array $options): string
    {
        $tlsFingerprint = 'insecure';
        if (isset($options['tlsCredentials']) && $options['tlsCredentials'] instanceof TlsCredentials) {
            $tls = $options['tlsCredentials'];
            $parts = [];
            $parts[] = $tls->isInsecure() ? 'insecure' : 'tls';
            if ($tls->getCaCertPath() !== null) {
                $parts[] = 'ca:' . $tls->getCaCertPath();
            }
            if ($tls->getClientCertPath() !== null) {
                $parts[] = 'mtls:' . $tls->getClientCertPath();
            }
            if (!$tls->shouldVerifyPeer()) {
                $parts[] = 'no-verify';
            }
            $tlsFingerprint = implode('|', $parts);
        } elseif (isset($options['credentials'])) {
            $tlsFingerprint = 'secure';
        }
        return $endpoints . ':' . $tlsFingerprint;
    }

    /**
     * Resolve gRPC channel credentials from options.
     *
     * Priority:
     * 1. Explicit tlsCredentials option (TlsCredentials instance)
     * 2. Explicit credentials option (pre-created ChannelCredentials)
     * 3. sslEnabled=false → TlsCredentials::createInsecure() (plaintext, for dev/CI)
     * 4. Default: TlsCredentials::createDefault() for secure connection
     *
     * SECURITY NOTE: SSL is enabled by default to prevent accidental plaintext
     * connections in production. Set sslEnabled=false explicitly only for
     * development/testing/CI.
     *
     * @param array $options Configuration options
     * @return \Grpc\ChannelCredentials|null Resolved credentials
     */
    private function resolveCredentials(array $options)
    {
        if (isset($options['tlsCredentials']) && $options['tlsCredentials'] instanceof TlsCredentials) {
            return $options['tlsCredentials']->toChannelCredentials();
        }

        if (isset($options['credentials'])) {
            return $options['credentials'];
        }

        $sslEnabled = $options['sslEnabled'] ?? true;

        if (!$sslEnabled) {
            $this->logger->debug("SSL disabled, using insecure (plaintext) connection");
            return TlsCredentials::createInsecure()->toChannelCredentials();
        }

        $this->logger->debug("Using default TLS configuration");
        return TlsCredentials::createDefault()->toChannelCredentials();
    }
}
