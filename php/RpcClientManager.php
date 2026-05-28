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
    private array $clientLastUsedTime = [];
    private int $idleTimeoutSeconds = 1800; // 30 minutes
    private int $checkIntervalSeconds = 60; // 1 minute
    private int $lastCheckTime = 0;
    private Logger $logger;

    private function __construct()
    {
        $this->logger = Logger::getInstance('RpcClientManager');
        $this->lastCheckTime = time();
    }

    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public static function reset(): void
    {
        self::$instance = null;
    }

    /**
     * Get or create a MessagingServiceClient for the given endpoints.
     *
     * @param string $endpoints
     * @param array $options
     * @return MessagingServiceClient
     */
    public function getClient(string $endpoints, array $options = []): MessagingServiceClient
    {
        $credentials = $this->resolveCredentials($options);
        $key = $this->makeKey($endpoints, $options);

        if (!isset($this->clients[$key])) {
            $this->logger->info("Creating new RPC client for: {$endpoints}");
            $this->clients[$key] = new MessagingServiceClient($endpoints, [
                'credentials' => $credentials,
            ]);
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
     * Release a specific client connection.
     *
     * @param string $endpoints
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
     * Release all client connections.
     */
    public function releaseAll(): void
    {
        $count = count($this->clients);
        $this->clients = [];
        $this->clientLastUsedTime = [];
        $this->logger->info("Released all {$count} RPC clients");
    }

    /**
     * Get the number of active connections.
     *
     * @return int
     */
    public function getConnectionCount(): int
    {
        return count($this->clients);
    }

    /**
     * Clean up idle client connections.
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
     * @param array $options
     * @return \Grpc\ChannelCredentials|null
     */
    private function resolveCredentials(array $options)
    {
        if (isset($options['tlsCredentials']) && $options['tlsCredentials'] instanceof TlsCredentials) {
            return $options['tlsCredentials']->toChannelCredentials();
        }

        if (isset($options['credentials'])) {
            return $options['credentials'];
        }

        return ChannelCredentials::createInsecure();
    }
}
