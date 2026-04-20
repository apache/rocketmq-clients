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

namespace Apache\Rocketmq\Connection;

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Exception\NetworkException;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\MetricName;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Grpc\ChannelCredentials;

/**
 * Connection pool for managing gRPC connections
 */
class ConnectionPool {
    /**
     * @var array Connection pool storage
     */
    private $pool = [];
    
    /**
     * @var array Connection configuration
     */
    private $config = [
        'max_connections' => 10,
        'max_idle_time' => 300, // 5 minutes
        'connection_timeout' => 5, // 5 seconds
    ];
    
    /**
     * @var array Connection last used time
     */
    private $lastUsed = [];
    
    /**
     * @var static Singleton instance
     */
    private static $instance;
    
    /**
     * @var MetricsCollector|null Metrics collector
     */
    private $metricsCollector;
    
    /**
     * @var array Connection borrow timestamps
     */
    private $borrowTimestamps = [];
    
    /**
     * Private constructor
     */
    private function __construct() {
        // Enable detailed gRPC logging for debugging
        putenv('GRPC_VERBOSITY=DEBUG');
        putenv('GRPC_TRACE=all');
        
        // Initialize metrics collector
        $this->metricsCollector = new MetricsCollector('connection_pool');
        
        // Start cleanup timer
        $this->startCleanupTimer();
    }
    
    /**
     * Get singleton instance
     *
     * @return ConnectionPool
     */
    public static function getInstance() {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Set pool configuration
     *
     * @param array $config
     * @return ConnectionPool
     */
    public function setConfig(array $config) {
        $this->config = array_merge($this->config, $config);
        return $this;
    }
    
    /**
     * Set configuration from ClientConfiguration
     *
     * @param \Apache\Rocketmq\ClientConfiguration $clientConfig
     * @return ConnectionPool
     */
    public function setConfigFromClientConfiguration(\Apache\Rocketmq\ClientConfiguration $clientConfig) {
        $poolConfig = $clientConfig->getConnectionPoolConfig();
        return $this->setConfig($poolConfig);
    }
    
    /**
     * Get connection from pool
     *
     * @param ClientConfiguration $clientConfig
     * @return MessagingServiceClient
     * @throws NetworkException
     */
    public function getConnection(ClientConfiguration $clientConfig) {
        $key = $this->getConnectionKey($clientConfig);
        $startTime = microtime(true);
        
        // Check if connection exists in pool
        if (isset($this->pool[$key]) && !empty($this->pool[$key])) {
            // Get the first available connection
            $client = array_shift($this->pool[$key]);
            $this->lastUsed[$key] = time();
            
            // Record metrics
            $waitTime = (microtime(true) - $startTime) * 1000; // Convert to milliseconds
            $this->recordMetrics($key, $waitTime);
            
            return $client;
        }
        
        // Create new connection if pool is not full
        if (!$this->isPoolFull($key)) {
            try {
                $client = $this->createConnection($clientConfig);
                $this->lastUsed[$key] = time();
                
                // Record metrics
                $waitTime = (microtime(true) - $startTime) * 1000; // Convert to milliseconds
                $this->recordMetrics($key, $waitTime);
                
                return $client;
            } catch (\Exception $e) {
                throw new NetworkException("Failed to create connection: " . $e->getMessage(), 503, $e);
            }
        }
        
        throw new NetworkException("Connection pool is full", 503);
    }
    
    /**
     * Get or create connection with specific client ID
     * This ensures all RPC calls use the same client ID as Telemetry Session
     *
     * @param ClientConfiguration $clientConfig
     * @param string $clientId Specific client ID to use
     * @return MessagingServiceClient
     * @throws NetworkException
     */
    public function getConnectionWithClientId(ClientConfiguration $clientConfig, string $clientId) {
        // Use a special key that includes the client ID
        $key = $this->getConnectionKey($clientConfig) . ':' . $clientId;
        $startTime = microtime(true);
        
        // Check if connection exists in pool
        if (isset($this->pool[$key]) && !empty($this->pool[$key])) {
            $client = array_shift($this->pool[$key]);
            $this->lastUsed[$key] = time();
            
            $waitTime = (microtime(true) - $startTime) * 1000;
            $this->recordMetrics($key, $waitTime);
            
            return $client;
        }
        
        // Create new connection with the specified client ID
        if (!$this->isPoolFull($key)) {
            try {
                $client = $this->createConnection($clientConfig, $clientId);
                $this->lastUsed[$key] = time();
                
                $waitTime = (microtime(true) - $startTime) * 1000;
                $this->recordMetrics($key, $waitTime);
                
                return $client;
            } catch (\Exception $e) {
                throw new NetworkException("Failed to create connection: " . $e->getMessage(), 503, $e);
            }
        }
        
        throw new NetworkException("Connection pool is full", 503);
    }
    
    /**
     * Return connection to pool
     *
     * @param ClientConfiguration $clientConfig
     * @param MessagingServiceClient $client
     * @return void
     */
    public function returnConnection(ClientConfiguration $clientConfig, MessagingServiceClient $client) {
        $key = $this->getConnectionKey($clientConfig);
        
        // Check if pool is not full
        if (!$this->isPoolFull($key)) {
            $this->pool[$key][] = $client;
            $this->lastUsed[$key] = time();
            
            // Record metrics
            $this->metricsCollector->incrementCounter(MetricName::CONNECTION_POOL_RETURN, [
                'endpoint' => $clientConfig->getEndpoints(),
                'ssl' => $clientConfig->isSslEnabled() ? 'true' : 'false'
            ]);
        }
    }
    
    /**
     * Create new connection
     *
     * @param ClientConfiguration $clientConfig
     * @param string|null $clientId Optional client ID to use (if null, generates a random one)
     * @return MessagingServiceClient
     */
    private function createConnection(ClientConfiguration $clientConfig, $clientId = null) {
        // Use provided client ID or generate a unique one
        if ($clientId === null) {
            $clientId = 'php-client-' . uniqid();
        }
        
        $options = [
            'credentials' => $clientConfig->isSslEnabled() 
                ? ChannelCredentials::createSsl() 
                : ChannelCredentials::createInsecure(),
            'update_metadata' => function ($metaData) use ($clientConfig, $clientId) {
                // Add client ID to headers
                $metaData['x-mq-client-id'] = [$clientId];
                
                // Add other required metadata (aligned with Java Signature.java)
                $metaData['x-mq-language'] = ['PHP'];
                $metaData['x-mq-protocol'] = ['GRPC_V2'];
                $metaData['x-mq-client-version'] = ['5.0.0'];
                $metaData['x-mq-request-id'] = [uniqid('php-', true)];
                
                // Add namespace if available
                $namespace = $clientConfig->getNamespace();
                if (!empty($namespace)) {
                    $metaData['x-mq-namespace'] = [$namespace];
                }
                
                // Add authentication information via SessionCredentialsProvider
                $provider = $clientConfig->getCredentialsProvider();
                if ($provider !== null) {
                    $credentials = $provider->getSessionCredentials();
                    if ($credentials !== null) {
                        $accessKey = $credentials->getAccessKey();
                        $accessSecret = $credentials->getAccessSecret();
                        
                        if (!empty($accessKey) && !empty($accessSecret)) {
                            // Generate signature (aligned with Java TLSHelper.sign)
                            // Java uses HMAC-SHA1 with uppercase hex output
                            $dateTime = gmdate('Ymd\THis\Z');
                            $signature = strtoupper(hash_hmac('sha1', $dateTime, $accessSecret));
                            
                            $authorization = 'MQv2-HMAC-SHA1 ' .
                                'Credential=' . $accessKey . ', ' .
                                'SignedHeaders=x-mq-date-time, ' .
                                'Signature=' . $signature;
                            
                            $metaData['authorization'] = [$authorization];
                            $metaData['x-mq-date-time'] = [$dateTime];
                            
                            // Add session token if available
                            $securityToken = $credentials->tryGetSecurityToken();
                            if ($securityToken !== null) {
                                $metaData['x-mq-session-token'] = [$securityToken];
                            }
                        }
                    }
                }
                
                return $metaData;
            },
            'timeout' => $this->config['connection_timeout'] * 1000, // Convert to milliseconds
            
            // Enable gRPC logging and tracing
            'grpc.log_verbosity' => GRPC_LOG_DEBUG,
            'grpc.trace' => 'all',
        ];
        
        return new MessagingServiceClient($clientConfig->getEndpoints(), $options);
    }
    
    /**
     * Get connection key
     *
     * @param ClientConfiguration $clientConfig
     * @return string
     */
    private function getConnectionKey(ClientConfiguration $clientConfig) {
        return $clientConfig->getEndpoints() . ($clientConfig->isSslEnabled() ? ':ssl' : ':plain');
    }
    
    /**
     * Check if pool is full
     *
     * @param string $key
     * @return bool
     */
    private function isPoolFull($key) {
        return isset($this->pool[$key]) && count($this->pool[$key]) >= $this->config['max_connections'];
    }
    
    /**
     * Start cleanup timer
     *
     * @return void
     */
    private function startCleanupTimer() {
        // This is a simple implementation, in production you might want to use a more robust approach
        // For example, using pcntl_alarm or a separate process
        
        // Register shutdown function to clean up connections
        register_shutdown_function(function() {
            $this->cleanup();
        });
    }
    
    /**
     * Clean up idle connections
     *
     * @return void
     */
    public function cleanup() {
        $now = time();
        $maxIdleTime = $this->config['max_idle_time'];
        
        foreach ($this->pool as $key => $connections) {
            if (isset($this->lastUsed[$key]) && ($now - $this->lastUsed[$key]) > $maxIdleTime) {
                // Clear idle connections
                $this->pool[$key] = [];
                unset($this->lastUsed[$key]);
            }
        }
    }
    
    /**
     * Get pool status
     *
     * @return array
     */
    public function getStatus() {
        $status = [];
        foreach ($this->pool as $key => $connections) {
            $status[$key] = [
                'active_connections' => count($connections),
                'last_used' => isset($this->lastUsed[$key]) ? $this->lastUsed[$key] : null,
            ];
        }
        return $status;
    }
    
    /**
     * Clear all connections
     *
     * @return void
     */
    public function clear() {
        $this->pool = [];
        $this->lastUsed = [];
    }
    
    /**
     * Get connection count
     *
     * @return int
     */
    public function getConnectionCount() {
        $count = 0;
        foreach ($this->pool as $connections) {
            $count += count($connections);
        }
        return $count;
    }
    
    /**
     * Record connection pool metrics
     *
     * @param string $key Connection key
     * @param float $waitTime Wait time in milliseconds
     * @return void
     */
    private function recordMetrics($key, $waitTime) {
        // Split key into endpoint and ssl flag
        $parts = explode(':', $key);
        $endpoint = $parts[0];
        $ssl = isset($parts[1]) && $parts[1] === 'ssl' ? 'true' : 'false';
        
        // Record connection pool metrics
        $this->metricsCollector->incrementCounter(MetricName::CONNECTION_POOL_BORROW, [
            'endpoint' => $endpoint,
            'ssl' => $ssl
        ]);
        
        $this->metricsCollector->observeHistogram(MetricName::CONNECTION_POOL_WAIT_TIME, [
            'endpoint' => $endpoint,
            'ssl' => $ssl
        ], $waitTime);
        
        // Update pool size metrics
        $currentSize = $this->getConnectionCount();
        $this->metricsCollector->setGauge(MetricName::CONNECTION_POOL_SIZE, [
            'endpoint' => $endpoint,
            'ssl' => $ssl
        ], $currentSize);
        
        $this->metricsCollector->setGauge(MetricName::CONNECTION_POOL_MAX_SIZE, [
            'endpoint' => $endpoint,
            'ssl' => $ssl
        ], $this->config['max_connections']);
    }
    
    /**
     * Get metrics collector
     *
     * @return MetricsCollector
     */
    public function getMetricsCollector() {
        return $this->metricsCollector;
    }
    
    /**
     * Export metrics to JSON
     *
     * @return string
     */
    public function exportMetrics() {
        return $this->metricsCollector->exportToJson();
    }
}
