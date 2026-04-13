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

require_once __DIR__ . '/ExponentialBackoffRetryPolicy.php';

/**
 * RocketMQ client configuration class
 * 
 * Encapsulates all client configuration items, including:
 * - Server endpoints (endpoints)
 * - Namespace (namespace)
 * - Authentication credentials (credentials)
 * - Request timeout (requestTimeout)
 * - SSL/TLS enabled status (sslEnabled)
 * - Retry policy (retryPolicy)
 * 
 * Usage example:
 * ```php
 * // Basic configuration
 * $config = new ClientConfiguration('127.0.0.1:8080');
 * 
 * // Full configuration
 * $credentials = new Credentials('ak', 'sk');
 * $retryPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000);
 * $config = (new ClientConfiguration('127.0.0.1:8080'))
 *     ->withNamespace('my-namespace')
 *     ->withCredentials($credentials)
 *     ->withRequestTimeout(5)
 *     ->withSslEnabled(true)
 *     ->withRetryPolicy($retryPolicy);
 * ```
 */
class ClientConfiguration
{
    /**
     * @var string Server endpoint address
     */
    private $endpoints;
    
    /**
     * @var string Namespace
     */
    private $namespace = "";
    
    /**
     * @var SessionCredentialsProvider|null Session credentials provider
     */
    private $sessionCredentialsProvider = null;
    
    /**
     * @var int Request timeout in seconds, default is 3 seconds
     */
    private $requestTimeout = 3;
    
    /**
     * @var bool Whether SSL/TLS is enabled, default is true
     */
    private $sslEnabled = true;
    
    /**
     * @var int Max startup attempts
     */
    private $maxStartupAttempts = 3;
    
    /**
     * @var RetryPolicy|null Retry policy (optional)
     */
    private $retryPolicy = null;
    
    /**
     * @var array Connection pool configuration
     */
    private $connectionPoolConfig = [
        'max_connections' => 10,
        'max_idle_time' => 300, // 5 minutes
        'connection_timeout' => 5, // 5 seconds
    ];
    
    /**
     * @var array Cache configuration
     */
    private $cacheConfig = [
        'max_size' => 1000,
        'ttl' => 30, // 30 seconds
        'background_refresh' => true,
    ];
    
    /**
     * @var array Heartbeat configuration
     */
    private $heartbeatConfig = [
        'interval' => 30, // 30 seconds
        'timeout' => 5, // 5 seconds
    ];
    
    /**
     * @var array Load balancing configuration
     */
    private $loadBalancingConfig = [
        'strategy' => 'round_robin', // round_robin, random, least_active
    ];
    
    /**
     * @var array Advanced configuration
     */
    private $advancedConfig = [
        'compression' => false,
        'compression_threshold' => 1024, // 1KB
        'rate_limit' => 0, // 0 means no limit
        'max_message_size' => 4 * 1024 * 1024, // 4MB
    ];
    
    /**
     * Constructor
     * 
     * @param string $endpoints Server endpoint address, format: host:port or host1:port1;host2:port2
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public function __construct($endpoints)
    {
        if (empty($endpoints)) {
            throw new \InvalidArgumentException("Endpoints cannot be empty");
        }
        
        $this->endpoints = $this->parseEndpoints($endpoints);
    }
    
    /**
     * Parse and normalize endpoint addresses
     * 
     * @param string $endpoints Raw endpoint string
     * @return string Normalized endpoint string
     */
    private function parseEndpoints($endpoints)
    {
        // Remove http:// or https:// prefix
        $endpoints = preg_replace('#^https?://#', '', $endpoints);
        
        // Trim leading and trailing whitespace
        return trim($endpoints);
    }
    
    /**
     * Set namespace (chainable)
     * 
     * @param string $namespace Namespace
     * @return self Returns itself to support chain calls
     */
    public function withNamespace($namespace)
    {
        $this->namespace = $namespace ?: '';
        return $this;
    }
    
    /**
     * Set authentication credentials (chainable)
     * 
     * @param Credentials $credentials Authentication credentials object
     * @return self Returns itself to support chain calls
     */
    public function withCredentials(Credentials $credentials)
    {
        $this->credentials = $credentials;
        return $this;
    }
    
    /**
     * Set request timeout (chainable)
     * 
     * @param int $timeout Timeout in seconds
     * @return self Returns itself to support chain calls
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public function withRequestTimeout($timeout)
    {
        if ($timeout <= 0) {
            throw new \InvalidArgumentException("Request timeout must be greater than 0");
        }
        
        $this->requestTimeout = intval($timeout);
        return $this;
    }
    
    /**
     * Set whether to enable SSL/TLS (chainable)
     * 
     * @param bool $enabled Whether to enable
     * @return self Returns itself to support chain calls
     */
    public function withSslEnabled($enabled)
    {
        $this->sslEnabled = (bool)$enabled;
        return $this;
    }
    
    /**
     * Set retry policy (chainable)
     * 
     * @param RetryPolicy $retryPolicy Retry policy object
     * @return self Returns itself to support chain calls
     */
    public function withRetryPolicy(RetryPolicy $retryPolicy)
    {
        $this->retryPolicy = $retryPolicy;
        return $this;
    }
    
    /**
     * Set session credentials provider (chainable)
     * 
     * @param SessionCredentialsProvider $sessionCredentialsProvider Session credentials provider
     * @return self Returns itself to support chain calls
     */
    public function withCredentialsProvider(SessionCredentialsProvider $sessionCredentialsProvider)
    {
        $this->sessionCredentialsProvider = $sessionCredentialsProvider;
        return $this;
    }
    
    /**
     * Set max startup attempts (chainable)
     * 
     * @param int $maxStartupAttempts Max attempt times when client startup
     * @return self Returns itself to support chain calls
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public function withMaxStartupAttempts($maxStartupAttempts)
    {
        if ($maxStartupAttempts <= 0) {
            throw new \InvalidArgumentException("Max startup attempts must be greater than 0");
        }
        
        $this->maxStartupAttempts = intval($maxStartupAttempts);
        return $this;
    }
    
    /**
     * Get server endpoint address
     * 
     * @return string Server endpoint address
     */
    public function getEndpoints()
    {
        return $this->endpoints;
    }
    
    /**
     * Get namespace
     * 
     * @return string Namespace
     */
    public function getNamespace()
    {
        return $this->namespace;
    }
    
    /**
     * Get session credentials provider
     * 
     * @return SessionCredentialsProvider|null Session credentials provider, returns null if not set
     */
    public function getSessionCredentialsProvider()
    {
        return $this->sessionCredentialsProvider;
    }
    
    /**
     * Check if session credentials provider is set
     * 
     * @return bool Whether session credentials provider is set
     */
    public function hasSessionCredentialsProvider()
    {
        return $this->sessionCredentialsProvider !== null;
    }
    
    /**
     * Get max startup attempts
     * 
     * @return int Max startup attempts
     */
    public function getMaxStartupAttempts()
    {
        return $this->maxStartupAttempts;
    }
    
    /**
     * Get request timeout
     * 
     * @return int Request timeout in seconds
     */
    public function getRequestTimeout()
    {
        return $this->requestTimeout;
    }
    
    /**
     * Check if SSL/TLS is enabled
     * 
     * @return bool Whether SSL/TLS is enabled
     */
    public function isSslEnabled()
    {
        return $this->sslEnabled;
    }
    
    /**
     * Get retry policy
     * 
     * @return RetryPolicy|null Retry policy object, returns null if not set
     */
    public function getRetryPolicy()
    {
        return $this->retryPolicy;
    }
    
    /**
     * Get session credentials provider
     * 
     * @return SessionCredentialsProvider|null Session credentials provider, returns null if not set
     */
    public function getCredentialsProvider()
    {
        return $this->credentialsProvider;
    }
    
    /**
     * Check if session credentials provider is set
     * 
     * @return bool Whether session credentials provider is set
     */
    public function hasCredentialsProvider()
    {
        return $this->credentialsProvider !== null;
    }
    
    /**
     * Check if credentials are set
     * 
     * @return bool Whether credentials are set
     */
    public function hasCredentials()
    {
        return isset($this->credentials) && $this->credentials !== null;
    }
    
    /**
     * Get or create default retry policy
     * 
     * @return RetryPolicy Retry policy object
     */
    public function getOrCreateRetryPolicy()
    {
        if ($this->retryPolicy === null) {
            $this->retryPolicy = new ExponentialBackoffRetryPolicy(3, 100, 5000, 2.0);
        }
        
        return $this->retryPolicy;
    }
    
    /**
     * Clone current configuration object
     * 
     * @return self Cloned configuration object
     */
    public function clone()
    {
        $clone = new self($this->endpoints);
        $clone->namespace = $this->namespace;
        $clone->credentials = $this->credentials;
        $clone->requestTimeout = $this->requestTimeout;
        $clone->sslEnabled = $this->sslEnabled;
        $clone->retryPolicy = $this->retryPolicy;
        $clone->connectionPoolConfig = $this->connectionPoolConfig;
        $clone->cacheConfig = $this->cacheConfig;
        $clone->heartbeatConfig = $this->heartbeatConfig;
        $clone->loadBalancingConfig = $this->loadBalancingConfig;
        $clone->advancedConfig = $this->advancedConfig;
        
        return $clone;
    }
    
    /**
     * Set connection pool configuration (chainable)
     * 
     * @param array $config Connection pool configuration
     * @return self Returns itself to support chain calls
     */
    public function withConnectionPoolConfig(array $config)
    {
        $this->connectionPoolConfig = array_merge($this->connectionPoolConfig, $config);
        return $this;
    }
    
    /**
     * Get connection pool configuration
     * 
     * @return array Connection pool configuration
     */
    public function getConnectionPoolConfig()
    {
        return $this->connectionPoolConfig;
    }
    
    /**
     * Set cache configuration (chainable)
     * 
     * @param array $config Cache configuration
     * @return self Returns itself to support chain calls
     */
    public function withCacheConfig(array $config)
    {
        $this->cacheConfig = array_merge($this->cacheConfig, $config);
        return $this;
    }
    
    /**
     * Get cache configuration
     * 
     * @return array Cache configuration
     */
    public function getCacheConfig()
    {
        return $this->cacheConfig;
    }
    
    /**
     * Set heartbeat configuration (chainable)
     * 
     * @param array $config Heartbeat configuration
     * @return self Returns itself to support chain calls
     */
    public function withHeartbeatConfig(array $config)
    {
        $this->heartbeatConfig = array_merge($this->heartbeatConfig, $config);
        return $this;
    }
    
    /**
     * Get heartbeat configuration
     * 
     * @return array Heartbeat configuration
     */
    public function getHeartbeatConfig()
    {
        return $this->heartbeatConfig;
    }
    
    /**
     * Set load balancing configuration (chainable)
     * 
     * @param array $config Load balancing configuration
     * @return self Returns itself to support chain calls
     */
    public function withLoadBalancingConfig(array $config)
    {
        $this->loadBalancingConfig = array_merge($this->loadBalancingConfig, $config);
        return $this;
    }
    
    /**
     * Get load balancing configuration
     * 
     * @return array Load balancing configuration
     */
    public function getLoadBalancingConfig()
    {
        return $this->loadBalancingConfig;
    }
    
    /**
     * Set advanced configuration (chainable)
     * 
     * @param array $config Advanced configuration
     * @return self Returns itself to support chain calls
     */
    public function withAdvancedConfig(array $config)
    {
        $this->advancedConfig = array_merge($this->advancedConfig, $config);
        return $this;
    }
    
    /**
     * Get advanced configuration
     * 
     * @return array Advanced configuration
     */
    public function getAdvancedConfig()
    {
        return $this->advancedConfig;
    }
    
    /**
     * Convert to string representation
     * 
     * @return string Configuration description
     */
    public function __toString()
    {
        $credInfo = $this->hasCredentials() ? ', credentials=' . $this->credentials : '';
        $retryInfo = $this->retryPolicy !== null ? ', retryPolicy=' . $this->retryPolicy : '';
        
        return sprintf(
            "ClientConfiguration[endpoints=%s, namespace=%s, timeout=%ds, ssl=%s%s%s]",
            $this->endpoints,
            $this->namespace ?: 'default',
            $this->requestTimeout,
            $this->sslEnabled ? 'true' : 'false',
            $credInfo,
            $retryInfo
        );
    }
}
