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
     * @var string Namespace (optional)
     */
    private $namespace = '';
    
    /**
     * @var Credentials|null Authentication credentials (optional)
     */
    private $credentials = null;
    
    /**
     * @var int Request timeout in seconds, default is 3 seconds
     */
    private $requestTimeout = 3;
    
    /**
     * @var bool Whether SSL/TLS is enabled, default is false
     */
    private $sslEnabled = false;
    
    /**
     * @var RetryPolicy|null Retry policy (optional)
     */
    private $retryPolicy = null;
    
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
     * Get authentication credentials
     * 
     * @return Credentials|null Authentication credentials object, returns null if not set
     */
    public function getCredentials()
    {
        return $this->credentials;
    }
    
    /**
     * Check if authentication credentials are set
     * 
     * @return bool Whether authentication credentials are set
     */
    public function hasCredentials()
    {
        return $this->credentials !== null;
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
        
        return $clone;
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
