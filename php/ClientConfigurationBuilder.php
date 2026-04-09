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
 * Builder to set ClientConfiguration
 */
class ClientConfigurationBuilder {
    /**
     * @var string Endpoints
     */
    private $endpoints;
    
    /**
     * @var SessionCredentialsProvider|null Session credentials provider
     */
    private $sessionCredentialsProvider = null;
    
    /**
     * @var int Request timeout in seconds
     */
    private $requestTimeout = 3;
    
    /**
     * @var bool Whether SSL is enabled
     */
    private $sslEnabled = true;
    
    /**
     * @var string Namespace
     */
    private $namespace = "";
    
    /**
     * @var int Max startup attempts
     */
    private $maxStartupAttempts = 3;
    
    /**
     * Configure the access point with which the SDK should communicate
     * 
     * @param string $endpoints Address of service
     * @return ClientConfigurationBuilder The client configuration builder instance
     * @throws \InvalidArgumentException If endpoints is null
     */
    public function setEndpoints(string $endpoints): self {
        if (empty($endpoints)) {
            throw new \InvalidArgumentException("endpoints should not be empty");
        }
        $this->endpoints = $endpoints;
        return $this;
    }
    
    /**
     * Config the session credential provider
     * 
     * @param SessionCredentialsProvider $sessionCredentialsProvider Session credential provider
     * @return ClientConfigurationBuilder The client configuration builder instance
     * @throws \InvalidArgumentException If sessionCredentialsProvider is null
     */
    public function setCredentialProvider(SessionCredentialsProvider $sessionCredentialsProvider): self {
        if ($sessionCredentialsProvider === null) {
            throw new \InvalidArgumentException("sessionCredentialsProvider should not be null");
        }
        $this->sessionCredentialsProvider = $sessionCredentialsProvider;
        return $this;
    }
    
    /**
     * Configure request timeout for ordinary RPC
     * 
     * @param int $requestTimeout RPC request timeout in seconds
     * @return ClientConfigurationBuilder The client configuration builder instance
     * @throws \InvalidArgumentException If requestTimeout is null or less than 1
     */
    public function setRequestTimeout(int $requestTimeout): self {
        if ($requestTimeout < 1) {
            throw new \InvalidArgumentException("requestTimeout should be at least 1 second");
        }
        $this->requestTimeout = $requestTimeout;
        return $this;
    }
    
    /**
     * Enable or disable the use of Secure Sockets Layer (SSL) for network transport
     * 
     * @param bool $sslEnabled A boolean value indicating whether SSL should be enabled or not
     * @return ClientConfigurationBuilder The client configuration builder instance
     */
    public function enableSsl(bool $sslEnabled): self {
        $this->sslEnabled = $sslEnabled;
        return $this;
    }
    
    /**
     * Configure namespace for client
     * 
     * @param string $namespace Namespace
     * @return ClientConfigurationBuilder The client configuration builder instance
     * @throws \InvalidArgumentException If namespace is null
     */
    public function setNamespace(string $namespace): self {
        if ($namespace === null) {
            throw new \InvalidArgumentException("namespace should not be null");
        }
        $this->namespace = $namespace;
        return $this;
    }
    
    /**
     * Configure maxStartupAttempts for client
     * 
     * @param int $maxStartupAttempts Max attempt times when client startup
     * @return ClientConfigurationBuilder The client configuration builder instance
     * @throws \InvalidArgumentException If maxStartupAttempts is less than 1
     */
    public function setMaxStartupAttempts(int $maxStartupAttempts): self {
        if ($maxStartupAttempts < 1) {
            throw new \InvalidArgumentException("maxStartupAttempts should be more than 0");
        }
        $this->maxStartupAttempts = $maxStartupAttempts;
        return $this;
    }
    
    /**
     * Finalize the build of ClientConfiguration
     * 
     * @return ClientConfiguration The client configuration instance
     * @throws \InvalidArgumentException If endpoints is null
     */
    public function build(): ClientConfiguration {
        if (empty($this->endpoints)) {
            throw new \InvalidArgumentException("endpoints should not be empty");
        }
        
        $config = new ClientConfiguration($this->endpoints);
        $config->withNamespace($this->namespace)
            ->withRequestTimeout($this->requestTimeout)
            ->withSslEnabled($this->sslEnabled)
            ->withMaxStartupAttempts($this->maxStartupAttempts);
        
        if ($this->sessionCredentialsProvider !== null) {
            $config->withCredentialsProvider($this->sessionCredentialsProvider);
        }
        
        return $config;
    }
}
