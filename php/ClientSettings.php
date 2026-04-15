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

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Settings as V2Settings;
use Apache\Rocketmq\V2\Publishing;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\BackoffStrategy;
use Google\Protobuf\Duration;

/**
 * Base settings class for client configuration
 * 
 * Reference: Java Settings class
 */
abstract class ClientSettings {
    /**
     * @var string Namespace
     */
    protected $namespace;
    
    /**
     * @var string Client ID
     */
    protected $clientId;
    
    /**
     * @var int Client type (use V2\ClientType constants)
     */
    protected $clientType;
    
    /**
     * @var string Endpoints
     */
    protected $endpoints;
    
    /**
     * @var int Max attempts for retry
     */
    protected $maxAttempts = 3;
    
    /**
     * @var Duration Request timeout
     */
    protected $requestTimeout;
    
    /**
     * Constructor
     * 
     * @param string $namespace Namespace
     * @param string $clientId Client ID
     * @param int $clientType Client type (use V2\ClientType constants)
     * @param string $endpoints Endpoints
     * @param int $maxAttempts Max retry attempts
     * @param int $requestTimeoutMs Request timeout in milliseconds
     */
    public function __construct(
        string $namespace,
        string $clientId,
        int $clientType,
        string $endpoints,
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        $this->namespace = $namespace;
        $this->clientId = $clientId;
        $this->clientType = $clientType;
        $this->endpoints = $endpoints;
        $this->maxAttempts = $maxAttempts;
        
        $timeout = new Duration();
        $timeout->setSeconds(intdiv($requestTimeoutMs, 1000));
        $timeout->setNanos(($requestTimeoutMs % 1000) * 1000000);
        $this->requestTimeout = $timeout;
    }
    
    /**
     * Convert to protobuf Settings
     * 
     * @return V2Settings Protobuf settings
     */
    abstract public function toProtobuf(): V2Settings;
    
    /**
     * Get namespace
     * 
     * @return string Namespace
     */
    public function getNamespace(): string {
        return $this->namespace;
    }
    
    /**
     * Get client ID
     * 
     * @return string Client ID
     */
    public function getClientId(): string {
        return $this->clientId;
    }
    
    /**
     * Get client type
     * 
     * @return int Client type
     */
    public function getClientType(): int {
        return $this->clientType;
    }
    
    /**
     * Get endpoints
     * 
     * @return string Endpoints
     */
    public function getEndpoints(): string {
        return $this->endpoints;
    }
    
    /**
     * Get max attempts
     * 
     * @return int Max attempts
     */
    public function getMaxAttempts(): int {
        return $this->maxAttempts;
    }
    
    /**
     * Get request timeout
     * 
     * @return Duration Request timeout
     */
    public function getRequestTimeout(): Duration {
        return $this->requestTimeout;
    }
}
