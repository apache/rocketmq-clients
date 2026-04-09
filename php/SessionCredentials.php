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
 * Session credentials used in service authentications
 */
class SessionCredentials {
    /**
     * @var string Access key
     */
    private $accessKey;
    
    /**
     * @var string Access secret
     */
    private $accessSecret;
    
    /**
     * @var string|null Security token
     */
    private $securityToken;
    
    /**
     * Constructor
     * 
     * @param string $accessKey Access key
     * @param string $accessSecret Access secret
     * @param string|null $securityToken Security token (optional)
     * @throws \InvalidArgumentException If access key or access secret is null
     */
    public function __construct(string $accessKey, string $accessSecret, ?string $securityToken = null) {
        if (empty($accessKey)) {
            throw new \InvalidArgumentException("Access key should not be empty");
        }
        if (empty($accessSecret)) {
            throw new \InvalidArgumentException("Access secret should not be empty");
        }
        
        $this->accessKey = $accessKey;
        $this->accessSecret = $accessSecret;
        $this->securityToken = $securityToken;
    }
    
    /**
     * Get access key
     * 
     * @return string Access key
     */
    public function getAccessKey(): string {
        return $this->accessKey;
    }
    
    /**
     * Get access secret
     * 
     * @return string Access secret
     */
    public function getAccessSecret(): string {
        return $this->accessSecret;
    }
    
    /**
     * Try to get security token
     * 
     * @return string|null Security token if present, null otherwise
     */
    public function tryGetSecurityToken(): ?string {
        return $this->securityToken;
    }
    
    /**
     * Check if security token is present
     * 
     * @return bool True if security token is present, false otherwise
     */
    public function hasSecurityToken(): bool {
        return $this->securityToken !== null;
    }
}
