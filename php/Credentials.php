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
 * RocketMQ authentication credentials class
 * 
 * Used to store and manage authentication information required to access RocketMQ services
 * 
 * Usage example:
 * $credentials = new Credentials('YOUR_ACCESS_KEY', 'YOUR_ACCESS_SECRET');
 * // Or use temporary security token (STS)
 * $credentials = new Credentials('YOUR_ACCESS_KEY', 'YOUR_ACCESS_SECRET', 'YOUR_SECURITY_TOKEN');
 */
class Credentials
{
    /**
     * @var string Access key ID
     */
    private $accessKey;
    
    /**
     * @var string Access key secret
     */
    private $accessSecret;
    
    /**
     * @var string|null Security token (optional, for STS temporary credentials)
     */
    private $securityToken;
    
    /**
     * Constructor
     * 
     * @param string $accessKey Access key ID
     * @param string $accessSecret Access key secret
     * @param string|null $securityToken Security token (optional)
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public function __construct($accessKey, $accessSecret, $securityToken = null)
    {
        if (empty($accessKey)) {
            throw new \InvalidArgumentException("Access key cannot be empty");
        }
        
        if (empty($accessSecret)) {
            throw new \InvalidArgumentException("Access secret cannot be empty");
        }
        
        $this->accessKey = $accessKey;
        $this->accessSecret = $accessSecret;
        $this->securityToken = $securityToken;
    }
    
    /**
     * Get access key ID
     * 
     * @return string Access key ID
     */
    public function getAccessKey()
    {
        return $this->accessKey;
    }
    
    /**
     * Get access key secret
     * 
     * @return string Access key secret
     */
    public function getAccessSecret()
    {
        return $this->accessSecret;
    }
    
    /**
     * Get security token
     * 
     * @return string|null Security token, returns null if not set
     */
    public function getSecurityToken()
    {
        return $this->securityToken;
    }
    
    /**
     * Check if security token is set
     * 
     * @return bool Whether security token is set
     */
    public function hasSecurityToken()
    {
        return !empty($this->securityToken);
    }
    
    /**
     * Convert to string representation (hide sensitive information)
     * 
     * @return string Credentials description
     */
    public function __toString()
    {
        $tokenInfo = $this->hasSecurityToken() ? ', hasToken=true' : '';
        return sprintf(
            "Credentials[accessKey=%s%s]",
            substr($this->accessKey, 0, 4) . '***',
            $tokenInfo
        );
    }
}
