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
 * SessionCredentials - Holds AK/SK authentication credentials.
 */
class SessionCredentials
{
    private $accessKey;
    private $accessSecret;
    private $securityToken; // Optional STS token

    /**
     * Constructor.
     *
     * @param string $accessKey Access key for authentication
     * @param string $accessSecret Access secret for HMAC signing
     * @param string|null $securityToken Optional STS security token
     * @throws \InvalidArgumentException If accessKey or accessSecret is empty
     */
    public function __construct(string $accessKey, string $accessSecret, ?string $securityToken = null)
    {
        if (empty($accessKey)) {
            throw new \InvalidArgumentException("AccessKey cannot be empty");
        }
        if (empty($accessSecret)) {
            throw new \InvalidArgumentException("AccessSecret cannot be empty");
        }
        $this->accessKey = $accessKey;
        $this->accessSecret = $accessSecret;
        $this->securityToken = $securityToken;
    }

    /**
     * Get the access key.
     *
     * @return string The access key
     */
    public function getAccessKey(): string
    {
        return $this->accessKey;
    }

    /**
     * Get the access secret.
     *
     * @return string The access secret
     */
    public function getAccessSecret(): string
    {
        return $this->accessSecret;
    }

    /**
     * Get the optional STS security token.
     *
     * @return string|null The security token, or null if not set
     */
    public function getSecurityToken(): ?string
    {
        return $this->securityToken;
    }
}
