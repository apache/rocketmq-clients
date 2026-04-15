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

namespace Apache\Rocketmq\Consumer;

/**
 * MessageInterceptorContext - Context for message interceptor
 * 
 * Holds metadata about the current message processing phase
 */
class MessageInterceptorContext
{
    /**
     * @var string Current phase (RECEIVE, CONSUME, ACK, FORWARD_TO_DLQ, etc.)
     */
    private $phase;
    
    /**
     * @var string Status (OK, ERROR, etc.)
     */
    private $status;
    
    /**
     * @var array Additional attributes
     */
    private $attributes = [];
    
    /**
     * Constructor
     * 
     * @param string $phase Current phase
     * @param string $status Status
     */
    public function __construct(string $phase, string $status = 'OK')
    {
        $this->phase = $phase;
        $this->status = $status;
    }
    
    /**
     * Get phase
     * 
     * @return string
     */
    public function getPhase(): string
    {
        return $this->phase;
    }
    
    /**
     * Get status
     * 
     * @return string
     */
    public function getStatus(): string
    {
        return $this->status;
    }
    
    /**
     * Set status
     * 
     * @param string $status Status
     * @return void
     */
    public function setStatus(string $status): void
    {
        $this->status = $status;
    }
    
    /**
     * Get attribute
     * 
     * @param string $key Attribute key
     * @param mixed $default Default value
     * @return mixed
     */
    public function getAttribute(string $key, $default = null)
    {
        return $this->attributes[$key] ?? $default;
    }
    
    /**
     * Set attribute
     * 
     * @param string $key Attribute key
     * @param mixed $value Attribute value
     * @return void
     */
    public function setAttribute(string $key, $value): void
    {
        $this->attributes[$key] = $value;
    }
    
    /**
     * Get all attributes
     * 
     * @return array
     */
    public function getAttributes(): array
    {
        return $this->attributes;
    }
}
