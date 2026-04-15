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
 * Message Interceptor Context Implementation
 * 
 * Reference: Java MessageInterceptorContextImpl
 */
class MessageInterceptorContextImpl implements MessageInterceptorContextInterface {
    /**
     * @var int Message hook points
     */
    private $messageHookPoints;
    
    /**
     * @var int Hook points status
     */
    private $status;
    
    /**
     * @var array Map of AttributeKey => Attribute
     */
    private $attributes = [];
    
    /**
     * Constructor
     * 
     * @param int $messageHookPoints Message hook points
     * @param int $status Hook points status
     * @param array $attributes Initial attributes (optional)
     */
    public function __construct(int $messageHookPoints, int $status, array $attributes = []) {
        $this->messageHookPoints = $messageHookPoints;
        $this->status = $status;
        $this->attributes = $attributes;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageHookPoints(): int {
        return $this->messageHookPoints;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getStatus(): int {
        return $this->status;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getAttribute(AttributeKey $key): ?Attribute {
        foreach ($this->attributes as $item) {
            // $item is ['key' => AttributeKey, 'attribute' => Attribute]
            if (isset($item['key']) && $item['key']->equals($key)) {
                return $item['attribute'];
            }
        }
        return null;
    }
    
    /**
     * {@inheritdoc}
     */
    public function putAttribute(AttributeKey $key, Attribute $attribute): void {
        // Use key name as array index to avoid "Illegal offset type" error
        $this->attributes[$key->getName()] = [
            'key' => $key,
            'attribute' => $attribute
        ];
    }
    
    /**
     * Get all attributes
     * 
     * @return array List of ['key' => AttributeKey, 'attribute' => Attribute]
     */
    public function getAttributes(): array {
        // Return as list to avoid using objects as array keys
        return array_values($this->attributes);
    }
    
    /**
     * Set status
     * 
     * @param int $status New status
     * @return void
     */
    public function setStatus(int $status): void {
        $this->status = $status;
    }
}
