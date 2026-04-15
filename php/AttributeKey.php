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
 * AttributeKey - Key for accessing attributes in message interceptor context
 * 
 * Reference: Java AttributeKey class
 * 
 * @template T
 */
class AttributeKey {
    /**
     * @var string Attribute key name
     */
    private $name;
    
    /**
     * Private constructor - use create() factory method
     * 
     * @param string $name Key name
     */
    private function __construct(string $name) {
        $this->name = $name;
    }
    
    /**
     * Create a new AttributeKey
     * 
     * @template V
     * @param string $name Key name
     * @return AttributeKey<V> New attribute key
     */
    public static function create(string $name): AttributeKey {
        return new self($name);
    }
    
    /**
     * Get the key name
     * 
     * @return string Key name
     */
    public function getName(): string {
        return $this->name;
    }
    
    /**
     * Check equality with another AttributeKey
     * 
     * @param AttributeKey $other Other key
     * @return bool True if keys are equal
     */
    public function equals(AttributeKey $other): bool {
        return $this->name === $other->name;
    }
    
    /**
     * Get hash code for the key
     * 
     * @return int Hash code
     */
    public function hashCode(): int {
        return crc32($this->name);
    }
}
