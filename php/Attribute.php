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
 * Attribute - Type-safe container for context attributes
 * 
 * Reference: Java Attribute class
 * 
 * @template T
 */
class Attribute {
    /**
     * @var mixed The attribute value
     */
    private $value;
    
    /**
     * Constructor
     * 
     * @param mixed $value Attribute value
     */
    public function __construct($value) {
        $this->value = $value;
    }
    
    /**
     * Get the attribute value
     * 
     * @return mixed The attribute value
     */
    public function get() {
        return $this->value;
    }
    
    /**
     * Set the attribute value
     * 
     * @param mixed $value New value
     * @return void
     */
    public function set($value): void {
        $this->value = $value;
    }
    
    /**
     * Create a new Attribute instance
     * 
     * @template V
     * @param V $value Attribute value
     * @return Attribute<V> New attribute instance
     */
    public static function create($value): Attribute {
        return new self($value);
    }
}
