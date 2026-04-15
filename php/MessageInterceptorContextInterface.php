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
 * Message Interceptor Context Interface
 * 
 * Reference: Java MessageInterceptorContext
 */
interface MessageInterceptorContextInterface {
    /**
     * Get message hook points
     * 
     * @return int Hook points (use MessageHookPoints constants)
     */
    public function getMessageHookPoints(): int;
    
    /**
     * Get hook points status
     * 
     * @return int Status (use MessageHookPointsStatus constants)
     */
    public function getStatus(): int;
    
    /**
     * Get attribute by key
     * 
     * @template T
     * @param AttributeKey<T> $key Attribute key
     * @return Attribute<T>|null Attribute value or null if not found
     */
    public function getAttribute(AttributeKey $key): ?Attribute;
    
    /**
     * Put attribute
     * 
     * @template T
     * @param AttributeKey<T> $key Attribute key
     * @param Attribute<T> $attribute Attribute value
     * @return void
     */
    public function putAttribute(AttributeKey $key, Attribute $attribute): void;
}
