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

use Apache\Rocketmq\Message\MessageInterface;
use Apache\Rocketmq\Logger;

/**
 * Composited Message Interceptor - Manages a chain of interceptors
 * 
 * Executes interceptors in order before processing, and in reverse order after processing.
 * This ensures proper attribute propagation and error handling.
 * 
 * Reference: Java CompositedMessageInterceptor
 */
class CompositedMessageInterceptor implements MessageInterceptor {
    /**
     * @var AttributeKey Key for storing interceptor attributes
     */
    private static $INTERCEPTOR_ATTRIBUTES_KEY;
    
    /**
     * @var MessageInterceptor[] List of interceptors
     */
    private $interceptors = [];
    
    /**
     * Initialize static properties
     */
    public function __construct() {
        if (self::$INTERCEPTOR_ATTRIBUTES_KEY === null) {
            self::$INTERCEPTOR_ATTRIBUTES_KEY = AttributeKey::create('composited_interceptor_attributes');
        }
    }
    
    /**
     * Add an interceptor to the chain
     * 
     * @param MessageInterceptor $interceptor Interceptor to add
     * @return void
     */
    public function addInterceptor(MessageInterceptor $interceptor): void {
        $this->interceptors[] = $interceptor;
        Logger::debug("Added message interceptor: {}", [get_class($interceptor)]);
    }
    
    /**
     * Remove an interceptor from the chain
     * 
     * @param MessageInterceptor $interceptor Interceptor to remove
     * @return bool True if removed successfully
     */
    public function removeInterceptor(MessageInterceptor $interceptor): bool {
        $index = array_search($interceptor, $this->interceptors, true);
        if ($index !== false) {
            unset($this->interceptors[$index]);
            $this->interceptors = array_values($this->interceptors); // Re-index
            Logger::debug("Removed message interceptor: {}", [get_class($interceptor)]);
            return true;
        }
        return false;
    }
    
    /**
     * Clear all interceptors
     * 
     * @return void
     */
    public function clearInterceptors(): void {
        $this->interceptors = [];
        Logger::debug("Cleared all message interceptors");
    }
    
    /**
     * Get interceptor count
     * 
     * @return int Number of interceptors
     */
    public function getInterceptorCount(): int {
        return count($this->interceptors);
    }
    
    /**
     * {@inheritdoc}
     * Execute all interceptors before message processing
     */
    public function doBefore(MessageInterceptorContextInterface $context0, array $messages): void {
        if (empty($this->interceptors)) {
            return;
        }
        
        $attributeMap = [];
        $messageHookPoints = $context0->getMessageHookPoints();
        $status = $context0->getStatus();
        
        // Create new context for interceptor chain
        $context = new MessageInterceptorContextImpl($messageHookPoints, $status);
        
        // Copy attributes from original context if it's an implementation
        if ($context0 instanceof MessageInterceptorContextImpl) {
            foreach ($context0->getAttributes() as $item) {
                // $item is ['key' => AttributeKey, 'attribute' => Attribute]
                if (isset($item['key']) && isset($item['attribute'])) {
                    $context->putAttribute($item['key'], $item['attribute']);
                }
            }
        }
        
        // Execute each interceptor in order
        foreach ($this->interceptors as $index => $interceptor) {
            try {
                Logger::debug("Executing interceptor before processing: {} [index={}]", [
                    get_class($interceptor),
                    $index
                ]);
                
                $interceptor->doBefore($context, $messages);
                
                // Store attributes from this interceptor
                $attributeMap[$index] = $context->getAttributes();
                
            } catch (\Throwable $t) {
                Logger::error("Exception raised while executing interceptor before processing: {}, error={}", [
                    get_class($interceptor),
                    $t->getMessage()
                ]);
            }
        }
        
        // Store all interceptor attributes in the original context
        $context0->putAttribute(
            self::$INTERCEPTOR_ATTRIBUTES_KEY,
            Attribute::create($attributeMap)
        );
    }
    
    /**
     * {@inheritdoc}
     * Execute all interceptors after message processing (in reverse order)
     */
    public function doAfter(MessageInterceptorContextInterface $context0, array $messages): void {
        if (empty($this->interceptors)) {
            return;
        }
        
        // Get stored interceptor attributes
        $attributesAttr = $context0->getAttribute(self::$INTERCEPTOR_ATTRIBUTES_KEY);
        $attributeMap = $attributesAttr ? $attributesAttr->get() : [];
        
        // Execute each interceptor in reverse order
        for ($index = count($this->interceptors) - 1; $index >= 0; $index--) {
            $interceptor = $this->interceptors[$index];
            
            // Get attributes for this interceptor
            $attributes = isset($attributeMap[$index]) ? $attributeMap[$index] : [];
            
            $messageHookPoints = $context0->getMessageHookPoints();
            $status = $context0->getStatus();
            
            // Create new context with interceptor-specific attributes
            $context = new MessageInterceptorContextImpl($messageHookPoints, $status, $attributes);
            
            // Copy attributes from original context if it's an implementation
            if ($context0 instanceof MessageInterceptorContextImpl) {
                foreach ($context0->getAttributes() as $item) {
                    // $item is ['key' => AttributeKey, 'attribute' => Attribute]
                    if (isset($item['key']) && isset($item['attribute'])) {
                        $context->putAttribute($item['key'], $item['attribute']);
                    }
                }
            }
            
            try {
                Logger::debug("Executing interceptor after processing: {} [index={}]", [
                    get_class($interceptor),
                    $index
                ]);
                
                $interceptor->doAfter($context, $messages);
                
            } catch (\Throwable $t) {
                Logger::error("Exception raised while executing interceptor after processing: {}, error={}", [
                    get_class($interceptor),
                    $t->getMessage()
                ]);
            }
        }
    }
}
