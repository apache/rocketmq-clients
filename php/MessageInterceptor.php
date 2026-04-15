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

/**
 * Message Interceptor Interface
 * 
 * Allows intercepting messages before and after they are processed.
 * Interceptors are executed in a chain, with doBefore called in order
 * and doAfter called in reverse order.
 * 
 * Reference: Java MessageInterceptor
 */
interface MessageInterceptor {
    /**
     * Called before message processing (send or consume)
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param MessageInterface[] $messages Messages being processed
     * @return void
     * @throws \Exception If interception fails
     */
    public function doBefore(MessageInterceptorContextInterface $context, array $messages): void;
    
    /**
     * Called after message processing (send or consume)
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param MessageInterface[] $messages Messages that were processed
     * @return void
     */
    public function doAfter(MessageInterceptorContextInterface $context, array $messages): void;
}
