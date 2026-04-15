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
 * MessageInterceptor interface
 * 
 * Reference Java MessageInterceptor design:
 * - Intercepts message processing at various lifecycle points
 * - Supports before/after hooks
 * - Can modify or filter messages
 */
interface MessageInterceptor
{
    /**
     * Called before message is processed
     * 
     * @param MessageInterceptorContext $context Context
     * @param array $messages Messages to process
     * @return void
     */
    public function before(MessageInterceptorContext $context, array $messages): void;
    
    /**
     * Called after message is processed
     * 
     * @param MessageInterceptorContext $context Context
     * @param array $messages Processed messages
     * @return void
     */
    public function after(MessageInterceptorContext $context, array $messages): void;
}
