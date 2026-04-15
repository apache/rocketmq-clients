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
 * ConsumeService interface for message consumption
 * 
 * Reference Java ConsumeService design to provide different consumption strategies
 */
interface ConsumeService
{
    /**
     * Consume a message
     * 
     * @param mixed $message Message to consume
     * @return ConsumeResult Consumption result
     */
    public function consume($message): ConsumeResult;
    
    /**
     * Start the consume service
     * 
     * @return void
     */
    public function start(): void;
    
    /**
     * Shutdown the consume service
     * 
     * @return void
     */
    public function shutdown(): void;
}
