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
 * Retry policy interface
 * 
 * Defines retry behavior for message sending and consumption
 */
interface RetryPolicy
{
    /**
     * Get maximum retry attempts
     * 
     * @return int Maximum retry attempts
     */
    public function getMaxAttempts();
    
    /**
     * Get delay time for next retry (milliseconds)
     * 
     * @param int $attempt Current attempt count (starting from 1)
     * @return int Delay time (milliseconds)
     */
    public function getNextAttemptDelay($attempt);
    
    /**
     * Determine whether to retry
     * 
     * @param int $attempt Current attempt count
     * @param \Exception $exception Exception object
     * @return bool Whether should retry
     */
    public function shouldRetry($attempt, $exception);
}
