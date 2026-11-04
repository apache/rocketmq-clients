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
 * Interface RetryPolicyInterface - Contract for retry policy.
 *
 * The interface defines the standard methods that retry policy objects
 * must implement, eliminating the need for method_exists() checks.
 */
interface RetryPolicyInterface
{
    /**
     * Get the maximum number of retry attempts.
     * @return int Maximum retry attempts.
     */
    public function getMaxAttempts(): int;

    /**
     * Get the delay for the next retry attempt in milliseconds.
     *
     * @param int $attempt Current attempt number.(1-based)
     * @return int Delay in milliseconds.
     */
    public function getNextAttemptDelayMs(int $attempt): int;

    /**
     * Get the next delay with jitter in milliseconds.
     * @param int $attempt Current attempt number.(1-based)
     * @return int Delay with jitter in milliseconds.
     */
    public function getNextDelayWithJitterMs(int $attempt): int;

}
