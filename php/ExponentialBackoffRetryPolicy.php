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
 * ExponentialBackoffRetryPolicy - Retry policy with exponential backoff.
 *
 * Computes the next retry delay based on the current attempt number with 
 * the formula:
 *   delay = min(base * multiplier^(attempt-1), maxDelay)
 *
 * With jitter to avoid thundering herd:
 *   delayWithJitter = delay * (0.5 + random * 0.5)
 */
class ExponentialBackoffRetryPolicy
{
    private $maxAttempts;
    private $baseDelayMs;
    private $maxDelayMs;
    private $multiplier;

    /**
     * Construct a new ExponentialBackoffRetryPolicy.
     *
     * @param int $maxAttempts Maximum retry attempts (>= 1)
     * @param int $baseDelayMs Base delay in milliseconds
     * @param int $maxDelayMs Maximum delay cap in milliseconds
     * @param float $multiplier Multiplier for each subsequent attempt
     * @throws \InvalidArgumentException if maxAttempts < 1
     */
    public function __construct($maxAttempts = 3, $baseDelayMs = 1000, $maxDelayMs = 30000, $multiplier = 2.0)
    {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException("maxAttempts must be >= 1");
        }
        $this->maxAttempts = $maxAttempts;
        $this->baseDelayMs = max(0, $baseDelayMs);
        $this->maxDelayMs = max($baseDelayMs, $maxDelayMs);
        $this->multiplier = max(1.0, $multiplier);
    }

    /**
     * Get the maximum number of attempts.
     *
     * @return int
     */
    public function getMaxAttempts(): int
    {
        return $this->maxAttempts;
    }

    /**
     * Compute the next delay in milliseconds for the given attempt.
     *
     * @param int $attempt Current attempt number (1-based)
     * @return int Delay in milliseconds
     */
    public function getNextDelayMs($attempt): int
    {
        if ($attempt >= $this->maxAttempts) {
            return 0;
        }

        $delay = $this->baseDelayMs * pow($this->multiplier, $attempt - 1);
        return min((int)$delay, $this->maxDelayMs);
    }

    /**
     * Compute the next delay with jitter to avoid thundering herd.
     * Jitter adds random factor: delay * (0.5 + rand(0,1) * 0.5)
     *
     * @param int $attempt Current attempt number (1-based)
     * @return int Delay in milliseconds with jitter
     */
    public function getNextDelayWithJitterMs($attempt): int
    {
        $delay = $this->getNextDelayMs($attempt);
        if ($delay <= 0) {
            return 0;
        }

        $jitter = 0.5 + (mt_rand() / mt_getrandmax()) * 0.5;
        return (int)($delay * $jitter);
    }
}
