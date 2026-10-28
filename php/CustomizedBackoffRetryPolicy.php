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
 * CustomizedBackoffRetryPolicy - Retry policy with a fixed sequence of delays.
 *
 * Each retry uses the delay from the configured sequence, cycling through 
 * the list until maxAttempts is reached.
 */
class CustomizedBackoffRetryPolicy extends ExponentialBackoffRetryPolicy
{
    private readonly array $delays;
    public function __construct(
        int $maxAttempts,
        array $delays,
    ) {
        $this->delays = $delays;
        parent::__construct($maxAttempts, 0, 0, 1.0);
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
     * Get the delay for the given attempt from the configured sequence.
     * Cycles through the delays if the attempt exceeds the list length.
     *
     * @param int $attempt Current attempt number (1-based)
     * @return int Delay in milliseconds
     */
    public function getNextDelayMs(int $attempt): int
    {
        if ($attempt >= $this->maxAttempts) {
            return 0;
        }

        $count = count($this->delays);
        if ($count === 0) {
            return 0;
        }

        $index = min($attempt - 1, $count - 1);
        return $this->delays[$index];
    }

    /**
     * Get the configured delay durations in milliseconds.
     *
     * @return array
     */
    public function getDurations(): array
    {
        return $this->delays;
    }

    /**
     * Create from protobuf RetryPolicy.
     *
     * @param \Apache\Rocketmq\V2\RetryPolicy $protobuf
     * @return self
     * @throws \InvalidArgumentException if not a customized backoff
     */
    public static function fromProtobuf(V2\RetryPolicy $protobuf): self
    {
        if (!$protobuf->hasCustomizedBackoff()) {
            throw new \InvalidArgumentException(
                "RetryPolicy is not a customized backoff"
            );
        }

        $customizedBackoff = $protobuf->getCustomizedBackoff();
        $delays = [];
        foreach ($customizedBackoff->getNext() as $duration) {
            $delays[] = (int)($duration->getSeconds() * 1000 + intdiv($duration->getNanos(), 1000000));
        }

        return new self($protobuf->getMaxAttempts(), $delays);
    }

    /**
     * Convert to protobuf RetryPolicy.
     *
     * @return \Apache\Rocketmq\V2\RetryPolicy
     */
    public function toProtobuf(): V2\RetryPolicy
    {
        $customizedBackoff = new \Apache\Rocketmq\V2\CustomizedBackoff();
        $nextDurations = [];
        foreach ($this->delays as $delayMs) {
            $duration = new \Google\Protobuf\Duration();
            $duration->setSeconds(intdiv($delayMs, 1000));
            $duration->setNanos(($delayMs % 1000) * 1000000);
            $nextDurations[] = $duration;
        }
        $customizedBackoff->setNext($nextDurations);

        $retryPolicy = new \Apache\Rocketmq\V2\RetryPolicy();
        $retryPolicy->setMaxAttempts($this->maxAttempts);
        $retryPolicy->setCustomizedBackoff($customizedBackoff);

        return $retryPolicy;
    }

    /**
     * Inherit durations from server-side retry policy but keep own maxAttempts.
     *
     * @param \Apache\Rocketmq\V2\RetryPolicy $serverPolicy
     * @return self New policy with inherited durations
     * @throws \InvalidArgumentException if server policy is not customized backoff
     */
    public function inheritBackoff(V2\RetryPolicy $serverPolicy): self
    {
        if (!$serverPolicy->hasCustomizedBackoff()) {
            throw new \InvalidArgumentException(
                "Cannot inherit backoff: server policy is not a customized backoff"
            );
        }

        $serverBackoff = $serverPolicy->getCustomizedBackoff();
        $inheritedDelays = [];
        foreach ($serverBackoff->getNext() as $duration) {
            $inheritedDelays[] = (int)($duration->getSeconds() * 1000 + intdiv($duration->getNanos(), 1000000));
        }

        return new self($this->maxAttempts, $inheritedDelays);
    }
}
