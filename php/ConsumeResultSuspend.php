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
 * ConsumeResultSuspend - Signals the consumer to retry after a delay.
 * Extends ConsumeResult with a suspend time for server-driven retry delays.
 * Referencing Java ConsumeResultSuspend.java
 */
class ConsumeResultSuspend extends ConsumeResult
{
    const SUSPEND = 2;
    const MIN_SUSPEND_TIME_MS = 50;

    private $suspendTimeMs;

    private function __construct(int $suspendTimeMs)
    {
        if ($suspendTimeMs < self::MIN_SUSPEND_TIME_MS) {
            throw new \InvalidArgumentException(
                "Suspend time must be at least " . self::MIN_SUSPEND_TIME_MS . "ms, got {$suspendTimeMs}ms"
            );
        }
        $this->suspendTimeMs = $suspendTimeMs;
    }

    /**
     * Create a suspend result with the given delay.
     *
     * @param int $suspendTimeMs Duration in milliseconds to suspend before retry
     * @return ConsumeResultSuspend
     */
    public static function of(int $suspendTimeMs): ConsumeResultSuspend
    {
        return new self($suspendTimeMs);
    }

    /**
     * Get the suspend time in milliseconds.
     */
    public function getSuspendTimeMs(): int
    {
        return $this->suspendTimeMs;
    }

    /**
     * Get the suspend time as a named constant (SUSPEND=2).
     */
    public function getValue(): int
    {
        return self::SUSPEND;
    }
}
