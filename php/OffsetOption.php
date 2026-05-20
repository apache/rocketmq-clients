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
 * OffsetOption - Specifies where to start consuming from.
 * Referencing Java OffsetOption.java
 */
class OffsetOption
{
    const TYPE_POLICY = 'POLICY';
    const TYPE_OFFSET = 'OFFSET';
    const TYPE_TAIL_N = 'TAIL_N';
    const TYPE_TIMESTAMP = 'TIMESTAMP';

    const POLICY_LAST_VALUE = 0;
    const POLICY_MIN_VALUE = 1;
    const POLICY_MAX_VALUE = 2;

    private $type;
    private $value;

    private function __construct(string $type, int $value)
    {
        $this->type = $type;
        $this->value = $value;
    }

    /**
     * Resume from last consumed position.
     */
    public static function lastOffset(): OffsetOption
    {
        return new self(self::TYPE_POLICY, self::POLICY_LAST_VALUE);
    }

    /**
     * Start from earliest offset.
     */
    public static function minOffset(): OffsetOption
    {
        return new self(self::TYPE_POLICY, self::POLICY_MIN_VALUE);
    }

    /**
     * Start from latest offset.
     */
    public static function maxOffset(): OffsetOption
    {
        return new self(self::TYPE_POLICY, self::POLICY_MAX_VALUE);
    }

    /**
     * Start from a specific offset.
     *
     * @param int $offset Must be >= 0
     */
    public static function ofOffset(int $offset): OffsetOption
    {
        if ($offset < 0) {
            throw new \InvalidArgumentException("Offset must be >= 0");
        }
        return new self(self::TYPE_OFFSET, $offset);
    }

    /**
     * Start from N messages from tail.
     *
     * @param int $n Must be >= 0
     */
    public static function ofTailN(int $n): OffsetOption
    {
        if ($n < 0) {
            throw new \InvalidArgumentException("Tail N must be >= 0");
        }
        return new self(self::TYPE_TAIL_N, $n);
    }

    /**
     * Start from messages at/after a timestamp.
     *
     * @param int $timestamp Unix timestamp in seconds, must be >= 0
     */
    public static function ofTimestamp(int $timestamp): OffsetOption
    {
        if ($timestamp < 0) {
            throw new \InvalidArgumentException("Timestamp must be >= 0");
        }
        return new self(self::TYPE_TIMESTAMP, $timestamp);
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getValue(): int
    {
        return $this->value;
    }

    public function isPolicy(): bool
    {
        return $this->type === self::TYPE_POLICY;
    }

    public function isOffset(): bool
    {
        return $this->type === self::TYPE_OFFSET;
    }

    public function isTailN(): bool
    {
        return $this->type === self::TYPE_TAIL_N;
    }

    public function isTimestamp(): bool
    {
        return $this->type === self::TYPE_TIMESTAMP;
    }
}
