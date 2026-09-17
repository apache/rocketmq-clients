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
 * GrpcTimeout - gRPC operation timeout values.
 *
 * PHP 8.1 enum replacing ClientConstants::GRPC_* constants.
 * Use toMicroseconds() to get the timeout value in microseconds.
 */
enum GrpcTimeout
{
    case DEFAULT;
    case SEND_MESSAGE;
    case RECEIVE_MESSAGE;
    case ACK_MESSAGE;
    case HEARTBEAT;
    case QUERY_ROUTE;
    case QUERY_ASSIGNMENT;
    case END_TRANSACTION;
    case CHANGE_INVISIBLE;
    case FORWARD_DLQ;
    case RECALL_MESSAGE;
    case SYNC_LITE;

    /**
     * Get the timeout value in microseconds.
     *
     * @return int Timeout in microseconds
     */
    public function toMicroseconds(): int
    {
        return match ($this) {
            self::DEFAULT => 30_000_000,          // 30s
            self::SEND_MESSAGE => 10_000_000,      // 10s
            self::RECEIVE_MESSAGE => 60_000_000,   // 60s (long polling)
            self::ACK_MESSAGE => 5_000_000,        // 5s
            self::HEARTBEAT => 5_000_000,          // 5s
            self::QUERY_ROUTE => 10_000_000,       // 10s
            self::QUERY_ASSIGNMENT => 10_000_000,  // 10s
            self::END_TRANSACTION => 10_000_000,   // 10s
            self::CHANGE_INVISIBLE => 5_000_000,   // 5s
            self::FORWARD_DLQ => 5_000_000,        // 5s
            self::RECALL_MESSAGE => 10_000_000,    // 10s
            self::SYNC_LITE => 10_000_000,         // 10s
        };
    }

    /**
     * Get the timeout value in milliseconds.
     *
     * @return int Timeout in milliseconds
     */
    public function toMilliseconds(): int
    {
        return intdiv($this->toMicroseconds(), 1000);
    }

    /**
     * Get the timeout value in seconds.
     *
     * @return float Timeout in seconds
     */
    public function toSeconds(): float
    {
        return $this->toMicroseconds() / 1_000_000;
    }

    /**
     * Get the timeout as a gRPC metadata value string (microseconds with 'u' suffix).
     *
     * @return string e.g. "10000000u"
     */
    public function toGrpcMetadata(): string
    {
        return $this->toMicroseconds() . 'u';
    }
}
