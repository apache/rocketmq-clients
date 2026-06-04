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
 * Interface MessageViewInterface - Controls for message view objects.
 *
 * This interface defines the standard methods that message view objects
 * must implement, eliminating the need for method_exists() checks.
 */
interface MessageViewInterface
{
    /**
     * Get the system properties of the message.
     *
     * @return object|null System properties object
     */
    public function getSystemProperties(): ?object;

    /**
     * Get the message ID.
     *
     * @return string Message ID
     */
    public function getMessageId(): string;

    /**
     * Get the message body.
     *
     * @return string Topic resource string
     */
    public function getTopic(): string;

    /**
     * Get the delivery attempt count.
     *
     * @return int Current delivery attempt number.
     */
    public function getDeliveryAttempt(): int;

    /**
     * Increment the delivery attempt count.
     *
     * @return void
     */
    public function incrementDeliveryAttempt(): void;

    /**
     * Check if the message is corrupted.
     *
     * @return bool True if message is corrupted
     */
    public function isCorrupted(): bool;

    /**
     * Get the broker endpoint for this message.
     *
     * @return object|null Endpoint resource object
     */
    public function getEndpoints(): ?object;
}
