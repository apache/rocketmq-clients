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
 * Interface SystemPropertiesInterface - Contract for message system properties.
 *
 * This interface defines the standard system methods that system properties object.
 * must implement, eliminating the need to implement them in each concrete class.
 */
interface SystemPropertiesInterface
{
    /**
     * Get the message ID
     *
     * @return string Message ID
     */
    public function getMessageId(): string;

    /**
     * Get the receipt handle
     *
     * @return string|null Receipt handle
     */
    public function getReceiptHandle(): ?string;

    /**
     * Set the receipt handle
     *
     * @param string $handle Receipt handle
     * @return void
     */
    public function setReceiptHandle(string $handle): void;

    /**
     * Check if message has a message group (FIFO)
     *
     * @return bool True if has message group.
     */
    public function hasMessageGroup(): bool;

    /**
     * Get the message group.
     *
     * @return string Message group
     */
    public function getMessageGroup(): string;

    /**
     * Check if message has a lite topic.
     * @return bool True if has lite topic.
     */
    public function hasLiteTopic(): bool;

    /**
     * Get the lite topic.
     * @return string Lite topic
     */
    public function getLiteTopic(): string;

    /**
     * Check if message has priority.
     * @return bool True if has priority.
     */
    public function hasPriority(): bool;

    /**
     * Get the priority.
     * @return int Priority
     */
    public function getPriority(): int;

    /**
     * Check if message has delivery timestamp.
     * @return bool True if has delivery timestamp.
     */
    public function hasDeliveryTimestamp(): bool;

    /**
     * Get the delivery timestamp.
     * @return object Delivery timestamp
     */
    public function getDeliveryTimestamp(): object;

    /**
     * Get the body encoding.
     * @return int Body encoding
     */
    public function getBodyEncoding(): int;

    /**
     * Get the body digest.
     * @return object|null Body digest
     */
    public function getBodyDigest(): ?object;

    /**
     * Get the message tag.
     * @return string|null Message tag
     */
    public function getTag(): ?string;

    /**
     * Check if message has tag.
     * @return bool True if has tag.
     */
    public function hasTag(): bool;

    /**
     * Get the message keys.
     * @return array Message keys
     */
    public function getKeys(): array;

    /**
     * Get the trace context.
     * @return string|null Trace context
     */
    public function getTraceContext(): ?string;

    /**
     * Check if message has trace context.
     * @return bool True if has trace context.
     */
    public function hasTraceContext(): bool;

    /**
     * Get the born timestamp.
     * @return object|null Born timestamp
     */
    public function getBornTimestamp(): ?object;

    /**
     * Get the born host.
     * @return string|null Born host
     */
    public function getBornHost(): ?string;
}