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

use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\MessageType as V2MessageType;

/**
 * MessageValidator - Message validation and type detection.
 *
 * Extracted from Producer to separate validation concerns:
 * 1. validateMessage() - validates topic, body, and body size
 * 2. detectMessageType() - determines NORMAL/FIFO/DELAY/PRIORITY/TRANSACTION/LITE
 * 3. Configurable max body size and message type validation (updated by server settings)
 */
class MessageValidator
{
    private int $maxBodySizeBytes;
    private bool $validateMessageType;

    public function __construct(int $maxBodySizeBytes = 4194304, bool $validateMessageType = true)
    {
        $this->maxBodySizeBytes = $maxBodySizeBytes;
        $this->validateMessageType = $validateMessageType;
    }

    /**
     * Validate a message before sending.
     *
     * @param Message $message The message to validate
     * @throws \InvalidArgumentException If validation fails
     */
    public function validateMessage(Message $message): void
    {
        if (!$message->hasTopic() || empty(trim($message->getTopic()->getName()))) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        if (empty($message->getBody())) {
            throw new \InvalidArgumentException("Message body is required");
        }
        if (strlen($message->getBody()) > $this->maxBodySizeBytes) {
            $mb = $this->maxBodySizeBytes / (1024 * 1024);
            throw new \InvalidArgumentException("Message size exceeds limit ({$mb}MB)");
        }
    }

    /**
     * Detect the message type based on system properties.
     *
     * @param Message $msg The message to inspect
     * @param bool $txEnabled Whether transaction message type should be considered
     * @return int MessageType constant (NORMAL, FIFO, DELAY, PRIORITY, TRANSACTION, LITE)
     */
    public function detectMessageType(Message $msg, bool $txEnabled = false): int
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps !== null && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps !== null && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps !== null && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps !== null && $sysProps->hasDeliveryTimestamp();

        return match (true) {
            $txEnabled && !$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp
                => V2MessageType::TRANSACTION,
            !$txEnabled && $hasMessageGroup => V2MessageType::FIFO,
            !$txEnabled && $hasLiteTopic => V2MessageType::LITE,
            !$txEnabled && $hasDeliveryTimestamp => V2MessageType::DELAY,
            !$txEnabled && $hasPriority => V2MessageType::PRIORITY,
            default => V2MessageType::NORMAL,
        };
    }

    /**
     * Check if message type validation against route is enabled.
     *
     * @return bool
     */
    public function isValidateMessageType(): bool
    {
        return $this->validateMessageType;
    }

    /**
     * Enable or disable message type validation (updated by server settings).
     *
     * @param bool $validateMessageType
     */
    public function setValidateMessageType(bool $validateMessageType): void
    {
        $this->validateMessageType = $validateMessageType;
    }

    /**
     * Get the maximum allowed message body size in bytes.
     *
     * @return int
     */
    public function getMaxBodySizeBytes(): int
    {
        return $this->maxBodySizeBytes;
    }

    /**
     * Set the maximum allowed message body size in bytes (updated by server settings).
     *
     * @param int $maxBodySizeBytes
     */
    public function setMaxBodySizeBytes(int $maxBodySizeBytes): void
    {
        $this->maxBodySizeBytes = $maxBodySizeBytes;
    }
}
