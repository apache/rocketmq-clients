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
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Resource;
use Google\Protobuf\Timestamp;

/**
 * MessageBuilder - Fluent builder for creating messages.
 *
 * Provides a fluent API with validation for building messages.
 * Prevents conflicting message types (delay, fifo, lite, priority are mutually exclusive).
 */
class MessageBuilder
{
    const TOPIC_PATTERN = '/^[%a-zA-Z0-9_-]+$/';

    private ?string $topic = null;
    private ?string $body = null;
    private ?string $tag = null;
    private array $keys = [];
    private ?string $messageGroup = null;
    private ?Timestamp $deliveryTimestamp = null;
    private ?string $liteTopic = null;
    private ?int $priority = null;
    private array $properties = [];
    private ?string $encoding = null;

    /**
     * Set the topic (required).
     *
     * @param string $topic Topic name
     * @return $this
     */
    public function setTopic(string $topic): self
    {
        if (empty(trim($topic))) {
            throw new \InvalidArgumentException("Topic name cannot be empty");
        }
        if (!preg_match(self::TOPIC_PATTERN, $topic)) {
            throw new \InvalidArgumentException(
                "Topic name contains invalid characters. Allowed: letters, digits, %, _, -"
            );
        }
        $this->topic = $topic;
        return $this;
    }

    /**
     * Set the message body (required).
     *
     * @param string $body Message body content
     * @return $this
     */
    public function setBody(string $body): self
    {
        $this->body = $body;
        return $this;
    }

    /**
     * Set the message tag (optional).
     *
     * @param string|null $tag Tag value
     * @return $this
     */
    public function setTag(?string $tag): self
    {
        if ($tag !== null) {
            $this->validateTag($tag);
        }
        $this->tag = $tag;
        return $this;
    }

    /**
     * Set the message keys (optional).
     *
     * @param array $keys List of key strings
     * @return $this
     */
    public function setKeys(array $keys): self
    {
        foreach ($keys as $key) {
            $this->validateKey($key);
        }
        $this->keys = $keys;
        return $this;
    }

    /**
     * Add a single key.
     *
     * @param string $key
     * @return $this
     */
    public function addKey(string $key): self
    {
        $this->validateKey($key);
        $this->keys[] = $key;
        return $this;
    }

    /**
     * Set the message group for FIFO messages (optional).
     * Mutually exclusive with deliveryTimestamp, liteTopic, and priority.
     *
     * @param string|null $messageGroup
     * @return $this
     */
    public function setMessageGroup(?string $messageGroup): self
    {
        if ($messageGroup !== null) {
            $this->validateNoConflict('messageGroup');
        }
        $this->messageGroup = $messageGroup;
        return $this;
    }

    /**
     * Set the delivery timestamp for delayed/scheduled messages (optional).
     * Mutually exclusive with messageGroup, liteTopic, and priority.
     *
     * @param int|null $timestampMs Unix timestamp in milliseconds
     * @return $this
     */
    public function setDeliveryTimestamp(?int $timestampMs): self
    {
        if ($timestampMs !== null) {
            $this->validateNoConflict('deliveryTimestamp');
            $ts = new Timestamp();
            $ts->setSeconds(intdiv($timestampMs, 1000));
            $ts->setNanos(($timestampMs % 1000) * 1000000);
            $this->deliveryTimestamp = $ts;
        } else {
            $this->deliveryTimestamp = null;
        }
        return $this;
    }

    /**
     * Set the lite topic sub-classifier (optional).
     * Mutually exclusive with messageGroup, deliveryTimestamp, and priority.
     *
     * @param string|null $liteTopic
     * @return $this
     */
    public function setLiteTopic(?string $liteTopic): self
    {
        if ($liteTopic !== null) {
            if (trim($liteTopic) === '') {
                throw new \InvalidArgumentException("LiteTopic cannot be blank");
            }
            $this->validateNoConflict('liteTopic');
        }
        $this->liteTopic = $liteTopic;
        return $this;
    }

    /**
     * Set the message priority level (optional).
     * Mutually exclusive with messageGroup, deliveryTimestamp, and liteTopic.
     *
     * @param int|null $priority Priority value (lower = higher priority)
     * @return $this
     */
    public function setPriority(?int $priority): self
    {
        if ($priority !== null) {
            if ($priority < 1 || $priority > 9) {
                throw new \InvalidArgumentException("Priority must be between 1 and 9");
            }
            $this->validateNoConflict('priority');
        }
        $this->priority = $priority;
        return $this;
    }

    /**
     * Add a user property.
     *
     * @param string $key Property key
     * @param string $value Property value
     * @return $this
     */
    public function addProperty(string $key, string $value)
    {
        $this->properties[$key] = $value;
        return $this;
    }

    /**
     * Set the body encoding for compression.
     *
     * When set to GZIP, ZLIB, ZSTD, or LZ4, the body will be compressed
     * during build() and the encoding flag will be set in system properties.
     *
     * @param string|null $encoding One of Utilities::ENCODING_GZIP_STR, ENCODING_ZLIB_STR, etc.
     * @return $this
     */
    public function setEncoding(?string $encoding)
    {
        $this->encoding = $encoding;
        return $this;
    }

    /**
     * Build the Message protobuf object.
     *
     * @return Message Protobuf message
     * @throws \InvalidArgumentException if topic or body is missing
     */
    public function build()
    {
        if ($this->topic === null) {
            throw new \InvalidArgumentException("Message topic is required");
        }
        if ($this->body === null) {
            throw new \InvalidArgumentException("Message body is required");
        }

        $topicResource = new Resource();
        $topicResource->setName($this->topic);

        $message = new Message();
        $message->setTopic($topicResource);

        // Compress body if encoding is set
        $body = $this->body;
        if ($this->encoding !== null && $this->encoding !== Utilities::ENCODING_IDENTITY_STR) {
            $body = Utilities::compressBytes($this->body, $this->encoding);
        }
        $message->setBody($body);

        // Build system properties only if any are set
        if ($this->hasSystemProperties()) {
            $sysProps = new SystemProperties();

            if ($this->tag !== null) {
                $sysProps->setTag($this->tag);
            }
            if (!empty($this->keys)) {
                $sysProps->setKeys($this->keys);
            }
            if ($this->messageGroup !== null) {
                $sysProps->setMessageGroup($this->messageGroup);
            }
            if ($this->deliveryTimestamp !== null) {
                $sysProps->setDeliveryTimestamp($this->deliveryTimestamp);
            }
            if ($this->liteTopic !== null) {
                $sysProps->setLiteTopic($this->liteTopic);
            }
            if ($this->priority !== null) {
                $sysProps->setPriority($this->priority);
            }

            $message->setSystemProperties($sysProps);
        }

        // Set user properties
        foreach ($this->properties as $key => $value) {
            $message->getUserProperties()[$key] = $value;
        }

        return $message;
    }

    /**
     * Check if any system properties are configured.
     */
    private function hasSystemProperties(): bool
    {
        return $this->tag !== null
            || !empty($this->keys)
            || $this->messageGroup !== null
            || $this->deliveryTimestamp !== null
            || $this->liteTopic !== null
            || $this->priority !== null
            || $this->encoding !== null;
    }

    /**
     * Validate that the new message type does not conflict with existing ones.
     */
    private function validateNoConflict(string $newType)
    {
        $conflicts = [];

        if ($newType === 'messageGroup') {
            if ($this->deliveryTimestamp !== null) $conflicts[] = 'deliveryTimestamp';
            if ($this->liteTopic !== null) $conflicts[] = 'liteTopic';
            if ($this->priority !== null) $conflicts[] = 'priority';
        } elseif ($newType === 'deliveryTimestamp') {
            if ($this->messageGroup !== null) $conflicts[] = 'messageGroup';
            if ($this->liteTopic !== null) $conflicts[] = 'liteTopic';
            if ($this->priority !== null) $conflicts[] = 'priority';
        } elseif ($newType === 'liteTopic') {
            if ($this->messageGroup !== null) $conflicts[] = 'messageGroup';
            if ($this->deliveryTimestamp !== null) $conflicts[] = 'deliveryTimestamp';
            if ($this->priority !== null) $conflicts[] = 'priority';
        } elseif ($newType === 'priority') {
            if ($this->messageGroup !== null) $conflicts[] = 'messageGroup';
            if ($this->deliveryTimestamp !== null) $conflicts[] = 'deliveryTimestamp';
            if ($this->liteTopic !== null) $conflicts[] = 'liteTopic';
        }

        if (!empty($conflicts)) {
            throw new \InvalidArgumentException(
                "Message type conflict: '{$newType}' conflicts with [" . implode(', ', $conflicts) . "]. " .
                "Delay, FIFO, Lite, and Priority are mutually exclusive."
            );
        }
    }

    /**
     * Validate tag does not contain vertical bar or whitespace.
     */
    private function validateTag(string $tag)
    {
        if (strpos($tag, '|') !== false) {
            throw new \InvalidArgumentException("Tag cannot contain vertical bar '|'");
        }
        if (preg_match('/\s/', $tag)) {
            throw new \InvalidArgumentException("Tag cannot contain whitespace characters");
        }
    }

    /**
     * Validate key is not blank.
     */
    private function validateKey(string $key)
    {
        if (trim($key) === '') {
            throw new \InvalidArgumentException("Message key cannot be blank");
        }
    }
}
