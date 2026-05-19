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

require_once __DIR__ . '/vendor/autoload.php';

use Apache\Rocketmq\V2\Message;
use Apache\Rocketmq\V2\Endpoints;
use Apache\Rocketmq\V2\Encoding;

/**
 * MessageView - Rich wrapper for consumed messages.
 *
 * Mirrors Java's MessageViewImpl. Wraps the raw protobuf Message received from
 * the broker with additional delivery metadata (receipt handle, endpoints,
 * delivery attempt count, born timestamp, born host) that is needed for
 * ack/nack/retry operations.
 *
 * On construction, verifies body integrity (CRC32) and decompresses GZIP if needed.
 * Marks the message as corrupted if verification or decompression fails.
 */
class MessageView
{
    private $message;
    private $receiptHandle;
    private $endpoints;
    private $deliveryAttempt;
    private $bodyStr;
    private $corrupted = false;
    private $bornTimestamp = 0;
    private $bornHost = '';
    private $decodeTimestamp = 0;

    /**
     * @param Message $message The protobuf message from broker
     * @param string|null $receiptHandle Receipt handle for ack/nack
     * @param Endpoints|null $endpoints Broker endpoints
     * @param int $deliveryAttempt Number of delivery attempts (starts at 1)
     */
    public function __construct(Message $message, $receiptHandle = null, $endpoints = null, $deliveryAttempt = 1)
    {
        $this->message = $message;
        $this->receiptHandle = $receiptHandle;
        $this->endpoints = $endpoints;
        $this->deliveryAttempt = max(1, $deliveryAttempt);
        $this->decodeTimestamp = time();

        // Extract born timestamp and host from system properties
        $sysProps = $message->getSystemProperties();
        if ($sysProps) {
            if (method_exists($sysProps, 'getBornTimestamp')) {
                $bornTs = $sysProps->getBornTimestamp();
                if ($bornTs) {
                    $this->bornTimestamp = method_exists($bornTs, 'getSeconds') ? $bornTs->getSeconds() : 0;
                }
            }
            if (method_exists($sysProps, 'getBornHost')) {
                $this->bornHost = $sysProps->getBornHost() ?? '';
            }

            // Verify body integrity and decompress
            $this->bodyStr = $this->processBody($message, $sysProps);
        }

        if ($this->bodyStr === null) {
            $body = $message->getBody();
            $this->bodyStr = is_string($body) ? $body : (string)$body;
        }
    }

    /**
     * Process body: verify integrity and decompress if needed.
     * Returns the processed body string, or null if processing was skipped.
     */
    private function processBody(Message $message, $sysProps): ?string
    {
        $rawBody = $message->getBody();
        $body = is_string($rawBody) ? $rawBody : (string)$rawBody;

        if ($body === '') {
            return '';
        }

        // Step 1: Decompress if encoding is GZIP
        $encoding = Encoding::IDENTITY;
        if (method_exists($sysProps, 'getBodyEncoding')) {
            $encoding = $sysProps->getBodyEncoding();
        }

        if ($encoding === Encoding::GZIP) {
            $decompressed = @gzdecode($body);
            if ($decompressed === false) {
                $this->corrupted = true;
                Logger::getInstance('MessageView')->warning("Failed to decompress GZIP body for messageId=" . $this->getMessageId());
                return null;
            }
            $body = $decompressed;
        }

        // Step 2: Verify body integrity via digest
        if (method_exists($sysProps, 'getBodyDigest')) {
            $bodyDigest = $sysProps->getBodyDigest();
            if ($bodyDigest !== null && $bodyDigest !== '') {
                // getBodyDigest returns a Digest object, extract type and checksum
                $digestType = is_object($bodyDigest) && method_exists($bodyDigest, 'getType')
                    ? $bodyDigest->getType()
                    : null;
                $digestChecksum = is_object($bodyDigest) && method_exists($bodyDigest, 'getChecksum')
                    ? $bodyDigest->getChecksum()
                    : (string)$bodyDigest;
                if ($digestChecksum !== '' && $digestChecksum !== null) {
                    $this->verifyBodyDigest($body, $digestChecksum, $digestType);
                }
            }
        }

        return $body;
    }

    /**
     * Verify body integrity using the given digest type and checksum.
     */
    private function verifyBodyDigest(string $body, string $checksum, $digestType): void
    {
        $computed = '';
        if ($digestType === \Apache\Rocketmq\V2\DigestType::CRC32) {
            $computed = sprintf('%u', crc32($body));
        } elseif ($digestType === \Apache\Rocketmq\V2\DigestType::MD5) {
            $computed = strtoupper(md5($body));
        } elseif ($digestType === \Apache\Rocketmq\V2\DigestType::SHA1) {
            $computed = strtoupper(sha1($body));
        } else {
            // Unknown digest type, skip verification
            return;
        }

        if ($computed !== $checksum) {
            $this->corrupted = true;
            Logger::getInstance('MessageView')->warning(
                "Body digest mismatch for messageId=" . $this->getMessageId() .
                ", expected=" . $checksum . ", actual=" . $computed
            );
        }
    }

    /**
     * Get the underlying protobuf Message.
     *
     * @return Message
     */
    public function getMessage(): Message
    {
        return $this->message;
    }

    /**
     * Get the topic name.
     *
     * @return string
     */
    public function getTopic(): string
    {
        if ($this->message->hasTopic()) {
            return $this->message->getTopic()->getName();
        }
        return '';
    }

    /**
     * Get the message body as string.
     *
     * @return string
     */
    public function getBody(): string
    {
        return $this->bodyStr ?? '';
    }

    /**
     * Get the message ID from system properties.
     *
     * @return string
     */
    public function getMessageId(): string
    {
        $sysProps = $this->message->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'getMessageId')) {
            return $sysProps->getMessageId() ?? '';
        }
        return '';
    }

    /**
     * Get the tag from system properties.
     *
     * @return string|null
     */
    public function getTag(): ?string
    {
        $sysProps = $this->message->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'hasTag') && $sysProps->hasTag()) {
            return $sysProps->getTag();
        }
        return null;
    }

    /**
     * Get the message keys from system properties.
     *
     * @return array
     */
    public function getKeys(): array
    {
        $sysProps = $this->message->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'getKeys')) {
            $keys = $sysProps->getKeys();
            if ($keys === null) {
                return [];
            }
            if (is_object($keys) && method_exists($keys, 'getIterator')) {
                return iterator_to_array($keys);
            }
            return (array)$keys;
        }
        return [];
    }

    /**
     * Get the message group from system properties.
     *
     * @return string|null
     */
    public function getMessageGroup(): ?string
    {
        $sysProps = $this->message->getSystemProperties();
        if ($sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup()) {
            return $sysProps->getMessageGroup();
        }
        return null;
    }

    /**
     * Get the receipt handle for ack/nack operations.
     *
     * @return string|null
     */
    public function getReceiptHandle(): ?string
    {
        return $this->receiptHandle;
    }

    /**
     * Get the broker endpoints that delivered this message.
     *
     * @return Endpoints|null
     */
    public function getEndpoints(): ?Endpoints
    {
        return $this->endpoints;
    }

    /**
     * Get the number of delivery attempts for this message.
     * Starts at 1 for the first delivery.
     *
     * @return int
     */
    public function getDeliveryAttempt(): int
    {
        return $this->deliveryAttempt;
    }

    /**
     * Get user properties.
     *
     * @return array
     */
    public function getProperties(): array
    {
        $props = $this->message->getUserProperties();
        if ($props === null) {
            return [];
        }
        if (is_object($props) && method_exists($props, 'getIterator')) {
            return iterator_to_array($props);
        }
        return (array)$props;
    }

    /**
     * Get a specific user property.
     *
     * @param string $key
     * @return string|null
     */
    public function getProperty(string $key): ?string
    {
        $props = $this->getProperties();
        return isset($props[$key]) ? $props[$key] : null;
    }

    /**
     * Get system properties object.
     *
     * @return object|null
     */
    public function getSystemProperties()
    {
        return $this->message->getSystemProperties();
    }

    /**
     * Check if this is a FIFO message.
     *
     * @return bool
     */
    public function isFifo(): bool
    {
        return $this->getMessageGroup() !== null;
    }

    /**
     * Whether the message body integrity check or decompression failed.
     * Corrupted messages should not be processed.
     *
     * @return bool
     */
    public function isCorrupted(): bool
    {
        return $this->corrupted;
    }

    /**
     * Get the timestamp when the producer created this message.
     *
     * @return int Unix timestamp (seconds)
     */
    public function getBornTimestamp(): int
    {
        return $this->bornTimestamp;
    }

    /**
     * Get the host that produced this message.
     *
     * @return string
     */
    public function getBornHost(): string
    {
        return $this->bornHost;
    }

    /**
     * Get the timestamp when this message was decoded locally.
     *
     * @return int Unix timestamp (seconds)
     */
    public function getDecodeTimestamp(): int
    {
        return $this->decodeTimestamp;
    }

    /**
     * Increment delivery attempt (used internally for retry tracking).
     */
    public function incrementDeliveryAttempt(): void
    {
        $this->deliveryAttempt++;
    }

    /**
     * Convert to string representation for logging.
     *
     * @return string
     */
    public function __toString(): string
    {
        $parts = [
            'topic=' . $this->getTopic(),
            'messageId=' . $this->getMessageId(),
        ];
        $tag = $this->getTag();
        if ($tag !== null) {
            $parts[] = 'tag=' . $tag;
        }
        $group = $this->getMessageGroup();
        if ($group !== null) {
            $parts[] = 'group=' . $group;
        }
        if ($this->corrupted) {
            $parts[] = 'CORRUPTED';
        }
        return 'MessageView{' . implode(', ', $parts) . '}';
    }
}
