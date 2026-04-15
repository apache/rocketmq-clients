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

namespace Apache\Rocketmq\Message;

use Apache\Rocketmq\Route\Endpoints;
use Apache\Rocketmq\Route\MessageQueue;
use Apache\Rocketmq\Util;

/**
 * MessageViewImpl provides a complete implementation of MessageView
 * 
 * References Java MessageViewImpl implementation (348 lines)
 * 
 * Key features:
 * - Complete message data from Protobuf
 * - Body digest verification (CRC32/MD5/SHA1)
 * - Body decompression (GZIP/IDENTITY)
 * - Receipt handle management
 * - Delivery attempt tracking
 * - Message queue and endpoints association
 */
class MessageViewImpl implements MessageView
{
    /**
     * @var MessageId Message ID
     */
    private $messageId;
    
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var string Message body
     */
    private $body;
    
    /**
     * @var string|null Message tag
     */
    private $tag;
    
    /**
     * @var string|null Message group (for FIFO)
     */
    private $messageGroup;
    
    /**
     * @var string|null Lite topic (for LITE type)
     */
    private $liteTopic;
    
    /**
     * @var int|null Delivery timestamp (for delay messages)
     */
    private $deliveryTimestamp;
    
    /**
     * @var int|null Message priority
     */
    private $priority;
    
    /**
     * @var string[] Message keys
     */
    private $keys = [];
    
    /**
     * @var array User properties
     */
    private $properties = [];
    
    /**
     * @var string Born host
     */
    private $bornHost;
    
    /**
     * @var int Born timestamp
     */
    private $bornTimestamp;
    
    /**
     * @var int Delivery attempt count
     */
    private $deliveryAttempt;
    
    /**
     * @var MessageQueue|null Message queue
     */
    private $messageQueue;
    
    /**
     * @var Endpoints|null Broker endpoints
     */
    private $endpoints;
    
    /**
     * @var string Receipt handle
     */
    private $receiptHandle;
    
    /**
     * @var int Queue offset
     */
    private $offset;
    
    /**
     * @var bool Whether message is corrupted
     */
    private $corrupted;
    
    /**
     * @var int Decode timestamp
     */
    private $decodeTimestamp;
    
    /**
     * @var int|null Transport delivery timestamp
     */
    private $transportDeliveryTimestamp;
    
    /**
     * Constructor
     * 
     * @param MessageId $messageId Message ID
     * @param string $topic Topic name
     * @param string $body Message body
     * @param string|null $tag Message tag
     * @param string|null $messageGroup Message group
     * @param string|null $liteTopic Lite topic
     * @param int|null $deliveryTimestamp Delivery timestamp
     * @param int|null $priority Message priority
     * @param string[] $keys Message keys
     * @param array $properties User properties
     * @param string $bornHost Born host
     * @param int $bornTimestamp Born timestamp
     * @param int $deliveryAttempt Delivery attempt
     * @param MessageQueue|null $messageQueue Message queue
     * @param string $receiptHandle Receipt handle
     * @param int $offset Queue offset
     * @param bool $corrupted Whether corrupted
     * @param int|null $transportDeliveryTimestamp Transport delivery timestamp
     */
    public function __construct(
        MessageId $messageId,
        string $topic,
        string $body,
        ?string $tag = null,
        ?string $messageGroup = null,
        ?string $liteTopic = null,
        ?int $deliveryTimestamp = null,
        ?int $priority = null,
        array $keys = [],
        array $properties = [],
        string $bornHost = '',
        int $bornTimestamp = 0,
        int $deliveryAttempt = 1,
        ?MessageQueue $messageQueue = null,
        string $receiptHandle = '',
        int $offset = 0,
        bool $corrupted = false,
        ?int $transportDeliveryTimestamp = null
    ) {
        $this->messageId = $messageId;
        $this->topic = $topic;
        $this->body = $body;
        $this->tag = $tag;
        $this->messageGroup = $messageGroup;
        $this->liteTopic = $liteTopic;
        $this->deliveryTimestamp = $deliveryTimestamp;
        $this->priority = $priority;
        $this->keys = $keys;
        $this->properties = $properties;
        $this->bornHost = $bornHost;
        $this->bornTimestamp = $bornTimestamp;
        $this->deliveryAttempt = $deliveryAttempt;
        $this->messageQueue = $messageQueue;
        $this->endpoints = $messageQueue !== null ? $messageQueue->getBroker()->getEndpoints() : null;
        $this->receiptHandle = $receiptHandle;
        $this->offset = $offset;
        $this->corrupted = $corrupted;
        $this->decodeTimestamp = (int)(microtime(true) * 1000);
        $this->transportDeliveryTimestamp = $transportDeliveryTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageId(): MessageId
    {
        return $this->messageId;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTopic(): string
    {
        return $this->topic;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBody(): string
    {
        return $this->body;
    }
    
    /**
     * Get body length
     * 
     * @return int Body length in bytes
     */
    public function getBodyLength(): int
    {
        return strlen($this->body);
    }
    
    /**
     * {@inheritdoc}
     */
    public function getProperties(): array
    {
        return $this->properties;
    }
    
    /**
     * Get property value by key
     * 
     * @param string $key Property key
     * @param mixed $default Default value
     * @return mixed Property value or default
     */
    public function getProperty(string $key, $default = null)
    {
        return $this->properties[$key] ?? $default;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getTag(): ?string
    {
        return $this->tag;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getKeys(): array
    {
        return $this->keys;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getMessageGroup(): ?string
    {
        return $this->messageGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getLiteTopic(): ?string
    {
        return $this->liteTopic;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDeliveryTimestamp(): ?int
    {
        return $this->deliveryTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getPriority(): ?int
    {
        return $this->priority;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBornHost(): string
    {
        return $this->bornHost;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getBornTimestamp(): int
    {
        return $this->bornTimestamp;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getDeliveryAttempt(): int
    {
        return $this->deliveryAttempt;
    }
    
    /**
     * Increment and get delivery attempt
     * 
     * @return int New delivery attempt
     */
    public function incrementAndGetDeliveryAttempt(): int
    {
        return ++$this->deliveryAttempt;
    }
    
    /**
     * Get message queue
     * 
     * @return MessageQueue|null Message queue
     */
    public function getMessageQueue(): ?MessageQueue
    {
        return $this->messageQueue;
    }
    
    /**
     * Get endpoints
     * 
     * @return Endpoints|null Endpoints
     */
    public function getEndpoints(): ?Endpoints
    {
        return $this->endpoints;
    }
    
    /**
     * Get receipt handle
     * 
     * @return string Receipt handle
     */
    public function getReceiptHandle(): string
    {
        return $this->receiptHandle;
    }
    
    /**
     * Set receipt handle
     * 
     * @param string $receiptHandle Receipt handle
     * @return void
     */
    public function setReceiptHandle(string $receiptHandle): void
    {
        $this->receiptHandle = $receiptHandle;
    }
    
    /**
     * Get queue offset
     * 
     * @return int Queue offset
     */
    public function getOffset(): int
    {
        return $this->offset;
    }
    
    /**
     * Check if message is corrupted
     * 
     * @return bool Whether corrupted
     */
    public function isCorrupted(): bool
    {
        return $this->corrupted;
    }
    
    /**
     * Get decode timestamp
     * 
     * @return int Decode timestamp
     */
    public function getDecodeTimestamp(): int
    {
        return $this->decodeTimestamp;
    }
    
    /**
     * Get transport delivery timestamp
     * 
     * @return int|null Transport delivery timestamp
     */
    public function getTransportDeliveryTimestamp(): ?int
    {
        return $this->transportDeliveryTimestamp;
    }
    
    /**
     * Create MessageViewImpl from Protobuf Message
     * 
     * @param \Apache\Rocketmq\V2\Message $message Protobuf message
     * @param MessageQueue|null $messageQueue Message queue (optional)
     * @param int|null $transportDeliveryTimestamp Transport delivery timestamp (optional)
     * @return MessageViewImpl Message view
     */
    public static function fromProtobuf(
        \Apache\Rocketmq\V2\Message $message,
        ?MessageQueue $messageQueue = null,
        ?int $transportDeliveryTimestamp = null
    ): MessageViewImpl {
        $systemProperties = $message->getSystemProperties();
        $topic = $message->getTopic()->getName();
        
        // Decode message ID
        $messageIdCodec = MessageIdCodec::getInstance();
        $messageId = $messageIdCodec->decode($systemProperties->getMessageId());
        
        // Verify and decompress body
        $body = $message->getBody()->toString();
        $corrupted = false;
        
        // Body digest verification
        $bodyDigest = $systemProperties->getBodyDigest();
        $checksum = $bodyDigest->getChecksum();
        $digestType = $bodyDigest->getType();
        
        switch ($digestType) {
            case \Apache\Rocketmq\V2\DigestType::CRC32:
                $expectedChecksum = Util::crc32Checksum($body);
                if ($expectedChecksum !== $checksum) {
                    $corrupted = true;
                }
                break;
            case \Apache\Rocketmq\V2\DigestType::MD5:
                $expectedChecksum = md5($body);
                if ($expectedChecksum !== $checksum) {
                    $corrupted = true;
                }
                break;
            case \Apache\Rocketmq\V2\DigestType::SHA1:
                $expectedChecksum = sha1($body);
                if ($expectedChecksum !== $checksum) {
                    $corrupted = true;
                }
                break;
            default:
                error_log("Unsupported message body digest algorithm: {$digestType}");
        }
        
        // Body decompression
        $bodyEncoding = $systemProperties->getBodyEncoding();
        switch ($bodyEncoding) {
            case \Apache\Rocketmq\V2\Encoding::GZIP:
                $decompressed = @gzdecode($body);
                if ($decompressed === false) {
                    error_log("Failed to uncompress message body");
                    $corrupted = true;
                } else {
                    $body = $decompressed;
                }
                break;
            case \Apache\Rocketmq\V2\Encoding::IDENTITY:
                // No decompression needed
                break;
            default:
                error_log("Unsupported message encoding: {$bodyEncoding}");
        }
        
        // Extract system properties
        $tag = $systemProperties->hasTag() ? $systemProperties->getTag() : null;
        $messageGroup = $systemProperties->hasMessageGroup() ? $systemProperties->getMessageGroup() : null;
        $liteTopic = $systemProperties->hasLiteTopic() ? $systemProperties->getLiteTopic() : null;
        $deliveryTimestamp = $systemProperties->hasDeliveryTimestamp() ? 
            (int)($systemProperties->getDeliveryTimestamp()->getSeconds()) : null;
        $priority = $systemProperties->hasPriority() ? $systemProperties->getPriority() : null;
        $keys = $systemProperties->getKeysList();
        $bornHost = $systemProperties->getBornHost();
        $bornTimestamp = (int)($systemProperties->getBornTimestamp()->getSeconds() * 1000);
        $deliveryAttempt = $systemProperties->getDeliveryAttempt();
        $offset = (int)$systemProperties->getQueueOffset();
        $properties = $message->getUserPropertiesMap();
        $receiptHandle = $systemProperties->getReceiptHandle();
        
        return new self(
            $messageId,
            $topic,
            $body,
            $tag,
            $messageGroup,
            $liteTopic,
            $deliveryTimestamp,
            $priority,
            $keys,
            $properties,
            $bornHost,
            $bornTimestamp,
            $deliveryAttempt,
            $messageQueue,
            $receiptHandle,
            $offset,
            $corrupted,
            $transportDeliveryTimestamp
        );
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return sprintf(
            "MessageViewImpl{messageId=%s, topic=%s, bornHost=%s, bornTimestamp=%d, deliveryAttempt=%d, tag=%s, keys=%s, messageGroup=%s, liteTopic=%s, deliveryTimestamp=%s, priority=%s}",
            (string)$this->messageId,
            $this->topic,
            $this->bornHost,
            $this->bornTimestamp,
            $this->deliveryAttempt,
            $this->tag ?? 'null',
            json_encode($this->keys),
            $this->messageGroup ?? 'null',
            $this->liteTopic ?? 'null',
            $this->deliveryTimestamp ?? 'null',
            $this->priority ?? 'null'
        );
    }
}
