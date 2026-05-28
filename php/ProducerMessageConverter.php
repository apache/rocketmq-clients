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

use Apache\Rocketmq\V2\Message as ProtobufMessage;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\SystemProperties;
use Apache\Rocketmq\V2\Encoding;
use Apache\Rocketmq\V2\MessageType as V2MessageType;
use Google\Protobuf\Timestamp;

/**
 * ProducerMessageConverter handles message conversion from domain objects to protobuf.
 *
 * Responsibilities:
 * - Convert Message objects to protobuf format
 * - Detect message types (NORMAL, FIFO, DELAY, TRANSACTION, etc.)
 * - Enrich messages with system properties
 *
 * This class is internal to Producer and should not be used directly.
 */
class ProducerMessageConverter
{
    /**
     * Convert a domain Message to protobuf Message with enriched system properties.
     *
     * @param \Apache\Rocketmq\V2\Message $msg Protobuf message object
     * @param object $messageQueue Message queue with ID
     * @param bool $txEnabled Whether transaction is enabled
     * @return ProtobufMessage Protobuf message (same as input, but enriched)
     */
    public function toProtobufMessage(\Apache\Rocketmq\V2\Message $msg, $messageQueue, bool $txEnabled = false): ProtobufMessage
    {
        $messageId = MessageIdCodec::getInstance()->nextMessageId()->toString();

        $systemProperties = new SystemProperties();
        $systemProperties->setMessageId($messageId);
        $systemProperties->setBornTimestamp($this->createTimestamp());
        $systemProperties->setBornHost(gethostname() ?: 'localhost');
        $systemProperties->setBodyEncoding(Encoding::IDENTITY);
        
        $queueId = $messageQueue->getId();
        if ($queueId !== null) {
            $systemProperties->setQueueId($queueId);
        }
        
        $systemProperties->setMessageType($this->detectMessageType($msg, $txEnabled));

        // Copy user-defined system properties
        $inputSysProps = $msg->getSystemProperties();
        if ($inputSysProps) {
            if (method_exists($inputSysProps, 'getTag') && $inputSysProps->hasTag()) {
                $systemProperties->setTag($inputSysProps->getTag());
            }
            if (method_exists($inputSysProps, 'getKeys')) {
                $keys = $inputSysProps->getKeys();
                if (!ProtobufUtil::isRepeatedFieldEmpty($keys)) {
                    $systemProperties->setKeys($keys);
                }
            }
            if (method_exists($inputSysProps, 'getMessageGroup') && $inputSysProps->hasMessageGroup()) {
                $systemProperties->setMessageGroup($inputSysProps->getMessageGroup());
            }
            if (method_exists($inputSysProps, 'getDeliveryTimestamp') && $inputSysProps->hasDeliveryTimestamp()) {
                $systemProperties->setDeliveryTimestamp($inputSysProps->getDeliveryTimestamp());
            }
            if (method_exists($inputSysProps, 'getLiteTopic') && $inputSysProps->hasLiteTopic()) {
                $systemProperties->setLiteTopic($inputSysProps->getLiteTopic());
            }
            if (method_exists($inputSysProps, 'getPriority') && $inputSysProps->hasPriority()) {
                $systemProperties->setPriority($inputSysProps->getPriority());
            }
            if (method_exists($inputSysProps, 'getTraceContext') && $inputSysProps->hasTraceContext()) {
                $systemProperties->setTraceContext($inputSysProps->getTraceContext());
            }
        }

        $topicResource = new Resource();
        $topicResource->setName($msg->getTopic()->getName());

        $protoMsg = new ProtobufMessage();
        $protoMsg->setTopic($topicResource);
        $protoMsg->setBody($msg->getBody());
        $protoMsg->setSystemProperties($systemProperties);

        // Copy user properties
        $userProps = $msg->getUserProperties();
        if (!ProtobufUtil::isMapFieldEmpty($userProps)) {
            foreach ($userProps as $key => $value) {
                $protoMsg->getUserProperties()[$key] = $value;
            }
        }

        return $protoMsg;
    }

    /**
     * Detect message type based on system properties and transaction state.
     *
     * Message type priority:
     * 1. FIFO (has messageGroup)
     * 2. LITE (has liteTopic)
     * 3. DELAY (has deliveryTimestamp)
     * 4. PRIORITY (has priority)
     * 5. TRANSACTION (txEnabled and no other special properties)
     * 6. NORMAL (default)
     *
     * @param \Apache\Rocketmq\V2\Message $msg Protobuf message object
     * @param bool $txEnabled Whether transaction is enabled
     * @return int Message type constant
     */
    public function detectMessageType(\Apache\Rocketmq\V2\Message $msg, bool $txEnabled = false): int
    {
        $sysProps = $msg->getSystemProperties();
        $hasMessageGroup = $sysProps && method_exists($sysProps, 'hasMessageGroup') && $sysProps->hasMessageGroup();
        $hasLiteTopic = $sysProps && method_exists($sysProps, 'hasLiteTopic') && $sysProps->hasLiteTopic();
        $hasPriority = $sysProps && method_exists($sysProps, 'hasPriority') && $sysProps->hasPriority();
        $hasDeliveryTimestamp = $sysProps && method_exists($sysProps, 'hasDeliveryTimestamp') && $sysProps->hasDeliveryTimestamp();

        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::NORMAL;
        }
        if ($hasMessageGroup && !$txEnabled) {
            return V2MessageType::FIFO;
        }
        if ($hasLiteTopic && !$txEnabled) {
            return V2MessageType::LITE;
        }
        if ($hasDeliveryTimestamp && !$txEnabled) {
            return V2MessageType::DELAY;
        }
        if ($hasPriority && !$txEnabled) {
            return V2MessageType::PRIORITY;
        }
        if (!$hasMessageGroup && !$hasLiteTopic && !$hasPriority && !$hasDeliveryTimestamp && $txEnabled) {
            return V2MessageType::TRANSACTION;
        }

        return V2MessageType::NORMAL;
    }

    /**
     * Create a protobuf Timestamp from current time.
     *
     * @return Timestamp Protobuf timestamp
     */
    private function createTimestamp(): Timestamp
    {
        $now = microtime(true);
        $seconds = (int)$now;
        $nanos = (int)(($now - $seconds) * 1000000000);

        $timestamp = new Timestamp();
        $timestamp->setSeconds($seconds);
        $timestamp->setNanos($nanos);
        return $timestamp;
    }
}
