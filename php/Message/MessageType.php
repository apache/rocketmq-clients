<?php
declare(strict_types=1);
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

/**
 * MessageType defines the type of message
 * 
 * References Java MessageType enum (67 lines)
 */
class MessageType
{
    const NORMAL = 1;
    const FIFO = 2;
    const LITE = 3;
    const DELAY = 4;
    const PRIORITY = 5;
    const TRANSACTION = 6;
    
    /**
     * Convert from Protobuf
     * 
     * @param int $messageType Protobuf message type
     * @return int Message type constant
     * @throws \InvalidArgumentException If invalid type
     */
    public static function fromProtobuf(int $messageType): int
    {
        switch ($messageType) {
            case \Apache\Rocketmq\V2\MessageType::NORMAL:
                return self::NORMAL;
            case \Apache\Rocketmq\V2\MessageType::FIFO:
                return self::FIFO;
            case \Apache\Rocketmq\V2\MessageType::LITE:
                return self::LITE;
            case \Apache\Rocketmq\V2\MessageType::DELAY:
                return self::DELAY;
            case \Apache\Rocketmq\V2\MessageType::TRANSACTION:
                return self::TRANSACTION;
            case \Apache\Rocketmq\V2\MessageType::PRIORITY:
                return self::PRIORITY;
            default:
                throw new \InvalidArgumentException("Message type is not specified");
        }
    }
    
    /**
     * Convert to Protobuf
     * 
     * @param int $messageType Message type constant
     * @return int Protobuf message type
     */
    public static function toProtobuf(int $messageType): int
    {
        switch ($messageType) {
            case self::NORMAL:
                return \Apache\Rocketmq\V2\MessageType::NORMAL;
            case self::FIFO:
                return \Apache\Rocketmq\V2\MessageType::FIFO;
            case self::LITE:
                return \Apache\Rocketmq\V2\MessageType::LITE;
            case self::DELAY:
                return \Apache\Rocketmq\V2\MessageType::DELAY;
            case self::TRANSACTION:
                return \Apache\Rocketmq\V2\MessageType::TRANSACTION;
            case self::PRIORITY:
                return \Apache\Rocketmq\V2\MessageType::PRIORITY;
            default:
                return \Apache\Rocketmq\V2\MessageType::MESSAGE_TYPE_UNSPECIFIED;
        }
    }
    
    /**
     * Get message type name
     * 
     * @param int $messageType Message type constant
     * @return string Type name
     */
    public static function getName(int $messageType): string
    {
        switch ($messageType) {
            case self::NORMAL:
                return 'NORMAL';
            case self::FIFO:
                return 'FIFO';
            case self::LITE:
                return 'LITE';
            case self::DELAY:
                return 'DELAY';
            case self::PRIORITY:
                return 'PRIORITY';
            case self::TRANSACTION:
                return 'TRANSACTION';
            default:
                return 'UNKNOWN';
        }
    }
}
