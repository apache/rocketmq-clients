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

namespace Apache\Rocketmq\Test;

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../MessageId.php';
require_once __DIR__ . '/../MessageIdImpl.php';
require_once __DIR__ . '/../MessageIdCodec.php';

use Apache\Rocketmq\MessageIdCodec;

class MessageIdCodecTest
{
    private $codec;

    public function setUp(): void
    {
        $this->codec = MessageIdCodec::getInstance();
    }

    public function testNextMessageId()
    {
        $messageId = $this->codec->nextMessageId();
        TestRunner::assertEquals(
            MessageIdCodec::MESSAGE_ID_LENGTH_FOR_V1_OR_LATER,
            strlen($messageId->toString()),
            "Message ID length should be " . MessageIdCodec::MESSAGE_ID_LENGTH_FOR_V1_OR_LATER
        );
    }

    public function testNextMessageIdWithNoRepetition()
    {
        $messageIds = [];
        $messageIdCount = 64;
        for ($i = 0; $i < $messageIdCount; $i++) {
            $id = $this->codec->nextMessageId()->toString();
            $messageIds[$id] = true;
        }
        TestRunner::assertEquals(
            $messageIdCount,
            count($messageIds),
            "All {$messageIdCount} message IDs should be unique"
        );
    }

    public function testDecode()
    {
        $messageIdString = "0156F7E71C361B21BC024CCDBE00000000";
        $messageId = $this->codec->decode($messageIdString);
        TestRunner::assertEquals(
            MessageIdCodec::MESSAGE_ID_VERSION_V1,
            $messageId->getVersion(),
            "Version should be V1"
        );
        TestRunner::assertEquals(
            $messageIdString,
            $messageId->toString(),
            "Decoded message ID should match original"
        );
    }
}

echo "=== MessageIdCodecTest ===\n";
TestRunner::run(new MessageIdCodecTest());
