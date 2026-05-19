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
use Apache\Rocketmq\Test\TestRunner;

class MessageIdImplTest
{
    public function testToStringV0()
    {
        $codec = MessageIdCodec::getInstance();
        $messageId = new \Apache\Rocketmq\MessageIdImpl(
            MessageIdCodec::MESSAGE_ID_VERSION_V0,
            '0156F7E71C361B21BC024CCDBE00000000'
        );

        TestRunner::assertEqualsWithMessage(
            '0156F7E71C361B21BC024CCDBE00000000',
            $messageId->toString(),
            "V0 toString should return suffix directly"
        );
    }

    public function testToStringV1()
    {
        $codec = MessageIdCodec::getInstance();
        $messageId = new \Apache\Rocketmq\MessageIdImpl(
            MessageIdCodec::MESSAGE_ID_VERSION_V1,
            '56F7E71C361B21BC024CCDBE00000000'
        );

        TestRunner::assertEqualsWithMessage(
            '0156F7E71C361B21BC024CCDBE00000000',
            $messageId->toString(),
            "V1 toString should prefix with version"
        );
    }

    public function testEquals()
    {
        $id1 = new \Apache\Rocketmq\MessageIdImpl('01', 'ABC123');
        $id2 = new \Apache\Rocketmq\MessageIdImpl('01', 'ABC123');
        $id3 = new \Apache\Rocketmq\MessageIdImpl('01', 'DEF456');

        TestRunner::assertTrue($id1->equals($id2), "Same version and suffix should be equal");
        TestRunner::assertFalse($id1->equals($id3), "Different suffix should not be equal");
        TestRunner::assertTrue($id1->equals($id1), "Same instance should be equal");
        TestRunner::assertFalse($id1->equals(null), "Null should not be equal");
    }

    public function testHashCode()
    {
        $id1 = new \Apache\Rocketmq\MessageIdImpl('01', 'ABC123');
        $id2 = new \Apache\Rocketmq\MessageIdImpl('01', 'ABC123');

        TestRunner::assertEqualsWithMessage(
            $id1->hashCode(),
            $id2->hashCode(),
            "Equal IDs should have same hash code"
        );
    }

    public function testDupeString()
    {
        $id = new \Apache\Rocketmq\MessageIdImpl('01', 'ABC123');
        TestRunner::assertEqualsWithMessage(
            '01ABC123',
            (string)$id,
            "__toString should return toString"
        );
    }
}

echo "=== MessageIdImplTest ===\n";
$test = new MessageIdImplTest();
$test->testToStringV0();
echo "  [OK] testToStringV0\n";
$test->testToStringV1();
echo "  [OK] testToStringV1\n";
$test->testEquals();
echo "  [OK] testEquals\n";
$test->testHashCode();
echo "  [OK] testHashCode\n";
$test->testDupeString();
echo "  [OK] testDupeString\n";
