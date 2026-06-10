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

require_once __DIR__ . '/../autoload.php';

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\ConsumeResult;

class ConsumeResultTest extends TestCase
{
    public function testEnumValues()
    {
        $this->assertEquals(0, ConsumeResult::SUCCESS->value, "SUCCESS should be 0");
        $this->assertEquals(1, ConsumeResult::FAILURE->value, "FAILURE should be 1");
        $this->assertTrue(
            ConsumeResult::SUCCESS !== ConsumeResult::FAILURE,
            "SUCCESS and FAILURE should be different"
        );
    }

    public function testFromMixedInt()
    {
        $this->assertSame(ConsumeResult::SUCCESS, ConsumeResult::fromMixed(0));
        $this->assertSame(ConsumeResult::FAILURE, ConsumeResult::fromMixed(1));
    }

    public function testFromMixedEnum()
    {
        $this->assertSame(ConsumeResult::SUCCESS, ConsumeResult::fromMixed(ConsumeResult::SUCCESS));
        $this->assertSame(ConsumeResult::FAILURE, ConsumeResult::fromMixed(ConsumeResult::FAILURE));
    }
}
