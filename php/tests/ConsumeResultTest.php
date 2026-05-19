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
require_once __DIR__ . '/../ConsumeResult.php';

use Apache\Rocketmq\ConsumeResult;

class ConsumeResultTest
{
    public function testConstants()
    {
        TestRunner::assertEqualsWithMessage(0, ConsumeResult::SUCCESS, "SUCCESS should be 0");
        TestRunner::assertEqualsWithMessage(1, ConsumeResult::FAILURE, "FAILURE should be 1");
        TestRunner::assertTrueWithMessage(
            ConsumeResult::SUCCESS !== ConsumeResult::FAILURE,
            "SUCCESS and FAILURE should be different"
        );
    }
}

echo "=== ConsumeResultTest ===\n";
$test = new ConsumeResultTest();
$test->testConstants();
echo "  [OK] testConstants\n";
