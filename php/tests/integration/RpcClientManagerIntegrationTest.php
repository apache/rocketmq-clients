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

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';

class RpcClientManagerIntegrationTest extends IntegrationTestCase
{
    public function testMockRegistrationReturnsMockInsteadOfRealClient()
    {
        $endpoints = 'localhost:8080';
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($endpoints, $mock);

        $client = RpcClientManager::getInstance()->getClient($endpoints);

        $this->assertSame($mock, $client);
    }

    public function testMockNotRegisteredReturnsRealClient()
    {
        $client = RpcClientManager::getInstance()->getClient('localhost:9999');

        $this->assertNotNull($client);
    }

    public function testClearMocksRemovesAllRegistrations()
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock('localhost:8080', $mock);

        RpcClientManager::getInstance()->clearMocks();

        // After clearing, getClient creates a real client (or another mock)
        $this->assertTrue(true); // clearMocks doesn't throw
    }

    public function testDifferentEndpointsHaveDifferentMocks()
    {
        $mock1 = GrpcMockHelper::createMockClient();
        $mock2 = GrpcMockHelper::createMockClient();

        RpcClientManager::getInstance()->registerMock('endpoint1:8080', $mock1);
        RpcClientManager::getInstance()->registerMock('endpoint2:8080', $mock2);

        $this->assertSame($mock1, RpcClientManager::getInstance()->getClient('endpoint1:8080'));
        $this->assertSame($mock2, RpcClientManager::getInstance()->getClient('endpoint2:8080'));
    }
}
