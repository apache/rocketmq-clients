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

namespace Apache\Rocketmq\Test\Helpers;

use Apache\Rocketmq\RpcClientManager;
use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\Logger;
use Apache\Rocketmq\V2\MessagingServiceClient;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/../../autoload.php';
require_once __DIR__ . '/../../vendor/autoload.php';

/**
 * Base class for integration tests.
 *
 * Handles singleton reset between tests and provides convenience
 * methods for creating and registering mock gRPC clients.
 */
abstract class IntegrationTestCase extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        RpcClientManager::reset();
        TelemetrySession::resetAll();
        Logger::close();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        RpcClientManager::reset();
        TelemetrySession::resetAll();
        Logger::close();
    }

    /**
     * Create a mock MessagingServiceClient and register it for the given endpoints.
     *
     * @param string $endpoints Server endpoint (e.g. 'localhost:8080')
     * @return MessagingServiceClient|\PHPUnit\Framework\MockObject\MockObject
     */
    protected function createAndRegisterMock(string $endpoints): MessagingServiceClient
    {
        $mock = GrpcMockHelper::createMockClient();
        RpcClientManager::getInstance()->registerMock($endpoints, $mock);
        return $mock;
    }
}
