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

use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\Resource;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../TelemetrySession.php';

class TelemetrySessionIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testSyncSettingsSuccess()
    {
        $mock = GrpcMockHelper::createMockClient();

        $serverSettings = new TelemetryCommand();
        $echoedSettings = new Settings();
        $echoedSettings->setClientType(ClientType::PRODUCER);
        $serverSettings->setSettings($echoedSettings);

        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$serverSettings]);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::PRODUCER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertTrue($result);
    }

    public function testSettingsWithSubscription()
    {
        $mock = GrpcMockHelper::createMockClient();

        $serverSettings = new TelemetryCommand();
        $echoedSettings = new Settings();
        $echoedSettings->setClientType(ClientType::SIMPLE_CONSUMER);
        $subscription = new Subscription();
        $group = new Resource();
        $group->setName('test-group');
        $subscription->setGroup($group);
        $echoedSettings->setSubscription($subscription);
        $serverSettings->setSettings($echoedSettings);

        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$serverSettings]);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::SIMPLE_CONSUMER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertTrue($result);
    }

    public function testSyncSettingsTimeout()
    {
        $mock = GrpcMockHelper::createMockClient();
        // No response from server — times out
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', []);

        $session = TelemetrySession::getInstance($mock, $this->endpoints, 'test-client');

        $command = new TelemetryCommand();
        $requestSettings = new Settings();
        $requestSettings->setClientType(ClientType::PRODUCER);
        $command->setSettings($requestSettings);

        $result = $session->syncSettings($command);
        $this->assertFalse($result);
    }

    public function testSingletonReturnsSameInstance()
    {
        $mock = GrpcMockHelper::createMockClient();

        $session1 = TelemetrySession::getInstance($mock, $this->endpoints, 'client-1');
        $session2 = TelemetrySession::getInstance($mock, $this->endpoints, 'client-1');

        $this->assertSame($session1, $session2);
    }
}
