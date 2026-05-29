<?php

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
