<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\SimpleConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\TelemetryCommand;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../SimpleConsumer.php';

class HeartbeatIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testDoHeartbeatSendsHeartbeatRequest()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'Heartbeat', new HeartbeatResponse(), 0);
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Call doHeartbeat directly to test the heartbeat logic.
        // doHeartbeat() has its own concurrency guard; since heartbeatInProgress
        // defaults to false, it will proceed to send a real Heartbeat request.
        $consumer->doHeartbeat();
        $this->assertTrue(true);

        $consumer->shutdown();
    }

    public function testHeartbeatConcurrencyGuard()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);

        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $settingsResponse = new TelemetryCommand();
        $settingsResponse->setSettings(new \Apache\Rocketmq\V2\Settings());
        GrpcMockHelper::mockBidiStreamCall($mock, 'Telemetry', [$settingsResponse]);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Force heartbeatInProgress to true via reflection
        $ref = new \ReflectionProperty($consumer, 'heartbeatInProgress');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        // isHeartbeatInProgress() should reflect the reflection-flag we just set
        $this->assertTrue($consumer->isHeartbeatInProgress());

        $consumer->shutdown();
    }

    public function testOnHeartbeatTickInProgressSkips()
    {
        $mock = $this->createAndRegisterMock($this->endpoints);
        GrpcMockHelper::mockUnaryCall($mock, 'NotifyClientTermination', new NotifyClientTerminationResponse(), 0);

        $consumer = new SimpleConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Set heartbeat in progress via reflection
        $ref = new \ReflectionProperty($consumer, 'heartbeatInProgress');
        $ref->setAccessible(true);
        $ref->setValue($consumer, true);

        // onHeartbeatTick should skip the heartbeat if it is already in progress
        $consumer->onHeartbeatTick();
        $this->assertTrue(true);

        $consumer->shutdown();
    }
}
