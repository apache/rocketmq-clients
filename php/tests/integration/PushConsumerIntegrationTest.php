<?php

namespace Apache\Rocketmq\Test\Integration;

use Apache\Rocketmq\PushConsumer;
use Apache\Rocketmq\Test\Helpers\IntegrationTestCase;
use Apache\Rocketmq\Test\Helpers\GrpcMockHelper;
use Apache\Rocketmq\V2\HeartbeatResponse;
use Apache\Rocketmq\V2\NotifyClientTerminationResponse;
use Apache\Rocketmq\V2\TelemetryCommand;

require_once __DIR__ . '/../helpers/IntegrationTestCase.php';
require_once __DIR__ . '/../helpers/GrpcMockHelper.php';
require_once __DIR__ . '/../../PushConsumer.php';

class PushConsumerIntegrationTest extends IntegrationTestCase
{
    private $endpoints = 'localhost:8080';

    public function testConstructorCreatesGprcClient()
    {
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->assertNotNull($consumer->getClient());
    }

    public function testGetClientId()
    {
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'clientId' => 'custom-push-client',
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->assertEquals('custom-push-client', $consumer->getClientId());
    }

    public function testIsRunningInitiallyFalse()
    {
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->assertFalse($consumer->isRunning());
    }

    public function testShutdownBeforeStartIsSafe()
    {
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        // Should not throw
        $consumer->shutdown();
        $this->assertTrue(true);
    }

    public function testNamespaceConfiguration()
    {
        $consumer = new PushConsumer($this->endpoints, 'test-group', [
            'namespace' => 'my-namespace',
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $this->assertEquals('my-namespace', $consumer->getNamespace());
    }

    public function testGetGroupResource()
    {
        $consumer = new PushConsumer($this->endpoints, 'my-group', [
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $resource = $consumer->getGroupResource();
        $this->assertEquals('my-group', $resource->getName());
    }

    public function testGetGroupResourceWithNamespace()
    {
        $consumer = new PushConsumer($this->endpoints, 'my-group', [
            'namespace' => 'my-ns',
            'subscriptionExpressions' => ['test-topic' => '*'],
        ]);

        $resource = $consumer->getGroupResourceWithNamespace();
        $this->assertEquals('my-group', $resource->getName());
        $this->assertEquals('my-ns', $resource->getResourceNamespace());
    }
}
