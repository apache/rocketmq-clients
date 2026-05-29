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
