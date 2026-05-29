<?php

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
