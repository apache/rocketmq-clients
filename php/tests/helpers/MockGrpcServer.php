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
use Apache\Rocketmq\V2\MessagingServiceClient;

/**
 * MockGrpcServer — Fluent API for setting up mock gRPC servers in tests.
 *
 * Provides a builder-style interface for configuring mock gRPC behaviour
 * and verifying call counts. Backed by GrpcMockHelper and RpcClientManager.
 */
class MockGrpcServer {
    private $mockClient;
    private array $stubs = [];
    private array $callCounts = [];
    private static array $registeredEndpoints = [];

    /**
     * Create a new mock gRPC server.
     * @return self
     */
    public static function create(): self
    {
        $instance = new self();
        $instance->mockClient = GrpcMockHelper::createMockClient();
        return $instance;
    }

    /**
     * Register the mock client with RpcClientManager for the given endpoints.
     *
     * @param string $endpoints Server endpoint (e.g. 'localhost:8080')
     * @return self
     */
    public function register(string $endpoints): self
    {
        RpcClientManager::getInstance()->registerMock($endpoints, $this->mockClient);
        self::$registeredEndpoints[] = $endpoints;
        return $this;
    }

    /**
     * Configure a unary gRPC call stub.
     *
     * @param string $methodName gRPC method name (e.g. 'QueryRoute')
     * @param object|null $response Protobuf response message
     * @param int $statusCode gRPC status code (0 = OK)
     * @param string $statusDetails Status details string
     * @return self
     */
    public function withUnary(string $methodName, $response, int $statusCode = 0, string $statusDetails = 'OK'): self
    {
        GrpcMockHelper::mockUnaryCall($this->mockClient, $methodName, $response, $statusCode, $statusDetails);
        $this->stubs[$methodName] = 'unary';
        $this->callCounts[$methodName] = 0;
        return $this;
    }

    /**
     * Configure a server-streaming gRPC call stub.
     *
     * @param string $methodName gRPC method name (e.g. 'ReceiveMessage')
     * @param array $responses Array of protobuf response messages to yield
     * @return self
     */
    public function withStream(string $methodName, array $responses): self
    {
        GrpcMockHelper::mockServerStreamCall($this->mockClient, $methodName, $responses);
        $this->stubs[$methodName] = 'stream';
        $this->callCounts[$methodName] = 0;
        return $this;
    }

    /**
     * Configure a bidirectional-streaming gRPC call stub.
     *
     * @param string $methodName gRPC method name (e.g. 'Telemetry')
     * @param array $readResponses Array of protobuf response messages for read()
     * @return self
     */
    public function withBidiStream(string $methodName, array $readResponses = []): self
    {
        GrpcMockHelper::mockBidiStreamCall($this->mockClient, $methodName, $readResponses);
        $this->stubs[$methodName] = 'bidi';
        $this->callCounts[$methodName] = 0;
        return $this;
    }

    /**
     * Get the underlying mock MessagingServiceClient.
     *
     * @return MessagingServiceClient
     */
    public function getClient(): MessagingServiceClient
    {
        return $this->mockClient;
    }

    /**
     * Get the call count for a specific method.
     *
     * @param string $methodName gRPC method name
     * @return int Number of times the method was called (0 if never stubbed or called)
     */
    public function getCallCount(string $methodName): int
    {
        return $this->callCounts[$methodName] ?? 0;
    }

    /**
     * Reset all call counts and stubs.
     *
     * @return self
     */
    public function reset(): self
    {
        $this->callCounts = [];
        $this->stubs = [];
        return $this;
    }

    /**
     * Clean up all registered mocks from RpcClientManager.
     *
     * @return void
     */
    public static function cleanup(): void
    {
        RpcClientManager::reset();
        self::$registeredEndpoints = [];
    }
}
