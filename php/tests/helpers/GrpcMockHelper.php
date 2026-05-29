<?php

namespace Apache\Rocketmq\Test\Helpers;

use Apache\Rocketmq\V2\MessagingServiceClient;

/**
 * Helper for creating and configuring gRPC mock objects.
 */
class GrpcMockHelper
{
    /**
     * Create a mock MessagingServiceClient with all gRPC methods stubbed.
     *
     * @return MessagingServiceClient|\PHPUnit\Framework\MockObject\MockObject
     */
    public static function createMockClient(): MessagingServiceClient
    {
        $methods = [
            'QueryRoute', 'Heartbeat', 'SendMessage', 'ReceiveMessage',
            'AckMessage', 'ForwardMessageToDeadLetterQueue', 'EndTransaction',
            'NotifyClientTermination', 'ChangeInvisibleDuration',
            'QueryAssignment', 'RecallMessage', 'SyncLiteSubscription',
            'Telemetry',
        ];

        return (new \PHPUnit\Framework\MockObject\Generator())->getMock(
            MessagingServiceClient::class,
            $methods,
            [],
            '',
            false
        );
    }

    /**
     * Configure a unary gRPC call on the mock client.
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param string $methodName gRPC method name (e.g. 'QueryRoute')
     * @param object|null $response Protobuf response message
     * @param int $statusCode gRPC status code (0 = OK)
     * @param string $statusDetails Status details string
     * @return void
     */
    public static function mockUnaryCall(
        $mockClient,
        string $methodName,
        $response,
        int $statusCode = 0,
        string $statusDetails = 'OK'
    ): void {
        $status = new \stdClass();
        $status->code = $statusCode;
        $status->details = $statusDetails;

        $call = new class($response, $status) {
            private $response;
            private $status;

            public function __construct($response, $status)
            {
                $this->response = $response;
                $this->status = $status;
            }

            public function wait(): array
            {
                return [$this->response, $this->status];
            }
        };

        $mockClient->method($methodName)->willReturn($call);
    }

    /**
     * Configure a server-streaming gRPC call on the mock client.
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param string $methodName gRPC method name (e.g. 'ReceiveMessage')
     * @param array $responses Array of protobuf response messages to yield
     * @return void
     */
    public static function mockServerStreamCall(
        $mockClient,
        string $methodName,
        array $responses
    ): void {
        $call = new class($responses) {
            private array $responses;

            public function __construct(array $responses)
            {
                $this->responses = $responses;
            }

            public function responses(): \Generator
            {
                foreach ($this->responses as $response) {
                    yield $response;
                }
            }
        };

        $mockClient->method($methodName)->willReturn($call);
    }

    /**
     * Configure a bidi-streaming gRPC call on the mock client.
     *
     * @param object $mockClient Mock MessagingServiceClient
     * @param string $methodName gRPC method name
     * @param array $readResponses Array of protobuf response messages for read()
     * @return void
     */
    public static function mockBidiStreamCall(
        $mockClient,
        string $methodName,
        array $readResponses = []
    ): void {
        $stream = new class($readResponses) {
            private array $written = [];
            private array $readResponses;
            private int $readIndex = 0;

            public function __construct(array $readResponses)
            {
                $this->readResponses = $readResponses;
            }

            public function write($data, array $options = []): void
            {
                $this->written[] = $data;
            }

            public function read()
            {
                if ($this->readIndex < count($this->readResponses)) {
                    return $this->readResponses[$this->readIndex++];
                }
                return null;
            }

            public function writesDone(): void
            {
            }

            public function getWritten(): array
            {
                return $this->written;
            }

            public function cancel(): void
            {
            }

            public function isCancelled(): bool
            {
                return false;
            }
        };

        $mockClient->method($methodName)->willReturn($stream);
    }

    /**
     * Create a standalone unary call object for use with willReturnOnConsecutiveCalls().
     *
     * @param object|null $response Protobuf response message
     * @param int $statusCode gRPC status code
     * @return object Anonymous object with wait() method
     */
    public static function createUnaryCall($response, int $statusCode = 0): object
    {
        $status = new \stdClass();
        $status->code = $statusCode;
        $status->details = $statusCode === 0 ? 'OK' : 'Error';

        return new class($response, $status) {
            private $response;
            private $status;

            public function __construct($response, $status)
            {
                $this->response = $response;
                $this->status = $status;
            }

            public function wait(): array
            {
                return [$this->response, $this->status];
            }
        };
    }
}
