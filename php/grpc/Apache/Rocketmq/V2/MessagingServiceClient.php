<?php
// GENERATED CODE -- DO NOT EDIT!

// Original file comments:
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
namespace Apache\Rocketmq\V2;

/**
 * For all the RPCs in MessagingService, the following error handling policies
 * apply:
 *
 * If the request doesn't bear a valid authentication credential, return a
 * response with common.status.code == `UNAUTHENTICATED`. If the authenticated
 * user is not granted with sufficient permission to execute the requested
 * operation, return a response with common.status.code == `PERMISSION_DENIED`.
 * If the per-user-resource-based quota is exhausted, return a response with
 * common.status.code == `RESOURCE_EXHAUSTED`. If any unexpected server-side
 * errors raise, return a response with common.status.code == `INTERNAL`.
 */
class MessagingServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Queries the route entries of the requested topic in the perspective of the
     * given endpoints. On success, servers should return a collection of
     * addressable message-queues. Note servers may return customized route
     * entries based on endpoints provided.
     *
     * If the requested topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
     * @param \Apache\Rocketmq\V2\QueryRouteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function QueryRoute(\Apache\Rocketmq\V2\QueryRouteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/QueryRoute',
        $argument,
        ['\Apache\Rocketmq\V2\QueryRouteResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Producer or consumer sends HeartbeatRequest to servers periodically to
     * keep-alive. Additionally, it also reports client-side configuration,
     * including topic subscription, load-balancing group name, etc.
     *
     * Returns `OK` if success.
     *
     * If a client specifies a language that is not yet supported by servers,
     * returns `INVALID_ARGUMENT`
     * @param \Apache\Rocketmq\V2\HeartbeatRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function Heartbeat(\Apache\Rocketmq\V2\HeartbeatRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/Heartbeat',
        $argument,
        ['\Apache\Rocketmq\V2\HeartbeatResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delivers messages to brokers.
     * Clients may further:
     * 1. Refine a message destination to message-queues which fulfills parts of
     * FIFO semantic;
     * 2. Flag a message as transactional, which keeps it invisible to consumers
     * until it commits;
     * 3. Time a message, making it invisible to consumers till specified
     * time-point;
     * 4. And more...
     *
     * Returns message-id or transaction-id with status `OK` on success.
     *
     * If the destination topic doesn't exist, returns `NOT_FOUND`.
     * @param \Apache\Rocketmq\V2\SendMessageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function SendMessage(\Apache\Rocketmq\V2\SendMessageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/SendMessage',
        $argument,
        ['\Apache\Rocketmq\V2\SendMessageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Queries the assigned route info of a topic for current consumer,
     * the returned assignment result is decided by server-side load balancer.
     *
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
     * @param \Apache\Rocketmq\V2\QueryAssignmentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function QueryAssignment(\Apache\Rocketmq\V2\QueryAssignmentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/QueryAssignment',
        $argument,
        ['\Apache\Rocketmq\V2\QueryAssignmentResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Receives messages from the server in batch manner, returns a set of
     * messages if success. The received messages should be acked or redelivered
     * after processed.
     *
     * If the pending concurrent receive requests exceed the quota of the given
     * consumer group, returns `UNAVAILABLE`. If the upstream store server hangs,
     * return `DEADLINE_EXCEEDED` in a timely manner. If the corresponding topic
     * or consumer group doesn't exist, returns `NOT_FOUND`. If there is no new
     * message in the specific topic, returns `OK` with an empty message set.
     * Please note that client may suffer from false empty responses.
     *
     * If failed to receive message from remote, server must return only one
     * `ReceiveMessageResponse` as the reply to the request, whose `Status` indicates
     * the specific reason of failure, otherwise, the reply is considered successful.
     * @param \Apache\Rocketmq\V2\ReceiveMessageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function ReceiveMessage(\Apache\Rocketmq\V2\ReceiveMessageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/apache.rocketmq.v2.MessagingService/ReceiveMessage',
        $argument,
        ['\Apache\Rocketmq\V2\ReceiveMessageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Acknowledges the message associated with the `receipt_handle` or `offset`
     * in the `AckMessageRequest`, it means the message has been successfully
     * processed. Returns `OK` if the message server remove the relevant message
     * successfully.
     *
     * If the given receipt_handle is illegal or out of date, returns
     * `INVALID_ARGUMENT`.
     * @param \Apache\Rocketmq\V2\AckMessageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function AckMessage(\Apache\Rocketmq\V2\AckMessageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/AckMessage',
        $argument,
        ['\Apache\Rocketmq\V2\AckMessageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Forwards one message to dead letter queue if the max delivery attempts is
     * exceeded by this message at client-side, return `OK` if success.
     * @param \Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ForwardMessageToDeadLetterQueue(\Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/ForwardMessageToDeadLetterQueue',
        $argument,
        ['\Apache\Rocketmq\V2\ForwardMessageToDeadLetterQueueResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Commits or rollback one transactional message.
     * @param \Apache\Rocketmq\V2\EndTransactionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function EndTransaction(\Apache\Rocketmq\V2\EndTransactionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/EndTransaction',
        $argument,
        ['\Apache\Rocketmq\V2\EndTransactionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Once a client starts, it would immediately establishes bi-lateral stream
     * RPCs with brokers, reporting its settings as the initiative command.
     *
     * When servers have need of inspecting client status, they would issue
     * telemetry commands to clients. After executing received instructions,
     * clients shall report command execution results through client-side streams.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function Telemetry($metadata = [], $options = []) {
        return $this->_bidiRequest('/apache.rocketmq.v2.MessagingService/Telemetry',
        ['\Apache\Rocketmq\V2\TelemetryCommand','decode'],
        $metadata, $options);
    }

    /**
     * Notify the server that the client is terminated.
     * @param \Apache\Rocketmq\V2\NotifyClientTerminationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function NotifyClientTermination(\Apache\Rocketmq\V2\NotifyClientTerminationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/NotifyClientTermination',
        $argument,
        ['\Apache\Rocketmq\V2\NotifyClientTerminationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Once a message is retrieved from consume queue on behalf of the group, it
     * will be kept invisible to other clients of the same group for a period of
     * time. The message is supposed to be processed within the invisible
     * duration. If the client, which is in charge of the invisible message, is
     * not capable of processing the message timely, it may use
     * ChangeInvisibleDuration to lengthen invisible duration.
     * @param \Apache\Rocketmq\V2\ChangeInvisibleDurationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall
     */
    public function ChangeInvisibleDuration(\Apache\Rocketmq\V2\ChangeInvisibleDurationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/apache.rocketmq.v2.MessagingService/ChangeInvisibleDuration',
        $argument,
        ['\Apache\Rocketmq\V2\ChangeInvisibleDurationResponse', 'decode'],
        $metadata, $options);
    }

}
