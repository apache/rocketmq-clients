# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import grpc

from rocketmq.grpc_protocol import service_pb2 as service__pb2


class MessagingServiceStub(object):
    """For all the RPCs in MessagingService, the following error handling policies
    apply:

    If the request doesn't bear a valid authentication credential, return a
    response with common.status.code == `UNAUTHENTICATED`. If the authenticated
    user is not granted with sufficient permission to execute the requested
    operation, return a response with common.status.code == `PERMISSION_DENIED`.
    If the per-user-resource-based quota is exhausted, return a response with
    common.status.code == `RESOURCE_EXHAUSTED`. If any unexpected server-side
    errors raise, return a response with common.status.code == `INTERNAL`.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.QueryRoute = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/QueryRoute',
                request_serializer=service__pb2.QueryRouteRequest.SerializeToString,
                response_deserializer=service__pb2.QueryRouteResponse.FromString,
                )
        self.Heartbeat = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/Heartbeat',
                request_serializer=service__pb2.HeartbeatRequest.SerializeToString,
                response_deserializer=service__pb2.HeartbeatResponse.FromString,
                )
        self.SendMessage = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/SendMessage',
                request_serializer=service__pb2.SendMessageRequest.SerializeToString,
                response_deserializer=service__pb2.SendMessageResponse.FromString,
                )
        self.QueryAssignment = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/QueryAssignment',
                request_serializer=service__pb2.QueryAssignmentRequest.SerializeToString,
                response_deserializer=service__pb2.QueryAssignmentResponse.FromString,
                )
        self.ReceiveMessage = channel.unary_stream(
                '/apache.rocketmq.v2.MessagingService/ReceiveMessage',
                request_serializer=service__pb2.ReceiveMessageRequest.SerializeToString,
                response_deserializer=service__pb2.ReceiveMessageResponse.FromString,
                )
        self.AckMessage = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/AckMessage',
                request_serializer=service__pb2.AckMessageRequest.SerializeToString,
                response_deserializer=service__pb2.AckMessageResponse.FromString,
                )
        self.ForwardMessageToDeadLetterQueue = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/ForwardMessageToDeadLetterQueue',
                request_serializer=service__pb2.ForwardMessageToDeadLetterQueueRequest.SerializeToString,
                response_deserializer=service__pb2.ForwardMessageToDeadLetterQueueResponse.FromString,
                )
        self.PullMessage = channel.unary_stream(
                '/apache.rocketmq.v2.MessagingService/PullMessage',
                request_serializer=service__pb2.PullMessageRequest.SerializeToString,
                response_deserializer=service__pb2.PullMessageResponse.FromString,
                )
        self.UpdateOffset = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/UpdateOffset',
                request_serializer=service__pb2.UpdateOffsetRequest.SerializeToString,
                response_deserializer=service__pb2.UpdateOffsetResponse.FromString,
                )
        self.GetOffset = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/GetOffset',
                request_serializer=service__pb2.GetOffsetRequest.SerializeToString,
                response_deserializer=service__pb2.GetOffsetResponse.FromString,
                )
        self.QueryOffset = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/QueryOffset',
                request_serializer=service__pb2.QueryOffsetRequest.SerializeToString,
                response_deserializer=service__pb2.QueryOffsetResponse.FromString,
                )
        self.EndTransaction = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/EndTransaction',
                request_serializer=service__pb2.EndTransactionRequest.SerializeToString,
                response_deserializer=service__pb2.EndTransactionResponse.FromString,
                )
        self.Telemetry = channel.stream_stream(
                '/apache.rocketmq.v2.MessagingService/Telemetry',
                request_serializer=service__pb2.TelemetryCommand.SerializeToString,
                response_deserializer=service__pb2.TelemetryCommand.FromString,
                )
        self.NotifyClientTermination = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/NotifyClientTermination',
                request_serializer=service__pb2.NotifyClientTerminationRequest.SerializeToString,
                response_deserializer=service__pb2.NotifyClientTerminationResponse.FromString,
                )
        self.ChangeInvisibleDuration = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/ChangeInvisibleDuration',
                request_serializer=service__pb2.ChangeInvisibleDurationRequest.SerializeToString,
                response_deserializer=service__pb2.ChangeInvisibleDurationResponse.FromString,
                )
        self.RecallMessage = channel.unary_unary(
                '/apache.rocketmq.v2.MessagingService/RecallMessage',
                request_serializer=service__pb2.RecallMessageRequest.SerializeToString,
                response_deserializer=service__pb2.RecallMessageResponse.FromString,
                )


class MessagingServiceServicer(object):
    """For all the RPCs in MessagingService, the following error handling policies
    apply:

    If the request doesn't bear a valid authentication credential, return a
    response with common.status.code == `UNAUTHENTICATED`. If the authenticated
    user is not granted with sufficient permission to execute the requested
    operation, return a response with common.status.code == `PERMISSION_DENIED`.
    If the per-user-resource-based quota is exhausted, return a response with
    common.status.code == `RESOURCE_EXHAUSTED`. If any unexpected server-side
    errors raise, return a response with common.status.code == `INTERNAL`.
    """

    def QueryRoute(self, request, context):
        """Queries the route entries of the requested topic in the perspective of the
        given endpoints. On success, servers should return a collection of
        addressable message-queues. Note servers may return customized route
        entries based on endpoints provided.

        If the requested topic doesn't exist, returns `NOT_FOUND`.
        If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Heartbeat(self, request, context):
        """Producer or consumer sends HeartbeatRequest to servers periodically to
        keep-alive. Additionally, it also reports client-side configuration,
        including topic subscription, load-balancing group name, etc.

        Returns `OK` if success.

        If a client specifies a language that is not yet supported by servers,
        returns `INVALID_ARGUMENT`
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendMessage(self, request, context):
        """Delivers messages to brokers.
        Clients may further:
        1. Refine a message destination to message-queues which fulfills parts of
        FIFO semantic;
        2. Flag a message as transactional, which keeps it invisible to consumers
        until it commits;
        3. Time a message, making it invisible to consumers till specified
        time-point;
        4. And more...

        Returns message-id or transaction-id with status `OK` on success.

        If the destination topic doesn't exist, returns `NOT_FOUND`.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueryAssignment(self, request, context):
        """Queries the assigned route info of a topic for current consumer,
        the returned assignment result is decided by server-side load balancer.

        If the corresponding topic doesn't exist, returns `NOT_FOUND`.
        If the specific endpoints is empty, returns `INVALID_ARGUMENT`.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReceiveMessage(self, request, context):
        """Receives messages from the server in batch manner, returns a set of
        messages if success. The received messages should be acked or redelivered
        after processed.

        If the pending concurrent receive requests exceed the quota of the given
        consumer group, returns `UNAVAILABLE`. If the upstream store server hangs,
        return `DEADLINE_EXCEEDED` in a timely manner. If the corresponding topic
        or consumer group doesn't exist, returns `NOT_FOUND`. If there is no new
        message in the specific topic, returns `OK` with an empty message set.
        Please note that client may suffer from false empty responses.

        If failed to receive message from remote, server must return only one
        `ReceiveMessageResponse` as the reply to the request, whose `Status` indicates
        the specific reason of failure, otherwise, the reply is considered successful.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AckMessage(self, request, context):
        """Acknowledges the message associated with the `receipt_handle` or `offset`
        in the `AckMessageRequest`, it means the message has been successfully
        processed. Returns `OK` if the message server remove the relevant message
        successfully.

        If the given receipt_handle is illegal or out of date, returns
        `INVALID_ARGUMENT`.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ForwardMessageToDeadLetterQueue(self, request, context):
        """Forwards one message to dead letter queue if the max delivery attempts is
        exceeded by this message at client-side, return `OK` if success.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def PullMessage(self, request, context):
        """PullMessage and ReceiveMessage RPCs serve a similar purpose,
        which is to attempt to get messages from the server, but with different semantics.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateOffset(self, request, context):
        """Update the consumption progress of the designated queue of the
        consumer group to the remote.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetOffset(self, request, context):
        """Query the consumption progress of the designated queue of the
        consumer group to the remote.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueryOffset(self, request, context):
        """Query the offset of the designated queue by the query offset policy.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def EndTransaction(self, request, context):
        """Commits or rollback one transactional message.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Telemetry(self, request_iterator, context):
        """Once a client starts, it would immediately establishes bi-lateral stream
        RPCs with brokers, reporting its settings as the initiative command.

        When servers have need of inspecting client status, they would issue
        telemetry commands to clients. After executing received instructions,
        clients shall report command execution results through client-side streams.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def NotifyClientTermination(self, request, context):
        """Notify the server that the client is terminated.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ChangeInvisibleDuration(self, request, context):
        """Once a message is retrieved from consume queue on behalf of the group, it
        will be kept invisible to other clients of the same group for a period of
        time. The message is supposed to be processed within the invisible
        duration. If the client, which is in charge of the invisible message, is
        not capable of processing the message timely, it may use
        ChangeInvisibleDuration to lengthen invisible duration.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RecallMessage(self, request, context):
        """Recall a message,
        for delay message, should recall before delivery time, like the rollback operation of transaction message,
        for normal message, not supported for now.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_MessagingServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'QueryRoute': grpc.unary_unary_rpc_method_handler(
                    servicer.QueryRoute,
                    request_deserializer=service__pb2.QueryRouteRequest.FromString,
                    response_serializer=service__pb2.QueryRouteResponse.SerializeToString,
            ),
            'Heartbeat': grpc.unary_unary_rpc_method_handler(
                    servicer.Heartbeat,
                    request_deserializer=service__pb2.HeartbeatRequest.FromString,
                    response_serializer=service__pb2.HeartbeatResponse.SerializeToString,
            ),
            'SendMessage': grpc.unary_unary_rpc_method_handler(
                    servicer.SendMessage,
                    request_deserializer=service__pb2.SendMessageRequest.FromString,
                    response_serializer=service__pb2.SendMessageResponse.SerializeToString,
            ),
            'QueryAssignment': grpc.unary_unary_rpc_method_handler(
                    servicer.QueryAssignment,
                    request_deserializer=service__pb2.QueryAssignmentRequest.FromString,
                    response_serializer=service__pb2.QueryAssignmentResponse.SerializeToString,
            ),
            'ReceiveMessage': grpc.unary_stream_rpc_method_handler(
                    servicer.ReceiveMessage,
                    request_deserializer=service__pb2.ReceiveMessageRequest.FromString,
                    response_serializer=service__pb2.ReceiveMessageResponse.SerializeToString,
            ),
            'AckMessage': grpc.unary_unary_rpc_method_handler(
                    servicer.AckMessage,
                    request_deserializer=service__pb2.AckMessageRequest.FromString,
                    response_serializer=service__pb2.AckMessageResponse.SerializeToString,
            ),
            'ForwardMessageToDeadLetterQueue': grpc.unary_unary_rpc_method_handler(
                    servicer.ForwardMessageToDeadLetterQueue,
                    request_deserializer=service__pb2.ForwardMessageToDeadLetterQueueRequest.FromString,
                    response_serializer=service__pb2.ForwardMessageToDeadLetterQueueResponse.SerializeToString,
            ),
            'PullMessage': grpc.unary_stream_rpc_method_handler(
                    servicer.PullMessage,
                    request_deserializer=service__pb2.PullMessageRequest.FromString,
                    response_serializer=service__pb2.PullMessageResponse.SerializeToString,
            ),
            'UpdateOffset': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateOffset,
                    request_deserializer=service__pb2.UpdateOffsetRequest.FromString,
                    response_serializer=service__pb2.UpdateOffsetResponse.SerializeToString,
            ),
            'GetOffset': grpc.unary_unary_rpc_method_handler(
                    servicer.GetOffset,
                    request_deserializer=service__pb2.GetOffsetRequest.FromString,
                    response_serializer=service__pb2.GetOffsetResponse.SerializeToString,
            ),
            'QueryOffset': grpc.unary_unary_rpc_method_handler(
                    servicer.QueryOffset,
                    request_deserializer=service__pb2.QueryOffsetRequest.FromString,
                    response_serializer=service__pb2.QueryOffsetResponse.SerializeToString,
            ),
            'EndTransaction': grpc.unary_unary_rpc_method_handler(
                    servicer.EndTransaction,
                    request_deserializer=service__pb2.EndTransactionRequest.FromString,
                    response_serializer=service__pb2.EndTransactionResponse.SerializeToString,
            ),
            'Telemetry': grpc.stream_stream_rpc_method_handler(
                    servicer.Telemetry,
                    request_deserializer=service__pb2.TelemetryCommand.FromString,
                    response_serializer=service__pb2.TelemetryCommand.SerializeToString,
            ),
            'NotifyClientTermination': grpc.unary_unary_rpc_method_handler(
                    servicer.NotifyClientTermination,
                    request_deserializer=service__pb2.NotifyClientTerminationRequest.FromString,
                    response_serializer=service__pb2.NotifyClientTerminationResponse.SerializeToString,
            ),
            'ChangeInvisibleDuration': grpc.unary_unary_rpc_method_handler(
                    servicer.ChangeInvisibleDuration,
                    request_deserializer=service__pb2.ChangeInvisibleDurationRequest.FromString,
                    response_serializer=service__pb2.ChangeInvisibleDurationResponse.SerializeToString,
            ),
            'RecallMessage': grpc.unary_unary_rpc_method_handler(
                    servicer.RecallMessage,
                    request_deserializer=service__pb2.RecallMessageRequest.FromString,
                    response_serializer=service__pb2.RecallMessageResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'apache.rocketmq.v2.MessagingService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class MessagingService(object):
    """For all the RPCs in MessagingService, the following error handling policies
    apply:

    If the request doesn't bear a valid authentication credential, return a
    response with common.status.code == `UNAUTHENTICATED`. If the authenticated
    user is not granted with sufficient permission to execute the requested
    operation, return a response with common.status.code == `PERMISSION_DENIED`.
    If the per-user-resource-based quota is exhausted, return a response with
    common.status.code == `RESOURCE_EXHAUSTED`. If any unexpected server-side
    errors raise, return a response with common.status.code == `INTERNAL`.
    """

    @staticmethod
    def QueryRoute(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/QueryRoute',
            service__pb2.QueryRouteRequest.SerializeToString,
            service__pb2.QueryRouteResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Heartbeat(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/Heartbeat',
            service__pb2.HeartbeatRequest.SerializeToString,
            service__pb2.HeartbeatResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SendMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/SendMessage',
            service__pb2.SendMessageRequest.SerializeToString,
            service__pb2.SendMessageResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueryAssignment(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/QueryAssignment',
            service__pb2.QueryAssignmentRequest.SerializeToString,
            service__pb2.QueryAssignmentResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ReceiveMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/apache.rocketmq.v2.MessagingService/ReceiveMessage',
            service__pb2.ReceiveMessageRequest.SerializeToString,
            service__pb2.ReceiveMessageResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AckMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/AckMessage',
            service__pb2.AckMessageRequest.SerializeToString,
            service__pb2.AckMessageResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ForwardMessageToDeadLetterQueue(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/ForwardMessageToDeadLetterQueue',
            service__pb2.ForwardMessageToDeadLetterQueueRequest.SerializeToString,
            service__pb2.ForwardMessageToDeadLetterQueueResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def PullMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/apache.rocketmq.v2.MessagingService/PullMessage',
            service__pb2.PullMessageRequest.SerializeToString,
            service__pb2.PullMessageResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpdateOffset(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/UpdateOffset',
            service__pb2.UpdateOffsetRequest.SerializeToString,
            service__pb2.UpdateOffsetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetOffset(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/GetOffset',
            service__pb2.GetOffsetRequest.SerializeToString,
            service__pb2.GetOffsetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueryOffset(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/QueryOffset',
            service__pb2.QueryOffsetRequest.SerializeToString,
            service__pb2.QueryOffsetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def EndTransaction(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/EndTransaction',
            service__pb2.EndTransactionRequest.SerializeToString,
            service__pb2.EndTransactionResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Telemetry(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/apache.rocketmq.v2.MessagingService/Telemetry',
            service__pb2.TelemetryCommand.SerializeToString,
            service__pb2.TelemetryCommand.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def NotifyClientTermination(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/NotifyClientTermination',
            service__pb2.NotifyClientTerminationRequest.SerializeToString,
            service__pb2.NotifyClientTerminationResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ChangeInvisibleDuration(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/ChangeInvisibleDuration',
            service__pb2.ChangeInvisibleDurationRequest.SerializeToString,
            service__pb2.ChangeInvisibleDurationResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RecallMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/apache.rocketmq.v2.MessagingService/RecallMessage',
            service__pb2.RecallMessageRequest.SerializeToString,
            service__pb2.RecallMessageResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
