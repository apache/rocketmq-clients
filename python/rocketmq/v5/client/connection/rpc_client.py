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

import asyncio
import threading
import time
from concurrent.futures import Future
from typing import Optional

from grpc import ChannelConnectivity

from rocketmq.grpc_protocol import (AckMessageRequest,
                                    ChangeInvisibleDurationRequest,
                                    EndTransactionRequest,
                                    ForwardMessageToDeadLetterQueueRequest,
                                    HeartbeatRequest,
                                    NotifyClientTerminationRequest,
                                    QueryAssignmentRequest, QueryRouteRequest,
                                    RecallMessageRequest,
                                    ReceiveMessageRequest, SendMessageRequest,
                                    SyncLiteSubscriptionRequest,
                                    TelemetryCommand)
from rocketmq.v5.client.connection import RpcChannel, RpcEndpoints
from rocketmq.v5.log import logger
from rocketmq.v5.util import ConcurrentMap


class RpcClient:
    """Manages gRPC channels to RocketMQ brokers.

    Provides asynchronous RPC methods for all MessageService operations
    (send, receive, ack, query route, heartbeat, etc.). All RPC calls are
    executed on a shared asyncio event loop running in a dedicated I/O thread.
    """
    _instance_lock = threading.Lock()
    _channel_lock = threading.Lock()
    _io_loop = None  # event loop for all async io
    _io_loop_thread = None  # thread for io loop
    RPC_CLIENT_MAX_IDLE_SECONDS = 60 * 30

    def __init__(self, tls_enable=False):
        with RpcClient._instance_lock:
            # start an event loop for async io
            if RpcClient._io_loop is None:
                initialized_event = threading.Event()
                RpcClient._io_loop_thread = threading.Thread(
                    target=RpcClient.__init_io_loop,
                    args=(initialized_event,),
                    name="channel_io_loop_thread",
                    daemon=True
                )
                RpcClient._io_loop_thread.start()
                # waiting for thread start success
                initialized_event.wait()
        self.channels = ConcurrentMap()
        self.__enable_retrieve_channel = True
        self.__tls_enable = tls_enable

    def retrieve_or_create_channel(self, endpoints: RpcEndpoints):
        """Get an existing gRPC channel or create a new one for the given endpoints.

        Args:
            endpoints: The target broker endpoints.

        Returns:
            A :class:`RpcChannel` for communication.

        Raises:
            Exception: If RpcClient is not running.
        """
        if not self.__enable_retrieve_channel:
            raise Exception("RpcClient is not running.")
        try:
            # get or create a new grpc channel
            channel = self.__get_channel(endpoints)
            if channel is not None:
                channel.update_time = int(time.time())
            else:
                with RpcClient._channel_lock:
                    channel = RpcChannel(endpoints, self.__tls_enable)
                    channel.create_channel(RpcClient.get_channel_io_loop())
                    self.__put_channel(endpoints, channel)
            return channel
        except Exception as e:
            logger.error(f"retrieve or create channel exception: {e}")
            raise e

    def clear_idle_rpc_channels(self):
        """Close and remove gRPC channels that have been idle for more than 30 minutes."""
        items = self.channels.items()
        now = int(time.time())
        idle_endpoints = list()
        for endpoints, channel in items:
            if now - channel.update_time > RpcClient.RPC_CLIENT_MAX_IDLE_SECONDS:
                idle_endpoints.append(endpoints)
        if len(idle_endpoints) > 0:
            with RpcClient._channel_lock:
                for endpoints in idle_endpoints:
                    logger.info(f"remove idle channel {endpoints}")
                    self.__close_rpc_channel(endpoints)
                    self.channels.remove(endpoints)

    def stop(self):
        with RpcClient._channel_lock:
            self.__enable_retrieve_channel = False
            all_endpoints = self.channels.keys()
            for endpoints in all_endpoints:
                self.__close_rpc_channel(endpoints)

    @staticmethod
    def get_channel_io_loop():
        return RpcClient._io_loop

    def query_topic_route_async(
        self, endpoints: RpcEndpoints, req: QueryRouteRequest, metadata, timeout=3
    ):
        """Query topic route information asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The query request.
            metadata: gRPC metadata (authentication headers).
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the route response.
        """
        return RpcClient.__run_message_service_async(
            self.__query_route_async_0(
                endpoints, req, metadata=metadata, timeout=timeout
            )
        )

    def send_message_async(
        self, endpoints: RpcEndpoints, req: SendMessageRequest, metadata, timeout=3
    ):
        """Send a message asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The send request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the send response.
        """
        return RpcClient.__run_message_service_async(
            self.__send_message_0(endpoints, req, metadata=metadata, timeout=timeout)
        )

    def receive_message_async(
        self, endpoints: RpcEndpoints, req: ReceiveMessageRequest, metadata, timeout=3
    ):
        """Receive messages asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The reception request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the reception response.
        """
        return RpcClient.__run_message_service_async(
            self.__receive_message_0(endpoints, req, metadata=metadata, timeout=timeout)
        )

    async def receive_message(
            self, endpoints, req, metadata, timeout=3
    ):
        """Create a receive-message stream asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The reception request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``grpc.aio.UnaryStreamCall`` for reading reception responses.
        """

        return await self.__receive_message_0(
            endpoints,
            req,
            metadata=metadata,
            timeout=timeout,
        )

    def ack_message_async(
        self, endpoints: RpcEndpoints, req: AckMessageRequest, metadata, timeout=3
    ):
        """Acknowledge a message asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The ack request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the ack response.
        """
        return RpcClient.__run_message_service_async(
            self.__ack_message_0(endpoints, req, metadata=metadata, timeout=timeout)
        )

    def change_invisible_duration_async(
        self,
        endpoints: RpcEndpoints,
        req: ChangeInvisibleDurationRequest,
        metadata,
        timeout=3,
    ):
        """Change the invisible duration (visibility timeout) of a message asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The change invisible duration request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__change_invisible_duration_0(
                endpoints, req, metadata=metadata, timeout=timeout
            )
        )

    def heartbeat_async(
        self, endpoints: RpcEndpoints, req: HeartbeatRequest, metadata, timeout=3
    ):
        """Send heartbeat to a broker asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The heartbeat request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the heartbeat response.
        """
        return RpcClient.__run_message_service_async(
            self.__heartbeat_async_0(endpoints, req, metadata=metadata, timeout=timeout)
        )

    def telemetry_write_async(self, endpoints: RpcEndpoints, req: TelemetryCommand):
        """Write a telemetry command to the bidirectional stream asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The telemetry command to send.

        Returns:
            A ``concurrent.futures.Future`` resolving when write completes.
        """
        return RpcClient.__run_message_service_async(
            self.retrieve_or_create_channel(
                endpoints
            ).telemetry_stream_stream_call.stream_write(req)
        )

    def end_transaction_async(
        self, endpoints: RpcEndpoints, req: EndTransactionRequest, metadata, timeout=3
    ):
        """End a transactional message (commit or rollback) asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The end transaction request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__end_transaction_0(endpoints, req, metadata=metadata, timeout=timeout)
        )

    def notify_client_termination_async(
        self,
        endpoints: RpcEndpoints,
        req: NotifyClientTerminationRequest,
        metadata,
        timeout=3,
    ):
        """Notify the broker of client termination asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The termination notification request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__notify_client_termination_0(
                endpoints, req, metadata=metadata, timeout=timeout
            )
        )

    def recall_message_async(self, endpoints: RpcEndpoints, req: RecallMessageRequest, metadata, timeout=3):
        """Recall a previously sent scheduled message asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The recall message request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__recall_message_0(endpoints, req, metadata=metadata, timeout=timeout))

    def query_assignment_async(self, endpoints: RpcEndpoints, req: QueryAssignmentRequest, metadata, timeout=3):
        """Query message queue assignments for a consumer from the broker asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The query assignment request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the assignment response.
        """
        return RpcClient.__run_message_service_async(
            self.__query_assignment_0(endpoints, req, metadata=metadata, timeout=timeout))

    def forward_message_to_dead_letter_queue_async(self, endpoints: RpcEndpoints, req: ForwardMessageToDeadLetterQueueRequest, metadata, timeout=3):
        """Forward a message to the dead letter queue after repeated consumption failures asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The forward to DLQ request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__forward_message_to_dead_letter_queue_async_0(endpoints, req, metadata=metadata, timeout=timeout))

    def sync_lite_subscription_async(self, endpoints: RpcEndpoints, req: SyncLiteSubscriptionRequest, metadata, timeout=3):
        """Synchronize lite subscription information with the broker asynchronously.

        Args:
            endpoints: Target broker endpoints.
            req: The sync lite subscription request.
            metadata: gRPC metadata.
            timeout: Request timeout in seconds.

        Returns:
            A ``concurrent.futures.Future`` resolving to the response.
        """
        return RpcClient.__run_message_service_async(
            self.__sync_lite_subscription_0(endpoints, req, metadata=metadata, timeout=timeout))

    def telemetry_stream(
        self, endpoints: RpcEndpoints, client, metadata, rebuild, timeout=3000
    ):
        """Establish or rebuild a bidirectional telemetry stream with a broker.

        The stream is used for server-to-client settings push and client-to-server
        metrics reporting.

        Args:
            endpoints: Target broker endpoints.
            client: The client handler to receive server commands.
            metadata: gRPC metadata.
            rebuild: If True, rebuild an existing stream.
            timeout: Stream timeout in seconds.
        """
        # build grpc stream_stream_call

        channel = self.retrieve_or_create_channel(endpoints)
        stream = channel.async_stub.Telemetry(
            metadata=metadata, timeout=timeout, wait_for_ready=True
        )
        channel.register_telemetry_stream_stream_call(stream, client)
        asyncio.run_coroutine_threadsafe(
            channel.telemetry_stream_stream_call.start_stream_read(),
            RpcClient.get_channel_io_loop(),
        )
        logger.info(
            f"{client} rebuild stream_steam_call to {endpoints}."
            if rebuild
            else f"{client} create stream_steam_call to {endpoints}."
        )
        return channel

    async def __query_route_async_0(
        self, endpoints: RpcEndpoints, req: QueryRouteRequest, metadata, timeout=3
    ):
        return await self.retrieve_or_create_channel(endpoints).async_stub.QueryRoute(
            req, metadata=metadata, timeout=timeout
        )

    async def __send_message_0(
        self, endpoints: RpcEndpoints, req: SendMessageRequest, metadata, timeout=3
    ):
        return await self.retrieve_or_create_channel(endpoints).async_stub.SendMessage(
            req, metadata=metadata, timeout=timeout
        )

    async def __receive_message_0(
        self, endpoints: RpcEndpoints, req: ReceiveMessageRequest, metadata, timeout=3
    ):
        return self.retrieve_or_create_channel(endpoints).async_stub.ReceiveMessage(
            req, metadata=metadata, timeout=timeout
        )

    async def __ack_message_0(
        self, endpoints: RpcEndpoints, req: AckMessageRequest, metadata, timeout=3
    ):
        return await self.retrieve_or_create_channel(endpoints).async_stub.AckMessage(
            req, metadata=metadata, timeout=timeout
        )

    async def __heartbeat_async_0(
        self, endpoints: RpcEndpoints, req: HeartbeatRequest, metadata, timeout=3
    ):
        return await self.retrieve_or_create_channel(endpoints).async_stub.Heartbeat(
            req, metadata=metadata, timeout=timeout
        )

    async def __change_invisible_duration_0(
        self,
        endpoints: RpcEndpoints,
        req: ChangeInvisibleDurationRequest,
        metadata,
        timeout=3,
    ):
        return await self.retrieve_or_create_channel(
            endpoints
        ).async_stub.ChangeInvisibleDuration(req, metadata=metadata, timeout=timeout)

    async def __end_transaction_0(
        self, endpoints: RpcEndpoints, req: EndTransactionRequest, metadata, timeout=3
    ):
        return await self.retrieve_or_create_channel(
            endpoints
        ).async_stub.EndTransaction(req, metadata=metadata, timeout=timeout)

    async def __notify_client_termination_0(
        self,
        endpoints: RpcEndpoints,
        req: NotifyClientTerminationRequest,
        metadata,
        timeout=3,
    ):
        return await self.retrieve_or_create_channel(
            endpoints
        ).async_stub.NotifyClientTermination(req, metadata=metadata, timeout=timeout)

    async def __recall_message_0(self, endpoints: RpcEndpoints, req: RecallMessageRequest, metadata, timeout=3):
        return await self.retrieve_or_create_channel(endpoints).async_stub.RecallMessage(req, metadata=metadata,
                                                                                         timeout=timeout)

    async def __query_assignment_0(self, endpoints: RpcEndpoints, req: QueryAssignmentRequest, metadata, timeout=3):
        return await self.retrieve_or_create_channel(endpoints).async_stub.QueryAssignment(req, metadata=metadata,
                                                                                           timeout=timeout)

    async def __forward_message_to_dead_letter_queue_async_0(self, endpoints: RpcEndpoints,
                                                             req: ForwardMessageToDeadLetterQueueRequest, metadata,
                                                             timeout=3):
        return await self.retrieve_or_create_channel(endpoints).async_stub.ForwardMessageToDeadLetterQueue(req, metadata=metadata,
                                                                                                           timeout=timeout)

    async def __sync_lite_subscription_0(self, endpoints: RpcEndpoints,
                                         req: SyncLiteSubscriptionRequest, metadata, timeout=3):
        return await self.retrieve_or_create_channel(endpoints).async_stub.SyncLiteSubscription(req, metadata=metadata, timeout=timeout)

    async def __create_channel_async(self, endpoints: RpcEndpoints):
        return self.retrieve_or_create_channel(endpoints)

    def __get_channel(self, endpoints: RpcEndpoints) -> Optional[RpcChannel]:
        return self.channels.get(endpoints)

    def __put_channel(self, endpoints: RpcEndpoints, channel):
        self.channels.put(endpoints, channel)

    def __close_rpc_channel(self, endpoints: RpcEndpoints):
        channel = self.__get_channel(endpoints)
        if (
            channel is not None
            and channel.channel_state() is not ChannelConnectivity.SHUTDOWN
        ):
            try:
                channel.close_channel(RpcClient.get_channel_io_loop())
                self.channels.remove(endpoints)
            except Exception as e:
                logger.error(f"close channel {endpoints} error: {e}")
                raise e

    @staticmethod
    def __init_io_loop(initialized_event):
        """Initialize the shared asyncio event loop in a dedicated I/O thread.

        All ``RpcClient`` instances share this single event loop for gRPC
        asynchronous operations. The loop is created once and runs forever
        until the process exits.

        Args:
            initialized_event: Threading event to signal that the loop is ready.
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            RpcClient._io_loop = loop
            initialized_event.set()
            logger.info("start io loop thread success.")
            loop.run_forever()
        except Exception as e:
            logger.error(f"start io loop thread exception: {e}")

    @staticmethod
    def __run_message_service_async(func):
        """Schedule an async gRPC coroutine on the shared I/O event loop.

        Args:
            func: An async coroutine to execute.

        Returns:
            A ``concurrent.futures.Future`` that resolves when the coroutine completes.
        """
        try:
            return asyncio.run_coroutine_threadsafe(
                func, RpcClient.get_channel_io_loop()
            )
        except Exception as e:
            future = Future()
            future.set_exception(e)
            return future
