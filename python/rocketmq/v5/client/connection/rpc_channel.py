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
import time

import grpc
from grpc import ChannelConnectivity, aio
from grpc.aio import AioRpcError

from rocketmq.grpc_protocol import (Address, AddressScheme, Code, Endpoints,
                                    MessagingServiceStub)
from rocketmq.v5.exception import (IllegalArgumentException,
                                   UnsupportedException)
from rocketmq.v5.log import logger


class RpcAddress:
    """Represents a single broker host:port address.

    Wraps a protobuf ``Address`` with hash, equality, and ordering
    support for use in sets and sorted collections.
    """

    def __init__(self, address: Address):
        """Create an RPC address.

        Args:
            address: A gRPC ``Address`` protobuf message.
        """
        self.__host = address.host
        self.__port = address.port

    def __hash__(self) -> int:
        return hash(self.__str__())

    def __str__(self) -> str:
        return self.__host + ":" + str(self.__port)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RpcAddress):
            return False
        return self.__str__() == (other.__str__())

    def __lt__(self, other):
        if not isinstance(other, RpcAddress):
            return False
        return self.__str__() < (other.__str__())

    def address0(self):
        """Convert back to a protobuf ``Address``.

        Returns:
            An ``Address`` protobuf message.
        """
        address = Address()
        address.host = self.__host
        address.port = self.__port
        return address


class RpcEndpoints:
    """Encapsulates broker endpoint addresses and connection scheme.

    Parses a protobuf ``Endpoints`` into a usable connection string
    (the *facade*) for gRPC channel creation. Supports IPv4, IPv6,
    and DNS schemes.
    """

    def __init__(self, endpoints: Endpoints):
        """Build RPC endpoints from protobuf data.

        Args:
            endpoints: A gRPC ``Endpoints`` protobuf message.

        Raises:
            UnsupportedException: If DNS scheme has multiple addresses.
        """
        self.__endpoints = endpoints
        self.__scheme = endpoints.scheme
        self.__addresses = set(
            map(lambda address: RpcAddress(address), endpoints.addresses)
        )
        if self.__scheme == AddressScheme.DOMAIN_NAME and len(self.__addresses) > 1:
            raise UnsupportedException(
                "Multiple addresses not allowed in domain schema"
            )
        self.__facade, self.__endpoint_desc = self.__facade()

    def __hash__(self) -> int:
        return hash(str(self.__scheme) + ":" + self.__facade)

    def __eq__(self, other):
        if not isinstance(other, RpcEndpoints):
            return False
        return self.__facade == other.__facade and self.__scheme == other.__scheme

    def __str__(self):
        return self.__endpoint_desc

    def __facade(self):
        if (
            self.__scheme is None
            or len(self.__addresses) == 0
            or self.__scheme == AddressScheme.ADDRESS_SCHEME_UNSPECIFIED
        ):
            return "", ""

        prefix = "dns:"
        if self.__scheme == AddressScheme.IPv4:
            prefix = "ipv4:"
        elif self.__scheme == AddressScheme.IPv6:
            prefix = "ipv6:"

        # formatted as: ip:port, ip:port, ip:port
        sorted_list = sorted(self.__addresses)
        ret = ""
        for address in sorted_list:
            ret = ret + address.__str__() + ","
        return prefix + ret[0:len(ret) - 1], ret[0:len(ret) - 1]

    @property
    def endpoints(self):
        """The underlying protobuf ``Endpoints`` message."""
        return self.__endpoints

    @property
    def facade(self):
        """The gRPC connection string (e.g., ``"ipv4:192.168.1.1:8080"``)."""
        return self.__facade


class RpcStreamStreamCall:
    """Wrapper around a bidirectional gRPC stream for telemetry.

    Manages server-side stream reading (settings sync, transaction recovery)
    and client-side stream writing. Associated with a specific set of
    broker endpoints and a handler for processing incoming messages.
    """

    def __init__(self, endpoints: RpcEndpoints, stream_stream_call, handler):
        """Create a stream call wrapper.

        Args:
            endpoints: Broker endpoint addresses for this stream.
            stream_stream_call: The raw gRPC bidirectional stream object.
            handler: Callback handler for processing server-side messages.
        """
        self.__endpoints = endpoints
        self.__stream_stream_call = stream_stream_call  # grpc stream_stream_call
        self.__handler = handler  # handler responsible for handling data from the server side stream.

    async def start_stream_read(self):
        """Continuously read from the gRPC stream and dispatch messages.

        Handles two message types from the server:
        - ``settings``: Updates client configuration and metrics.
        - ``recover_orphaned_transaction_command``: Triggers transaction recovery.
        """
        if self.__stream_stream_call is not None:
            try:
                while True:
                    res = await self.__stream_stream_call.read()
                    if res.HasField("settings"):
                        # read a response for send setting result
                        if res and res.status.code == Code.OK:
                            logger.debug(
                                f"{ self.__handler} sync setting success. response status code: {res.status.code}"
                            )
                            if res.settings:
                                # sync setting first to ensure consumer initialization
                                self.__handler.reset_setting(res.settings)
                                if res.settings.metric:
                                    # reset metrics if needed
                                    self.__handler.reset_metric(res.settings.metric)
                    elif res.HasField("recover_orphaned_transaction_command"):
                        # sever check for a transaction message
                        if self.__handler:
                            transaction_id = (
                                res.recover_orphaned_transaction_command.transaction_id
                            )
                            message = res.recover_orphaned_transaction_command.message
                            self.__handler.on_recover_orphaned_transaction_command(
                                self.__endpoints, message, transaction_id
                            )
            except AioRpcError as e:
                logger.warn(
                    f"{ self.__handler} read stream from endpoints {self.__endpoints} occurred AioRpcError. code: {e.code()}, message: {e.details()}"
                )
            except Exception as e:
                logger.error(
                    f"{ self.__handler} read stream from endpoints {self.__endpoints} exception, {e}"
                )

    async def stream_write(self, req):
        """Write a request to the gRPC stream.

        Args:
            req: The protobuf message to send.

        Raises:
            Exception: If the stream write fails.
        """
        if self.__stream_stream_call:
            await self.__stream_stream_call.write(req)

    def close(self):
        """Cancel the underlying gRPC stream, releasing associated resources."""
        if self.__stream_stream_call:
            self.__stream_stream_call.cancel()


class RpcChannel:
    """Manages a gRPC channel to a specific set of broker endpoints.

    Handles channel creation (secure/insecure), lifecycle management,
    and telemetry stream registration. Each channel instance corresponds
    to one broker address group.
    """

    def __init__(self, endpoints: RpcEndpoints, tls_enabled=False):
        """Create an RPC channel.

        Args:
            endpoints: Broker endpoint addresses to connect to.
            tls_enabled: Whether to use TLS (secure channel) or insecure channel.
        """
        self.__async_channel = None
        self.__async_stub = None
        self.__telemetry_stream_stream_call = None
        self.__tls_enabled = tls_enabled
        self.__endpoints = endpoints
        self.__update_time = int(time.time())

    def create_channel(self, loop):
        """Create the gRPC async channel on the given event loop.

        Args:
            loop: The asyncio event loop to bind the channel to.
        """
        # create grpc channel with the given loop
        # assert loop == RpcClient._io_loop
        asyncio.set_event_loop(loop)
        self.__create_aio_channel()

    def close_channel(self, loop):
        """Close the gRPC channel and associated telemetry stream.

        Gracefully shuts down the channel, cancels active streams,
        and clears all internal references.

        Args:
            loop: The asyncio event loop the channel is bound to.
        """
        if self.__async_channel:
            # close stream_stream_call
            if self.__telemetry_stream_stream_call:
                self.__telemetry_stream_stream_call.close()
                self.__telemetry_stream_stream_call = None
                logger.info(
                    f"channel[{self.__endpoints}] close stream_stream_call success."
                )
            if self.channel_state() is not ChannelConnectivity.SHUTDOWN:
                # close grpc channel
                asyncio.run_coroutine_threadsafe(self.__async_channel.close(), loop)
                self.__async_channel = None
                logger.info(f"channel[{self.__endpoints}] close success.")
            self.__async_stub = None
            self.__endpoints = None
            self.__update_time = None

    def channel_state(self, wait_for_ready=True):
        """Get the current connectivity state of the gRPC channel.

        Args:
            wait_for_ready: Whether to wait for a state change before returning.

        Returns:
            A ``ChannelConnectivity`` enum value.
        """
        if self.__async_channel:
            return self.__async_channel.get_state(wait_for_ready)
        return None

    def register_telemetry_stream_stream_call(self, stream_stream_call, handler):
        """Register a new bidirectional telemetry stream.

        Closes any existing stream before registering the new one.

        Args:
            stream_stream_call: Raw gRPC bidirectional stream object.
            handler: Callback handler for processing server messages.
        """
        if self.__telemetry_stream_stream_call is not None:
            self.__telemetry_stream_stream_call.close()
        self.__telemetry_stream_stream_call = RpcStreamStreamCall(
            self.__endpoints, stream_stream_call, handler
        )

    def __create_aio_channel(self):
        try:
            if not self.__endpoints:
                raise IllegalArgumentException(
                    "create_aio_channel exception, endpoints is None"
                )
            else:
                options = [
                    ("grpc.enable_retries", 0),
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                    ("grpc.use_local_subchannel_pool", 1),
                ]
                if self.__tls_enabled:
                    self.__async_channel = aio.secure_channel(
                        self.__endpoints.facade, grpc.ssl_channel_credentials(), options
                    )
                else:
                    self.__async_channel = aio.insecure_channel(
                        self.__endpoints.facade, options
                    )
                self.__async_stub = MessagingServiceStub(self.__async_channel)
                logger.info(
                    f"create_aio_channel to [{self.__endpoints}] success. channel state:{self.__async_channel.get_state()}"
                )
        except Exception as e:
            logger.error(
                f"create_aio_channel to [{self.__endpoints}] exception: {e}"
            )
            raise e

    @property
    def async_stub(self):
        """The async gRPC ``MessagingServiceStub`` for making RPC calls."""
        return self.__async_stub

    @property
    def telemetry_stream_stream_call(self):
        """The active :class:`RpcStreamStreamCall` for telemetry, or ``None``."""
        return self.__telemetry_stream_stream_call

    @property
    def update_time(self):
        """Unix timestamp (seconds) of when this channel was last created or updated."""
        return self.__update_time

    @update_time.setter
    def update_time(self, update_time):
        """Set the channel's last update timestamp."""
        self.__update_time = update_time
