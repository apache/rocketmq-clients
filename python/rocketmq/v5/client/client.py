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

import functools
import os
import threading
from concurrent.futures import ThreadPoolExecutor

from rocketmq.grpc_protocol import ClientType, Code
from rocketmq.v5.client.client_route_manager import ClientRouteManager
from rocketmq.v5.client.client_telemetry_manager import ClientTelemetryManager
from rocketmq.v5.client.connection import RpcClient
from rocketmq.v5.client.metrics import ClientMetrics
from rocketmq.v5.client.scheduler import ClientScheduler
from rocketmq.v5.exception import (IllegalArgumentException,
                                   IllegalStateException)
from rocketmq.v5.log import logger
from rocketmq.v5.util import ClientId, Misc, Signature


class Client:

    def __init__(
        self, client_configuration, topics, client_type, tls_enable=False
    ):
        if client_configuration is None:
            raise IllegalArgumentException("clientConfiguration should not be null.")
        self.__client_configuration = client_configuration
        self.__client_type = client_type
        self.__client_id = ClientId().client_id
        self.__rpc_client = RpcClient(tls_enable)
        self.__client_metrics = ClientMetrics(self.__client_id, client_configuration)
        self.__heartbeat_scheduler = None
        self.__clear_idle_rpc_channels_scheduler = None
        if topics:
            self.__topics = set(
                filter(lambda topic: Misc.is_valid_topic(topic), topics)
            )
        else:
            self.__topics = set()
        self.__client_callback_executor = None
        self.__is_running = False
        self.__had_shutdown = False
        self._init_settings_event = threading.Event()

        self.__route_manager = ClientRouteManager(self)
        self.__telemetry_manager = ClientTelemetryManager(self)

    def startup(self):
        """Start the client and establish connections to the broker.

        Performs the following in sequence:
        1. Calls subclass ``_pre_start()`` hook.
        2. Updates topic route data for all configured topics.
        3. Waits for initial settings from the server.
        4. Starts schedulers (route update, heartbeat, telemetry, idle cleanup).
        5. Starts the async RPC callback executor thread pool.
        6. Calls subclass ``_on_start()`` hook.

        Raises:
            Exception: If startup fails (topic route, settings, or scheduler).
        """
        try:
            if self.__had_shutdown:
                raise Exception(
                    f"{self} had shutdown, can't startup again."
                )

            self._pre_start()
            self.__init_routes()
            self.__start_scheduler()
            self.__start_async_rpc_callback_executor()
            self._on_start()
            self.__is_running = True
        except Exception as e:
            self.__is_running = False
            self.__stop_client_threads()
            self._on_start_failure()
            logger.error(f"{self} startup exception:  {e}")
            raise e

    def __str__(self):
        return f"{ClientType.Name(self.client_type)}, client_id:{self.client_id}"

    def shutdown(self):
        """Shutdown the client and release all resources.

        Stops all schedulers, the async callback executor, closes gRPC connections,
        clears topic route cache, and sends a termination notification to the server.

        Raises:
            IllegalStateException: If client is not running or already shutdown.
        """
        if not self.is_running:
            logger.warn(f"{self} is not running, can't shutdown")
            return

        if self.__had_shutdown:
            logger.warn(f"{self} had shutdown, can't shutdown again")
            return

        self._pre_shutdown()

        try:
            self.__stop_client_threads()
            self.__notify_client_termination()
            self.__rpc_client.stop()
            # self.__topic_route_cache.clear()
            self.__route_manager.clear()
            self.__topics.clear()
            self._init_settings_event = None
            self.__had_shutdown = True
            self.__is_running = False
        except Exception as e:
            logger.error(f"{self} shutdown exception: {e}")
            raise e

    def sign(self):
        """Generate signature metadata for gRPC RPC calls.

        Returns:
            A metadata dict/list containing authentication headers (ak, sk,
            client_id, timestamp) for the current request.
        """
        return Signature.metadata(self.__client_configuration, self.__client_id)

    def on_new_endpoints(self, endpoints):
        """Handle newly discovered broker endpoints.

        Called by :class:`ClientRouteManager` when route data changes and new
        endpoints are found. Establishes a telemetry stream and sends settings
        to the new endpoints.

        Args:
            endpoints: The newly discovered :class:`RpcEndpoints`.
        """
        """new endpoints handler (used by route_manager)"""
        self.__telemetry_manager.retrieve_telemetry_stream_stream_call(endpoints)
        self.__telemetry_manager.setting_write(endpoints)

    def reset_metric(self, metric):
        self.__client_metrics.reset_metrics(metric)

    def update_queue_selector(self, topic, topic_route):
        """each subclass implements its own queue selector"""
        pass

    def reset_setting(self, settings):
        """each subclass implements sync setting from server"""
        pass

    def sync_setting_req(self, endpoints):
        """each subclass implements its own telemetry settings scheme"""
        pass

    def _pre_start(self):
        """each subclass implements its own actions before startup"""
        pass

    def _on_start(self):
        """each subclass implements its own actions after a successful startup"""
        pass

    def _pre_shutdown(self):
        """each subclass implements its own actions before shutdown"""
        pass

    def _on_start_failure(self):
        """each subclass implements its own actions after a startup failure"""
        pass

    def _heartbeat_req(self):
        """each subclass implements its own heartbeat request"""
        pass

    def _notify_client_termination_req(self):
        """each subclass implements its own client termination request"""
        pass

    def __init_routes(self):
        # pre update topic route for producer or consumer.PushConsumer must be initialized with topics.
        # Producer and SimpleConsumer can be initialized without topics
        for topic in self.__topics:
            if not self.__route_manager.update_topic_route(topic):
                logger.error(f"update topic: {topic} route raise exception when client startup")

        if not self.__client_type == ClientType.PRODUCER and not self.__client_type == ClientType.SIMPLE_CONSUMER and not self.__client_type == ClientType.LITE_SIMPLE_CONSUMER:
            # waiting for settings from server
            if not self._init_settings_event.wait(timeout=10):
                raise IllegalStateException(
                    f"{self} failed to receive initial settings from server within 10s"
                )

    def __start_scheduler(self):
        # start schedulers in different threads, each thread use the same asyncio event loop.
        try:
            self.__route_manager.start_scheduler(self._rpc_channel_io_loop())
            logger.info("start topic route scheduler success.")
            # send heartbeat to all endpoints every 10 seconds
            self.__heartbeat_scheduler = ClientScheduler(f"{self.__client_id}_heartbeat_schedule_thread", self.__do_heartbeat, 1, 10,
                                                         self._rpc_channel_io_loop())
            self.__heartbeat_scheduler.start_scheduler()
            logger.info("start heartbeat scheduler success.")
            self.__telemetry_manager.start_scheduler(self._rpc_channel_io_loop())
            logger.info("start sync setting scheduler success.")
            # clear unused grpc channel(>30 minutes) every 60 seconds
            self.__clear_idle_rpc_channels_scheduler = ClientScheduler(f"{self.__client_id}_clear_idle_rpc_channel_schedule_thread", self.__do_clear_idle_rpc_channels, 5, 60,
                                                                       self._rpc_channel_io_loop())
            self.__clear_idle_rpc_channels_scheduler.start_scheduler()
            logger.info("start clear idle rpc channels scheduler success.")
        except Exception as e:
            logger.info(f"start scheduler exception: {e}")
            self.__stop_client_threads()
            raise e

    # schedule task #

    def __do_heartbeat(self):
        logger.debug(f"{self} run send heartbeat in scheduler.")
        # all_endpoints = self.__get_all_endpoints().values()
        all_endpoints = self.__route_manager.get_all_endpoints().values()
        for endpoints in all_endpoints:
            self.__heartbeat_async(endpoints)

    def __do_clear_idle_rpc_channels(self):
        logger.debug(
            f"{self} run scheduler for clear idle rpc channels."
        )
        self.__rpc_client.clear_idle_rpc_channels()

    def __start_async_rpc_callback_executor(self):
        # to handle callback when using async method such as send_async(), receive_async().
        # switches user's callback thread from RpcClient's _io_loop_thread to client's client_callback_worker_thread
        try:
            workers = os.cpu_count()
            if not workers:
                workers = 4
            self.__client_callback_executor = ThreadPoolExecutor(max_workers=workers,
                                                                 thread_name_prefix=f"client_callback_worker_{self.__client_id}")
            logger.info(f"{self} start callback executor success. max_workers:{workers}")
        except Exception as e:
            logger.error(f"{self} start async rpc callback raise exception: {e}")
            raise e

    @staticmethod
    def __handle_callback(callback_result):
        if callback_result.is_success:
            callback_result.future.set_result(callback_result.result)
        else:
            callback_result.future.set_exception(callback_result.result)

    def _retrieve_topic_route_data(self, topic):
        route = self.__route_manager.retrieve_topic_route_data(topic)
        if topic not in self.__topics:
            self.__topics.add(topic)
        return route

    def _remove_unused_topic_route_data(self, topic):
        self.__route_manager.remove_topic_route_data(topic)
        self.__topics.remove(topic)

    def _rpc_channel_io_loop(self):
        return self.__rpc_client.get_channel_io_loop()

    def _submit_callback(self, callback_result):
        self.__client_callback_executor.submit(Client.__handle_callback, callback_result)

    # heartbeat #

    def __heartbeat_async(self, endpoints):
        req = self._heartbeat_req()
        callback = functools.partial(self.__heartbeat_callback, endpoints=endpoints)
        future = self.__rpc_client.heartbeat_async(
            endpoints,
            req,
            metadata=self.sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.add_done_callback(callback)

    def __heartbeat_callback(self, future, endpoints):
        try:
            res = future.result()
            if res and res.status.code == Code.OK:
                logger.info(
                    f"{self} send heartbeat to {endpoints} success."
                )
            else:
                if res:
                    logger.error(
                        f"{self} send heartbeat to {endpoints} error, code:{res.status.code}, message:{res.status.message}."
                    )
                else:
                    logger.error(
                        f"{self} send heartbeat to {endpoints} error, response is none."
                    )
        except Exception as e:
            logger.error(
                f"{self} send heartbeat to {endpoints} exception, e: {e}"
            )
            raise e

    # client termination #

    def __client_termination(self, endpoints):
        req = self._notify_client_termination_req()
        future = self.__rpc_client.notify_client_termination_async(
            endpoints,
            req,
            metadata=self.sign(),
            timeout=self.__client_configuration.request_timeout,
        )
        future.result()

    def __notify_client_termination(self):
        # all_endpoints = self.__get_all_endpoints()
        all_endpoints = self.__route_manager.get_all_endpoints()
        for endpoints in all_endpoints.values():
            try:
                self.__client_termination(endpoints)
            except Exception as e:
                logger.error(f"notify client termination to {endpoints} exception: {e}")

    def __stop_client_threads(self):
        self.__route_manager.stop_scheduler()
        if self.__heartbeat_scheduler:
            self.__heartbeat_scheduler.stop_scheduler()
            self.__heartbeat_scheduler = None
        self.__telemetry_manager.stop_scheduler()
        if self.__clear_idle_rpc_channels_scheduler:
            self.__clear_idle_rpc_channels_scheduler.stop_scheduler()
            self.__clear_idle_rpc_channels_scheduler = None
        if self.__client_callback_executor:
            self.__client_callback_executor.shutdown()
            self.__client_callback_executor = None
            logger.info("stop client callback executor.")

    @property
    def is_running(self):
        return self.__is_running

    @property
    def client_id(self):
        return self.__client_id

    @property
    def topics(self):
        return self.__topics

    @property
    def client_configuration(self):
        return self.__client_configuration

    @property
    def client_type(self):
        return self.__client_type

    @property
    def rpc_client(self):
        return self.__rpc_client

    @property
    def client_metrics(self):
        return self.__client_metrics

    @property
    def route_manager(self):
        return self.__route_manager
