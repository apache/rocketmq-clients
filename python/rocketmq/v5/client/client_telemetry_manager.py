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
from asyncio import InvalidStateError

from grpc.aio import AioRpcError
from rocketmq.v5.log import logger

from .scheduler import ClientScheduler


class ClientTelemetryManager:
    """Manages telemetry stream and periodic client settings sync."""

    def __init__(self, client):
        self.__client = client
        self.__scheduler = None

    def retrieve_telemetry_stream_stream_call(self, endpoints, rebuild=False):
        try:
            self.__client.rpc_client.telemetry_stream(
                endpoints, self.__client, self.__client.sign(), rebuild, timeout=60 * 60 * 24 * 365
            )
        except Exception as e:
            logger.error(
                f"{self.__client} rebuild stream_steam_call to {endpoints} exception: {e}"
                if rebuild
                else f"{self.__client} create stream_steam_call to {endpoints} exception: {e}"
            )

    def setting_write(self, endpoints):
        req = self.__client.sync_setting_req(endpoints)
        callback = functools.partial(self.__setting_write_callback, endpoints=endpoints)
        future = self.__client.rpc_client.telemetry_write_async(endpoints, req)
        logger.debug(f"{self.__client} send setting to {endpoints}, {req}")
        future.add_done_callback(callback)

    def start_scheduler(self, io_loop):
        self.__scheduler = ClientScheduler(
            f"{self.__client.client_id}_sync_setting_schedule_thread",
            self.__sync_all_settings,
            1, 300,
            io_loop,
        )
        self.__scheduler.start_scheduler()
        logger.info("start sync setting scheduler success.")

    def stop_scheduler(self):
        if self.__scheduler:
            self.__scheduler.stop_scheduler()
            self.__scheduler = None

    def __sync_all_settings(self):
        all_endpoints = self.__client.route_manager.get_all_endpoints().values()
        for endpoints in all_endpoints:
            self.setting_write(endpoints)

    def __setting_write_callback(self, future, endpoints):
        try:
            future.result()
            logger.info(f"{self.__client} send setting to {endpoints} success.")
        except InvalidStateError as e:
            logger.warn(f"{self.__client} send setting to {endpoints} occurred InvalidStateError: {e}")
            self.retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)
        except AioRpcError as e:
            logger.warn(f"{self.__client} send setting to {endpoints} occurred AioRpcError: {e}")
            self.retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)
        except Exception as e:
            logger.error(f"{self.__client} send setting to {endpoints} exception: {e}")
            self.retrieve_telemetry_stream_stream_call(endpoints, rebuild=True)
