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

from rocketmq.log import logger
from rocketmq.protocol.service_pb2 import \
    TelemetryCommand as ProtoTelemetryCommand


class Session:
    def __init__(self, endpoints, streaming_call, client):
        self._endpoints = endpoints
        self._semaphore = asyncio.Semaphore(1)
        self._streaming_call = streaming_call
        self._client = client
        asyncio.create_task(self.loop())

    async def loop(self):
        try:
            while True:
                await self._streaming_call.read()
        except asyncio.exceptions.InvalidStateError as e:
            logger.error('Error:', e)

    async def write_async(self, telemetry_command: ProtoTelemetryCommand):
        await asyncio.sleep(1)
        try:
            await self._streaming_call.write(telemetry_command)
            # TODO handle read operation exceed the time limit
            # await asyncio.wait_for(self._streaming_call.read(), timeout=5)
        except asyncio.exceptions.InvalidStateError as e:
            self.on_error(e)
        except asyncio.TimeoutError:
            logger.error('Timeout: The read operation exceeded the time limit')

    async def sync_settings(self, await_resp):
        await self._semaphore.acquire()
        try:
            settings = self._client.get_settings()
            telemetry_command = ProtoTelemetryCommand()
            telemetry_command.settings.CopyFrom(settings.to_protobuf())
            await self.write_async(telemetry_command)
        finally:
            self._semaphore.release()

    def rebuild_telemetry(self):
        logger.info("Try to rebuild telemetry")
        stream = self._client.client_manager.telemetry(self._endpoints, 10)
        self._streaming_call = stream

    def on_error(self, exception):
        client_id = self._client.get_client_id()
        logger.error("Caught InvalidStateError: RPC already finished.")
        logger.error(f"Exception raised from stream, clientId={client_id}, endpoints={self._endpoints}", exception)
        max_retry = 3
        for i in range(max_retry):
            try:
                self.rebuild_telemetry()
                break
            except Exception as e:
                logger.error(f"An error occurred during rebuilding telemetry: {e}, attempt {i + 1} of {max_retry}")
