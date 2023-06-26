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
from threading import Event

from rocketmq.protocol.service_pb2 import TelemetryCommand as ProtoTelemetryCommand


class Session:
    def __init__(self, endpoints, streaming_call, client):
        self._endpoints = endpoints
        self._semaphore = asyncio.Semaphore(1)
        self._streaming_call = streaming_call
        self._client = client
        self._event = Event()

    async def write_async(self, telemetry_command: ProtoTelemetryCommand):
        await self._streaming_call.write(telemetry_command)
        response = await self._streaming_call.read()
        print(response)

    async def sync_settings(self, await_resp):
        await self._semaphore.acquire()
        try:
            settings = self._client.get_settings()
            telemetry_command = ProtoTelemetryCommand()
            telemetry_command.settings.CopyFrom(settings.to_protobuf())
            await self.write_async(telemetry_command)
        finally:
            self._semaphore.release()
