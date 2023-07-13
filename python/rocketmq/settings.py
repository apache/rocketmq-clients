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

import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Optional

from rocketmq.protocol.definition_pb2 import ClientType as ProtoClientType
from rocketmq.protocol.definition_pb2 import RetryPolicy as ProtoRetryPolicy
from rocketmq.protocol.definition_pb2 import Settings as ProtoSettings
from rocketmq.rpc_client import Endpoints


class IRetryPolicy(ABC):
    @abstractmethod
    def GetMaxAttempts(self) -> int:
        pass

    @abstractmethod
    def GetNextAttemptDelay(self, attempt: int) -> datetime.timedelta:
        pass

    @abstractmethod
    def ToProtobuf(self) -> ProtoRetryPolicy:
        pass

    @abstractmethod
    def InheritBackoff(self, retryPolicy: ProtoRetryPolicy):
        pass


class ClientType(Enum):
    Producer = 1
    SimpleConsumer = 2
    PushConsumer = 3


class ClientTypeHelper:
    @staticmethod
    def to_protobuf(clientType):
        return {
            ClientType.Producer: ProtoClientType.PRODUCER,
            ClientType.SimpleConsumer: ProtoClientType.SIMPLE_CONSUMER,
            ClientType.PushConsumer: ProtoClientType.PUSH_CONSUMER,
        }.get(clientType, ProtoClientType.CLIENT_TYPE_UNSPECIFIED)


@dataclass
class Settings:
    ClientId: str
    ClientType: ClientType
    Endpoints: Endpoints
    RetryPolicy: Optional[IRetryPolicy]
    RequestTimeout: timedelta

    def to_protobuf(self):
        settings = ProtoSettings()
        return settings

    def Sync(self, settings):
        # Sync the settings properties from the Protobuf message
        pass

    def GetRetryPolicy(self):
        return self.RetryPolicy
