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

import platform
import socket
from typing import Dict

from rocketmq.exponential_backoff_retry_policy import \
    ExponentialBackoffRetryPolicy
from rocketmq.protocol.definition_pb2 import UA
from rocketmq.protocol.definition_pb2 import Publishing as ProtoPublishing
from rocketmq.protocol.definition_pb2 import Resource as ProtoResource
from rocketmq.protocol.definition_pb2 import Settings as ProtoSettings
from rocketmq.rpc_client import Endpoints
from rocketmq.settings import ClientType, ClientTypeHelper, Settings
from rocketmq.signature import Signature


class UserAgent:
    def __init__(self):
        self._version = Signature()._Signature__CLIENT_VERSION_KEY
        self._platform = platform.platform()
        self._hostname = socket.gethostname()

    def to_protobuf(self) -> UA:
        return UA(
            version=self._version, hostname=self._hostname, platform=self._platform
        )


class PublishingSettings(Settings):
    def __init__(
        self,
        client_id: str,
        endpoints: Endpoints,
        retry_policy: ExponentialBackoffRetryPolicy,
        request_timeout: int,
        topics: Dict[str, bool],
    ):
        super().__init__(
            client_id, ClientType.Producer, endpoints, retry_policy, request_timeout
        )
        self._max_body_size_bytes = 4 * 1024 * 1024
        self._validate_message_type = True
        self._topics = topics

    def get_max_body_size_bytes(self) -> int:
        return self._max_body_size_bytes

    def is_validate_message_type(self) -> bool:
        return self._validate_message_type

    def sync(self, settings: ProtoSettings) -> None:
        if settings.pub_sub_case != ProtoSettings.PubSubOneofCase.PUBLISHING:
            return

        self.retry_policy = self.retry_policy.inherit_backoff(settings.backoff_policy)
        self._validate_message_type = settings.publishing.validate_message_type
        self._max_body_size_bytes = settings.publishing.max_body_size

    def to_protobuf(self):
        topics = [ProtoResource(name=topic_name) for topic_name in self._topics]

        publishing = ProtoPublishing(
            topics=topics,
            validate_message_type=self._validate_message_type,
            max_body_size=self._max_body_size_bytes,
        )
        return ProtoSettings(
            publishing=publishing,
            access_point=self.Endpoints.to_protobuf(),
            client_type=ClientTypeHelper.to_protobuf(self.ClientType),
            user_agent=UserAgent().to_protobuf(),
        )
