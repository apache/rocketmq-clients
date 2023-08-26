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

from typing import Dict

from google.protobuf.duration_pb2 import Duration
from rocketmq.filter_expression import ExpressionType
from rocketmq.log import logger
from rocketmq.protocol.definition_pb2 import \
    FilterExpression as ProtoFilterExpression
from rocketmq.protocol.definition_pb2 import FilterType as ProtoFilterType
from rocketmq.protocol.definition_pb2 import Resource as ProtoResource
from rocketmq.protocol.definition_pb2 import Settings as ProtoSettings
from rocketmq.protocol.definition_pb2 import Subscription as ProtoSubscription
from rocketmq.protocol.definition_pb2 import \
    SubscriptionEntry as ProtoSubscriptionEntry

from .settings import ClientType, ClientTypeHelper, Settings


# Assuming a simple representation of FilterExpression for the purpose of this example
class FilterExpression:
    def __init__(self, type, expression):
        self.Type = type
        self.Expression = expression


class SimpleSubscriptionSettings(Settings):

    def __init__(self, clientId, endpoints, consumerGroup, requestTimeout, longPollingTimeout,
                 subscriptionExpressions: Dict[str, FilterExpression]):
        super().__init__(clientId, ClientType.SimpleConsumer, endpoints, None, requestTimeout)
        self._group = consumerGroup  # Simplified as string for now
        self._longPollingTimeout = longPollingTimeout
        self._subscriptionExpressions = subscriptionExpressions

    def Sync(self, settings: ProtoSettings):
        if not isinstance(settings, ProtoSettings):
            logger.error(f"[Bug] Issued settings doesn't match with the client type, clientId={self.ClientId}, clientType={self.ClientType}")

    def to_protobuf(self):
        subscriptionEntries = []

        for key, value in self._subscriptionExpressions.items():
            topic = ProtoResource()
            topic.name = key

            subscriptionEntry = ProtoSubscriptionEntry()
            filterExpression = ProtoFilterExpression()

            if value.type == ExpressionType.Tag:
                filterExpression.type = ProtoFilterType.TAG
            elif value.type == ExpressionType.Sql92:
                filterExpression.type = ProtoFilterType.SQL
            else:
                logger.warn(f"[Bug] Unrecognized filter type={value.Type} for simple consumer")

            filterExpression.expression = value.expression
            subscriptionEntry.topic.CopyFrom(topic)
            subscriptionEntries.append(subscriptionEntry)

        subscription = ProtoSubscription()
        group = ProtoResource()
        group.name = self._group
        subscription.group.CopyFrom(group)
        subscription.subscriptions.extend(subscriptionEntries)
        duration_longPollingTimeout = Duration(seconds=self._longPollingTimeout)
        subscription.long_polling_timeout.CopyFrom(duration_longPollingTimeout)

        settings = super().to_protobuf()
        settings.access_point.CopyFrom(self.Endpoints.to_protobuf())  # Assuming Endpoints has a to_protobuf method
        settings.client_type = ClientTypeHelper.to_protobuf(self.ClientType)

        settings.request_timeout.CopyFrom(Duration(seconds=int(self.RequestTimeout.total_seconds())))
        settings.subscription.CopyFrom(subscription)

        return settings
