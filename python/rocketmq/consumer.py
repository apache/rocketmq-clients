import re
from google.protobuf.duration_pb2 import Duration
from rocketmq.client import Client
from rocketmq.protocol.definition_pb2 import FilterType, FilterExpression as ProtoFilterExpression, Resource as ProtoResource
from rocketmq.protocol.service_pb2 import ReceiveMessageRequest as ProtoReceiveMessageRequest
from filter_expression import ExpressionType
from message import MessageView
from typing import List


class ReceiveMessageResult:
    def __init__(self, endpoints, messages: List['MessageView']):
        self.endpoints = endpoints
        self.messages = messages


class Consumer(Client):
    CONSUMER_GROUP_REGEX = re.compile(r"^[%a-zA-Z0-9_-]+$")

    def __init__(self, client_config, consumer_group):
        super().__init__(client_config)
        self.consumer_group = consumer_group

    async def receive_message(self, request, mq, await_duration):
        tolerance = self.client_config.request_timeout
        timeout = tolerance + await_duration
        results = await self.client_manager.receive_message(mq.broker.endpoints, request, timeout)

        messages = [MessageView.from_protobuf(message, mq) for message in results]
        return ReceiveMessageResult(mq.broker.endpoints, messages)

    @staticmethod
    def _wrap_filter_expression(filter_expression):
        filter_type = FilterType.TAG
        if filter_expression.type == ExpressionType.Sql92:
            filter_type = FilterType.SQL
        return ProtoFilterExpression(
            type=filter_type,
            expression=filter_expression.expression
        )

    def wrap_receive_message_request(self, batch_size, mq, filter_expression, await_duration, invisible_duration):
        group = ProtoResource()
        group.name = self.consumer_group
        return ProtoReceiveMessageRequest(
            group=group,
            message_queue=mq.to_protobuf(),
            filter_expression=self._wrap_filter_expression(filter_expression),
            long_polling_timeout=Duration(seconds=await_duration),
            batch_size=batch_size,
            auto_renew=False,
            invisible_duration=Duration(seconds=invisible_duration)
        )
