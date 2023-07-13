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

# from rocketmq.status_checker import StatusChecker
from rocketmq.log import logger
from rocketmq.message_id import MessageId
from rocketmq.protocol.definition_pb2 import Code as ProtoCode


class SendReceipt:
    def __init__(self, message_id: MessageId, transaction_id, message_queue):
        self.message_id = message_id
        self.transaction_id = transaction_id
        self.message_queue = message_queue

    @property
    def endpoints(self):
        return self.message_queue.broker.endpoints

    def __str__(self):
        return f'MessageId: {self.message_id}'

    @staticmethod
    def process_send_message_response(mq, invocation):
        status = invocation.status
        for entry in invocation.entries:
            if entry.status.code == ProtoCode.OK:
                status = entry.status
        logger.debug(status)
        # May throw exception.
        # StatusChecker.check(status, invocation.request, invocation.request_id)
        return [SendReceipt(entry.message_id, entry.transaction_id, mq) for entry in invocation.entries]
