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

import threading
from datetime import datetime, timezone
from os import getpid
from time import time
from uuid import getnode

from rocketmq.v5.util.atomic import AtomicInteger

#
#   The codec for the message-id.
#
#   <p>Codec here provides the following two functions:
#   1. Provide decoding function of message-id of all versions above v0.
#   2. Provide a generator of message-id of v1 version.
#
#   <p>The message-id of versions above V1 consists of 17 bytes in total. The first two bytes represent the version
#   number. For V1, these two bytes are 0x0001.
#
#   <h3>V1 message id example</h3>
#
#   <pre>
#   ┌──┬────────────┬────┬────────┬────────┐
#   │01│56F7E71C361B│21BC│024CCDBE│00000000│
#   └──┴────────────┴────┴────────┴────────┘
#   </pre>
#
#   <h3>V1 version message id generation rules</h3>
#
#   <pre>
#                       process id(lower 2bytes)
#                               ▲
#   mac address(lower 6bytes)   │   sequence number(big endian)
#                      ▲        │          ▲ (4bytes)
#                      │        │          │
#                ┌─────┴─────┐ ┌┴┐ ┌───┐ ┌─┴─┐
#         0x01+  │     6     │ │2│ │ 4 │ │ 4 │
#                └───────────┘ └─┘ └─┬─┘ └───┘
#                                    │
#                                    ▼
#             seconds since 2021-01-01 00:00:00(UTC+0)
#                           (lower 4bytes)
#   </pre>
#


class MessageIdCodec:
    MESSAGE_ID_LENGTH_FOR_V1_OR_LATER = 34
    MESSAGE_ID_VERSION_V0 = "00"
    MESSAGE_ID_VERSION_V1 = "01"

    _instance_lock = threading.Lock()
    __index = AtomicInteger()

    def __new__(cls, *args, **kwargs):
        with MessageIdCodec._instance_lock:
            if not hasattr(MessageIdCodec, "_instance"):
                MessageIdCodec._instance = object.__new__(cls)
            return MessageIdCodec._instance

    def __init__(self):
        with MessageIdCodec._instance_lock:
            if not hasattr(self, "initialized"):
                buffer = bytearray(8)
                mac = getnode().to_bytes(6, byteorder="big")
                buffer[0:6] = mac
                pid = getpid()
                pid_buffer = bytearray(4)
                pid_buffer[0:4] = pid.to_bytes(4, byteorder="big")
                buffer[6:8] = pid_buffer[2:4]
                self.process_fixed_string_v1 = buffer.hex().upper()
                self.seconds_since_custom_epoch = int(
                    (
                        datetime.now(timezone.utc)
                        - datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    ).total_seconds()
                )
                self.seconds_start_timestamp = int(time())
                self.seconds = self.__delta_time()
                self.sequence = None
                self.initialized = True

    def next_message_id(self):
        delta_seconds = self.__delta_time()
        if self.seconds != delta_seconds:
            self.seconds = delta_seconds
        buffer = bytearray(8)
        buffer[0:4] = self.seconds.to_bytes(8, byteorder="big")[4:8]
        buffer[4:8] = self.__sequence_id().to_bytes(4, byteorder="big")
        return (
            MessageIdCodec.MESSAGE_ID_VERSION_V1
            + self.process_fixed_string_v1
            + buffer.hex().upper()
        )

    """ private """

    def __delta_time(self):
        return (
            int(time()) - self.seconds_start_timestamp + self.seconds_since_custom_epoch
        )

    def __sequence_id(self):
        self.sequence = MessageIdCodec.__index.get_and_increment()
        return self.sequence
