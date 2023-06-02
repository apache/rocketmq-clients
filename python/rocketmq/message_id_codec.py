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

import math
import os
import threading
import time
import uuid
from datetime import datetime, timezone


class MessageIdCodec:
    __MESSAGE_ID_VERSION_V1 = "01"

    @staticmethod
    def __get_process_fixed_string():
        mac = uuid.getnode()
        mac = format(mac, "012x")
        mac_bytes = bytes.fromhex(mac[-12:])
        pid = os.getpid() % 65536
        pid_bytes = pid.to_bytes(2, "big")
        return mac_bytes.hex().upper() + pid_bytes.hex().upper()

    @staticmethod
    def __get_seconds_since_custom_epoch():
        custom_epoch = datetime(2021, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - custom_epoch).total_seconds())

    __PROCESS_FIXED_STRING_V1 = __get_process_fixed_string()
    __SECONDS_SINCE_CUSTOM_EPOCH = __get_seconds_since_custom_epoch()
    __SECONDS_START_TIMESTAMP = int(time.time())

    @staticmethod
    def __delta_seconds():
        return (
            int(time.time())
            - MessageIdCodec.__SECONDS_START_TIMESTAMP
            + MessageIdCodec.__SECONDS_SINCE_CUSTOM_EPOCH
        )

    @staticmethod
    def __int_to_bytes_with_big_endian(number: int, min_bytes: int):
        num_bytes = max(math.ceil(number.bit_length() / 8), min_bytes)
        return number.to_bytes(num_bytes, "big")

    __SEQUENCE = 0
    __SEQUENCE_LOCK = threading.Lock()

    @staticmethod
    def __get_and_increment_sequence():
        with MessageIdCodec.__SEQUENCE_LOCK:
            temp = MessageIdCodec.__SEQUENCE
            MessageIdCodec.__SEQUENCE += 1
            return temp

    @staticmethod
    def next_message_id():
        seconds = MessageIdCodec.__delta_seconds()
        seconds_bytes = MessageIdCodec.__int_to_bytes_with_big_endian(seconds, 4)[-4:]
        sequence_bytes = MessageIdCodec.__int_to_bytes_with_big_endian(
            MessageIdCodec.__get_and_increment_sequence(), 4
        )[-4:]
        return (
            MessageIdCodec.__MESSAGE_ID_VERSION_V1
            + MessageIdCodec.__PROCESS_FIXED_STRING_V1
            + seconds_bytes.hex().upper()
            + sequence_bytes.hex().upper()
        )
