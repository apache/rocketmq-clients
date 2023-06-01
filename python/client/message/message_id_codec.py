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

import os
import threading
import time
import uuid
from datetime import datetime, timezone


class MessageIdCodec:
    MESSAGE_ID_LENGTH_FOR_V1_OR_LATER = 34
    MESSAGE_ID_VERSION_V0 = "00"
    MESSAGE_ID_VERSION_V1 = "01"

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(MessageIdCodec, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.processFixedStringV1 = self._get_process_fixed_string()
        self.secondsSinceCustomEpoch = self._get_seconds_since_custom_epoch()
        self.secondsStartTimestamp = int(time.time())
        self.seconds = self._delta_seconds()
        self.sequence = 0

    def _get_process_fixed_string(self):
        mac = uuid.getnode()
        mac = format(mac, '012x')
        mac_bytes = bytes.fromhex(mac[-12:])
        pid = os.getpid()
        pid_bytes = pid.to_bytes(2, 'big')
        return mac_bytes.hex() + pid_bytes.hex()

    def _get_seconds_since_custom_epoch(self):
        custom_epoch = datetime(2021, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return int((now - custom_epoch).total_seconds())

    def _delta_seconds(self):
        return int(time.time()) - self.secondsStartTimestamp + self.secondsSinceCustomEpoch

    def next_message_id(self):
        self.sequence += 1
        self.seconds = self._delta_seconds()
        seconds_bytes = self.seconds.to_bytes(4, 'big')
        sequence_bytes = self.sequence.to_bytes(4, 'big')
        return self.MESSAGE_ID_VERSION_V1 + self.processFixedStringV1 + seconds_bytes.hex() + sequence_bytes.hex()

    def decode(self, message_id):
        if len(message_id) != self.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER:
            return self.MESSAGE_ID_VERSION_V0, message_id
        return message_id[:2], message_id[2:]


if __name__ == "__main__":
    codec = MessageIdCodec()
    next_id = codec.next_message_id()
    print(next_id)
    print(codec.decode(next_id + '123'))
