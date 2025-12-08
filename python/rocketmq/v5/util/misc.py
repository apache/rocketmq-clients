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

import gzip
import socket
import time
import zlib
from hashlib import md5, sha1
from platform import system, version
from re import compile

from google.protobuf.timestamp_pb2 import Timestamp
from rocketmq.grpc_protocol import Language
from rocketmq.v5.log import logger


class Misc:
    __LOCAL_IP = None
    __OS_NAME = None
    TOPIC_PATTERN = compile(r"^[%a-zA-Z0-9_-]+$")
    CONSUMER_GROUP_PATTERN = compile(r"^[%a-zA-Z0-9_-]+$")
    SDK_VERSION = "5.0.9"

    @staticmethod
    def sdk_language():
        return Language.PYTHON

    @staticmethod
    def sdk_version():
        return Misc.SDK_VERSION

    @staticmethod
    def to_base36(n):
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        result = []
        if n == 0:
            return "0"
        while n > 0:
            n, r = divmod(n, 36)
            result.append(chars[r])
        return "".join(reversed(result))

    @staticmethod
    def get_local_ip():
        if Misc.__LOCAL_IP is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(("8.8.8.8", 80))
                Misc.__LOCAL_IP = s.getsockname()[0]
            except Exception as e:
                logger.error(f"get local ip exception: {e}")
                return "127.0.0.1"
            finally:
                s.close()
        return Misc.__LOCAL_IP

    @staticmethod
    def crc32_checksum(array):
        crc32_value = zlib.crc32(array) & 0xFFFFFFFF
        return format(crc32_value, "08X")

    @staticmethod
    def md5_checksum(array):
        md5_hash = md5()
        md5_hash.update(array)
        return md5_hash.hexdigest().upper()

    @staticmethod
    def sha1_checksum(array):
        sha1_hash = sha1()
        sha1_hash.update(array)
        return sha1_hash.hexdigest().upper()

    @staticmethod
    def uncompress_bytes_gzip(body):
        if body and body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)  # Standard Gzip format
        else:
            body = zlib.decompress(body)  # deflate zip
        return body

    @staticmethod
    def get_os_description():
        if Misc.__OS_NAME is None:
            os_name = system()  # os system name
            if os_name is None:
                return None
            os_version = version()  # os system version
            Misc.__OS_NAME = f"{os_name} {os_version}" if os_version else os_name
        return Misc.__OS_NAME

    @staticmethod
    def is_valid_topic(topic):
        if not bool(Misc.TOPIC_PATTERN.match(topic)):
            logger.warn(f"{topic} dose not match the regex [regex={Misc.TOPIC_PATTERN}]")
            return False
        return True

    @staticmethod
    def is_valid_consumer_group(topic):
        return bool(Misc.CONSUMER_GROUP_PATTERN.match(topic))

    @staticmethod
    def to_mills(timestamp: Timestamp):
        if not timestamp:
            return 0
        return timestamp.seconds * 1000 + timestamp.nanos // 1_000_000

    @staticmethod
    def current_mills():
        return time.time_ns() // 1_000_000
