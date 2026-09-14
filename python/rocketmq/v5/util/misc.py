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

from google.protobuf.timestamp_pb2 import Timestamp  # noqa

from rocketmq.grpc_protocol import Language
from rocketmq.v5.log import logger


class Misc:
    """Utility class providing helper functions used across the SDK.

    Includes SDK metadata (language, version), local IP detection, checksum
    computation, data decompression, and input validation for topic and
    consumer group names.
    """
    __LOCAL_IP = None
    __OS_NAME = None
    TOPIC_PATTERN = compile(r"^[%a-zA-Z0-9_-]+$")
    CONSUMER_GROUP_PATTERN = compile(r"^[%a-zA-Z0-9_-]+$")

    @staticmethod
    def sdk_language():
        """Return the SDK language identifier (``Language.PYTHON``)."""
        return Language.PYTHON

    @staticmethod
    def to_base36(n):
        """Convert an integer to a base-36 string representation.

        Args:
            n: Non-negative integer to convert.

        Returns:
            Base-36 encoded string using ``0-9`` and ``a-z`` characters.
        """
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
        """Detect the local machine's IP address.

        Uses a UDP socket connection to determine the outbound IP.
        Cached after first successful detection.

        Returns:
            Local IP address string, falls back to ``"127.0.0.1"`` on failure.
        """
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
        """Compute a CRC32 checksum and return it as an 8-character hex string.

        Args:
            array: Byte array to checksum.

        Returns:
            Uppercase hex string, zero-padded to 8 characters.
        """
        crc32_value = zlib.crc32(array) & 0xFFFFFFFF
        return format(crc32_value, "08X")

    @staticmethod
    def md5_checksum(array):
        """Compute an MD5 hex digest of the given byte array.

        Args:
            array: Byte array to hash.

        Returns:
            Uppercase hex string of the MD5 digest.
        """
        md5_hash = md5()
        md5_hash.update(array)
        return md5_hash.hexdigest().upper()

    @staticmethod
    def sha1_checksum(array):
        """Compute a SHA1 hex digest of the given byte array.

        Args:
            array: Byte array to hash.

        Returns:
            Uppercase hex string of the SHA1 digest.
        """
        sha1_hash = sha1()
        sha1_hash.update(array)
        return sha1_hash.hexdigest().upper()

    @staticmethod
    def uncompress_bytes_gzip(body):
        """Decompress gzip or deflate compressed byte data.

        Args:
            body: Compressed byte string. Detects gzip by ``\x1f\x8b`` magic bytes.

        Returns:
            Decompressed byte string.
        """
        if body and body[:2] == b"\x1f\x8b":
            body = gzip.decompress(body)  # Standard Gzip format
        else:
            body = zlib.decompress(body)  # deflate zip
        return body

    @staticmethod
    def get_os_description():
        """Get a human-readable OS name and version string (cached).

        Returns:
            OS description like ``"Darwin 25.3.0"`` or ``None``.
        """
        if Misc.__OS_NAME is None:
            os_name = system()  # os system name
            if os_name is None:
                return None
            os_version = version()  # os system version
            Misc.__OS_NAME = f"{os_name} {os_version}" if os_version else os_name
        return Misc.__OS_NAME

    @staticmethod
    def is_valid_topic(topic):
        """Validate a topic name against the allowed character pattern.

        Args:
            topic: Topic name string to validate.

        Returns:
            ``True`` if the topic name is valid, ``False`` otherwise.
        """
        if not bool(Misc.TOPIC_PATTERN.match(topic)):
            logger.warn(f"{topic} dose not match the regex [regex={Misc.TOPIC_PATTERN}]")
            return False
        return True

    @staticmethod
    def is_valid_consumer_group(consumer_group):
        """Validate a consumer group name against the allowed character pattern.

        Args:
            consumer_group: Consumer group name string to validate.

        Returns:
            ``True`` if the group name is valid, ``False`` otherwise.
        """
        return bool(Misc.CONSUMER_GROUP_PATTERN.match(consumer_group))

    @staticmethod
    def to_mills(timestamp: Timestamp):
        """Convert a protobuf ``Timestamp`` to milliseconds.

        Args:
            timestamp: Protobuf timestamp, or ``None``.

        Returns:
            Milliseconds since epoch, or ``0`` if timestamp is ``None``.
        """
        if not timestamp:
            return 0
        return timestamp.seconds * 1000 + timestamp.nanos // 1_000_000

    @staticmethod
    def current_mills():
        """Get the current wall clock time in milliseconds.

        Returns:
            Current time as milliseconds since epoch.
        """
        return time.time_ns() // 1_000_000
