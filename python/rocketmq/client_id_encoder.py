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
import socket
import threading
import time

import rocketmq.utils


class ClientIdEncoder:
    """This class generates a unique client ID for each client based on
    hostname, process id, index and the monotonic clock time.
    """

    #: The current index for client id generation.
    __INDEX = 0

    #: The lock used for thread-safe incrementing of the index.
    __INDEX_LOCK = threading.Lock()

    #: The separator used in the client id string.
    __CLIENT_ID_SEPARATOR = "@"

    @staticmethod
    def __get_and_increment_sequence() -> int:
        """Increment and return the current index in a thread-safe manner.

        :return: the current index after incrementing it.
        """
        with ClientIdEncoder.__INDEX_LOCK:
            temp = ClientIdEncoder.__INDEX
            ClientIdEncoder.__INDEX += 1
            return temp

    @staticmethod
    def generate() -> str:
        """Generate a unique client ID.

        :return: the generated client id
        """
        index = ClientIdEncoder.__get_and_increment_sequence()
        return (
            socket.gethostname()
            + ClientIdEncoder.__CLIENT_ID_SEPARATOR
            + str(os.getpid())
            + ClientIdEncoder.__CLIENT_ID_SEPARATOR
            + str(index)
            + ClientIdEncoder.__CLIENT_ID_SEPARATOR
            + str(rocketmq.utils.number_to_base(time.monotonic_ns(), 36))
        )
