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

from os import getpid
from socket import gethostname
from time import time_ns

from rocketmq.v5.util.atomic import AtomicInteger
from rocketmq.v5.util.misc import Misc


class ClientId:
    CLIENT_ID_SEPARATOR = "@"
    __client_index = AtomicInteger()

    def __init__(self):
        self.__client_index = ClientId.__client_index.get_and_increment()
        host_name = gethostname()
        process_id = getpid()
        base36_time = Misc.to_base36(time_ns())
        self.__client_id = (
            host_name
            + ClientId.CLIENT_ID_SEPARATOR
            + str(process_id)
            + ClientId.CLIENT_ID_SEPARATOR
            + str(self.__client_index)
            + ClientId.CLIENT_ID_SEPARATOR
            + base36_time
        )

    @property
    def client_id(self):
        return self.__client_id

    @property
    def client_index(self):
        return self.__client_index
