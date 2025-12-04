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


class SendReceipt:

    def __init__(self, message_id, transaction_id, message_queue, offset, recall_handle=None):
        self.__message_id = message_id
        self.__transaction_id = transaction_id
        self.__message_queue = message_queue
        self.__offset = offset
        self.__recall_handle = recall_handle

    def __str__(self):
        return f"message_id:{self.__message_id}"

    """ property """

    @property
    def message_id(self):
        return self.__message_id

    @property
    def transaction_id(self):
        return self.__transaction_id

    @property
    def message_queue(self):
        return self.__message_queue

    @property
    def offset(self):
        return self.__offset

    @property
    def recall_handle(self):
        return self.__recall_handle
