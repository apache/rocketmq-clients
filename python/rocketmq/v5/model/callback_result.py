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

from enum import Enum


class CallbackResultType(Enum):
    ASYNC_SEND_CALLBACK_RESULT = 1
    ASYNC_SEND_CALLBACK_EXCEPTION = 2
    ASYNC_RECEIVE_CALLBACK_RESULT = 3
    ASYNC_RECEIVE_CALLBACK_EXCEPTION = 4
    ASYNC_ACK_CALLBACK_RESULT = 5
    ASYNC_ACK_CALLBACK_EXCEPTION = 6
    ASYNC_CHANGE_INVISIBLE_DURATION_RESULT = 7
    ASYNC_CHANGE_INVISIBLE_DURATION_EXCEPTION = 8
    ASYNC_RECALL_MESSAGE_RESULT = 9
    ASYNC_RECALL_MESSAGE_EXCEPTION = 10
    END_CALLBACK_THREAD_RESULT = 100


class CallbackResult:

    def __init__(self):
        self.__future = None
        self.__result = None
        self.__result_type = None
        self.__is_success = None

    @staticmethod
    def callback_result(future, result, success):
        callback_result = CallbackResult()
        callback_result.__future = future
        callback_result.__result = result
        callback_result.__is_success = success
        return callback_result

    @staticmethod
    def async_send_callback_result(future, result, success=True):
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_SEND_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_SEND_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_receive_callback_result(future, result, success=True):
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_ACK_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_ACK_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_ack_callback_result(future, result, success=True):
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_RECEIVE_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_RECEIVE_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_change_invisible_duration_callback_result(future, result, success=True):
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_CHANGE_INVISIBLE_DURATION_RESULT
            if success
            else CallbackResultType.ASYNC_CHANGE_INVISIBLE_DURATION_EXCEPTION
        )
        return callback_result

    @staticmethod
    def end_callback_thread_result():
        callback_result = CallbackResult()
        callback_result.__result_type = CallbackResultType.END_CALLBACK_THREAD_RESULT
        return callback_result

    @staticmethod
    def recall_message_callback_result(future, result, success=True):
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_RECALL_MESSAGE_RESULT
            if success
            else CallbackResultType.ASYNC_RECALL_MESSAGE_EXCEPTION
        )
        return callback_result

    """ @property """

    @property
    def future(self):
        return self.__future

    @property
    def result(self):
        return self.__result

    @property
    def result_type(self):
        return self.__result_type

    @property
    def is_success(self):
        return self.__is_success
