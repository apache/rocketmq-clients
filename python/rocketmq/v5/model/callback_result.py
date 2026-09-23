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
    """Enumeration of async operation result types.

    Used to categorize callback outcomes from async send, receive,
    ack, recall, and invisible duration change operations.
    """
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
    """Container for the outcome of an asynchronous callback operation.

    Wraps a ``Future``, the result value, result type, and success status
    into a single object that can be passed through a callback queue.
    """

    def __init__(self):
        """Create an empty callback result. Use the static factory methods to populate fields."""
        self.__future = None
        self.__result = None
        self.__result_type = None
        self.__is_success = None

    @staticmethod
    def callback_result(future, result, success):
        """Base factory for creating a callback result.

        Args:
            future: The associated ``Future`` object.
            result: The result value or exception.
            success: Whether the operation succeeded.

        Returns:
            A :class:`CallbackResult` instance.
        """
        callback_result = CallbackResult()
        callback_result.__future = future
        callback_result.__result = result
        callback_result.__is_success = success
        return callback_result

    @staticmethod
    def async_send_callback_result(future, result, success=True):
        """Create a callback result for async send operations."""
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_SEND_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_SEND_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_receive_callback_result(future, result, success=True):
        """Create a callback result for async receive operations."""
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_RECEIVE_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_RECEIVE_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_ack_callback_result(future, result, success=True):
        """Create a callback result for async ack operations."""
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_ACK_CALLBACK_RESULT
            if success
            else CallbackResultType.ASYNC_ACK_CALLBACK_EXCEPTION
        )
        return callback_result

    @staticmethod
    def async_change_invisible_duration_callback_result(future, result, success=True):
        """Create a callback result for async change invisible duration operations."""
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_CHANGE_INVISIBLE_DURATION_RESULT
            if success
            else CallbackResultType.ASYNC_CHANGE_INVISIBLE_DURATION_EXCEPTION
        )
        return callback_result

    @staticmethod
    def end_callback_thread_result():
        """Create a sentinel result to signal callback thread termination."""
        callback_result = CallbackResult()
        callback_result.__result_type = CallbackResultType.END_CALLBACK_THREAD_RESULT
        return callback_result

    @staticmethod
    def recall_message_callback_result(future, result, success=True):
        """Create a callback result for async recall message operations."""
        callback_result = CallbackResult.callback_result(future, result, success)
        callback_result.__result_type = (
            CallbackResultType.ASYNC_RECALL_MESSAGE_RESULT
            if success
            else CallbackResultType.ASYNC_RECALL_MESSAGE_EXCEPTION
        )
        return callback_result

    @property
    def future(self):
        """The associated ``Future`` object."""
        return self.__future

    @property
    def result(self):
        """The result value or exception from the operation."""
        return self.__result

    @property
    def result_type(self):
        """The :class:`CallbackResultType` categorizing this result."""
        return self.__result_type

    @property
    def is_success(self):
        """Whether the operation succeeded."""
        return self.__is_success
