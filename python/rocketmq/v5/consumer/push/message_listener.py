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

import abc
from enum import Enum

from rocketmq.v5.model import Message


class ConsumeResult(Enum):
    """Result of message consumption returned by :class:`MessageListener.consume`."""
    SUCCESS = 0  # Consume message successfully.
    FAILURE = 1  # Failed to consume message.


class MessageListener(metaclass=abc.ABCMeta):
    """Abstract callback handler for consuming messages in :class:`PushConsumer`.

    Subclass this and implement :meth:`consume` to process received messages.
    The PushConsumer invokes this callback on a thread from its consumption
    thread pool.
    """

    @abc.abstractmethod
    def consume(self, message: Message) -> ConsumeResult:
        """Process a received message.

        Args:
            message: The received :class:`Message`.

        Returns:
            :attr:`ConsumeResult.SUCCESS` to acknowledge, or
            :attr:`ConsumeResult.FAILURE` to retry delivery.
        """
        pass
