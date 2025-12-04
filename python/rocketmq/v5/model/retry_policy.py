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

from rocketmq.v5.exception import IllegalArgumentException


class RetryPolicy:

    DEFAULT_MAX_ATTEMPTS = 17
    DEFAULT_RECONSUME_DELAY = 1

    def __init__(self, backoff_policy):
        if backoff_policy:
            self.__max_attempts = backoff_policy.max_attempts
        else:
            self.__max_attempts = RetryPolicy.DEFAULT_MAX_ATTEMPTS

    @property
    def max_attempts(self):
        return self.__max_attempts


class CustomizedBackoffRetryPolicy(RetryPolicy):

    def __init__(self, backoff_policy):
        super().__init__(backoff_policy)
        if backoff_policy:
            self.__durations = list(map(lambda item: item.seconds, backoff_policy.customized_backoff.next))
        else:
            self.__durations = list()

    def get_next_attempt_delay(self, attempt):
        if attempt < 0:
            raise IllegalArgumentException("attempt must be positive")
        size = len(self.__durations)
        if size > 0:
            return self.__durations[size - 1] if attempt > size else self.__durations[attempt - 1]
        else:
            return RetryPolicy.DEFAULT_RECONSUME_DELAY
