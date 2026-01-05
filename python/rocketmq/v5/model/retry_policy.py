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

    DEFAULT_RECONSUME_DELAY = 1  # seconds
    DEFAULT_RESEND_DELAY = 1  # seconds

    def __init__(self, backoff_policy, max_attempts):
        if backoff_policy:
            self.__max_attempts = backoff_policy.max_attempts
        else:
            self.__max_attempts = max_attempts

    @property
    def max_attempts(self):
        return self.__max_attempts


class CustomizedBackoffRetryPolicy(RetryPolicy):

    def __init__(self, backoff_policy, default_max_attempts):
        super().__init__(backoff_policy, default_max_attempts)
        if backoff_policy:
            self.__durations = list(map(lambda item: item.seconds, backoff_policy.customized_backoff.next))
        else:
            self.__durations = list()

    def __eq__(self, other):
        return self.max_attempts == other.max_attempts and self.__durations == other.__durations

    def get_next_attempt_delay(self, attempt):
        if attempt < 0:
            raise IllegalArgumentException("attempt must be positive")
        size = len(self.__durations)
        if size > 0:
            return self.__durations[size - 1] if attempt > size else self.__durations[attempt - 1]
        else:
            return RetryPolicy.DEFAULT_RECONSUME_DELAY

    @property
    def durations(self):
        return self.__durations


class ExponentialBackoffRetryPolicy(RetryPolicy):

    def __init__(self, backoff_policy, default_max_attempts):
        super().__init__(backoff_policy, default_max_attempts)
        if backoff_policy:
            self.__initial_backoff = backoff_policy.exponential_backoff.initial.seconds * 1_000_000_000 + backoff_policy.exponential_backoff.initial.nanos  # nanos
            self.__max_backoff = backoff_policy.exponential_backoff.max.seconds * 1_000_000_000 + backoff_policy.exponential_backoff.max.nanos  # nanos
            self.__multiplier = backoff_policy.exponential_backoff.multiplier
        else:
            self.__initial_backoff = None
            self.__max_backoff = None
            self.__multiplier = None

    def __eq__(self, other):
        return self.max_attempts == other.max_attempts and self.initial_backoff == other.initial_backoff and self.max_backoff == other.max_backoff and self.multiplier == other.multiplier

    def get_next_attempt_delay(self, attempt):
        if attempt < 0:
            raise IllegalArgumentException("attempt must be positive")
        if self.__initial_backoff and self.__max_backoff and self.__multiplier:
            exp_backoff_nanos = self.__initial_backoff * (self.__multiplier ** (attempt - 1))
            delay_nanos = int(min(exp_backoff_nanos, self.__max_backoff))
            if delay_nanos <= 0:
                return 0
            return delay_nanos / 1_000_000_000
        else:
            return RetryPolicy.DEFAULT_RESEND_DELAY

    @property
    def initial_backoff(self):
        return self.__initial_backoff

    @property
    def max_backoff(self):
        return self.__max_backoff

    @property
    def multiplier(self):
        return self.__multiplier
