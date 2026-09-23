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

from rocketmq.grpc_protocol import FilterType


class FilterExpression:
    """Filter expression for topic subscription.

    Used to filter messages by tag or SQL92 expression when subscribing
    to a topic. The default subscribes to all tags (``"*"``).

    Attributes:
        TAG_EXPRESSION_SUB_ALL: Wildcard expression matching all tags.
    """
    TAG_EXPRESSION_SUB_ALL = "*"

    def __init__(
        self,
        expression=TAG_EXPRESSION_SUB_ALL,
        filter_type: FilterType = FilterType.TAG,
    ):
        """Create a filter expression.

        Args:
            expression: Filter expression string. ``"*"`` matches all tags.
            filter_type: The filter type, either ``TAG`` or ``SQL92``.
        """
        self.__expression = expression
        self.__filter_type = filter_type

    @property
    def expression(self):
        return self.__expression

    @property
    def filter_type(self):
        return self.__filter_type
