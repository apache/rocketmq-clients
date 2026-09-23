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


class MessageMetricType(Enum):
    """Categories of message metrics collected by the client.

    Attributes:
        SEND: Metrics for message publishing operations.
        RECEIVE: Metrics for message reception from the broker.
        CONSUME: Metrics for message consumption processing.
    """
    # type of message publishing.
    SEND = 1

    # type of message reception.
    RECEIVE = 2

    # the type of message consumption.
    CONSUME = 3


class MetricContext:
    """Context object carrying metric attributes for a single operation.

    Used to accumulate attributes (topic, client_id, etc.) and timestamps
    across the before/after phases of an operation for metric recording.
    """

    def __init__(self, metric_type: MessageMetricType, attributes=None):
        """Create a metric context.

        Args:
            metric_type: The :class:`MessageMetricType` for this context.
            attributes: Optional dict of initial attributes.
        """
        if attributes is None:
            attributes = dict()
        self.__metric_type = metric_type
        self.__attributes = attributes

    def get_attr(self, key):
        """Get an attribute value by key."""
        return self.__attributes.get(key)

    def put_attr(self, key, value):
        """Set an attribute key-value pair."""
        self.__attributes[key] = value

    def remove_attr(self, key):
        """Remove an attribute by key."""
        del self.__attributes[key]

    @property
    def metric_type(self):
        """The :class:`MessageMetricType` of this context."""
        return self.__metric_type

    @property
    def attributes(self):
        """The dict of attribute key-value pairs."""
        return self.__attributes


class HistogramEnum(Enum):
    """Enum defining histogram instruments for OpenTelemetry metrics.

    Each member specifies a histogram name and its bucket boundaries
    used by the ``ClientMetrics`` to record timing data.

    Attributes:
        SEND_COST_TIME: Duration of successful message publish operations.
        DELIVERY_LATENCY: Time from message arrival at broker to client reception.
        AWAIT_TIME: Wait time before message consumption begins.
        PROCESS_TIME: Total processing time during message consumption.
    """
    # a histogram that records the cost time of successful api calls of message publishing.
    SEND_COST_TIME = (
        "rocketmq_send_cost_time",
        [1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0],
    )
    # a histogram that records the latency of message delivery from remote.
    DELIVERY_LATENCY = (
        "rocketmq_delivery_latency",
        [1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0],
    )

    # a histogram that records await time of message consumption.
    AWAIT_TIME = (
        "rocketmq_await_time",
        [1.0, 5.0, 20.0, 100.0, 1000.0, 5 * 1000.0, 10 * 1000.0],
    )

    # a histogram that records the process time of message consumption.
    PROCESS_TIME = (
        "rocketmq_process_time",
        [1.0, 5.0, 10.0, 100.0, 1000.0, 10 * 1000.0, 60 * 1000.0],
    )

    def __init__(self, histogram_name, buckets):
        """Initialize a histogram enum member.

        Args:
            histogram_name: The OpenTelemetry instrument name.
            buckets: List of bucket boundary values for the histogram.
        """
        self.__histogram_name = histogram_name
        self.__buckets = buckets

    @property
    def histogram_name(self):
        return self.__histogram_name

    @property
    def buckets(self):
        return self.__buckets
