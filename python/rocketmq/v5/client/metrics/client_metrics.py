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

import threading
import time

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import \
    OTLPMetricExporter
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (ExplicitBucketHistogramAggregation,
                                            View)
from opentelemetry.sdk.resources import Resource
from rocketmq.grpc_protocol import Metric
from rocketmq.v5.client.connection import RpcEndpoints
from rocketmq.v5.log import logger
from rocketmq.v5.model import HistogramEnum, MessageMetricType, MetricContext


class ClientMetrics:
    METRIC_EXPORTER_RPC_TIMEOUT = 5
    METRIC_READER_INTERVAL = 60000  # 1 minute
    METRIC_INSTRUMENTATION_NAME = "org.apache.rocketmq.message"

    def __init__(self, client_id, configuration):
        self.__enabled = False
        self.__endpoints = None
        self.__client_id = client_id
        self.__client_configuration = configuration
        self.__send_success_cost_time_instrument = None
        self.__meter_provider = None
        self.__metric_lock = threading.Lock()

    def reset_metrics(self, metric: Metric):
        # if metrics endpoints changed or metric.on from False to True, start a new client metrics
        with self.__metric_lock:
            if self.__satisfy(metric):
                return

            # metric.on from True to False, shutdown client metrics
            if not metric.on:
                self.__meter_provider_shutdown()
                self.__enabled = False
                self.__endpoints = None
                self.__send_success_cost_time_instrument = None
                return

            self.__enabled = metric.on
            self.__endpoints = RpcEndpoints(metric.endpoints)
            self.__meter_provider_start()
            logger.info(f"client:{self.__client_id} start metric provider success.")

    """ send metric """

    def send_before(self, topic):
        send_context = MetricContext(MessageMetricType.SEND)
        # record send message time
        start_timestamp = round(time.time() * 1000, 2)
        send_context.put_attr("send_stopwatch", start_timestamp)
        send_context.put_attr("topic", topic)
        send_context.put_attr("client_id", self.__client_id)
        return send_context

    def send_after(self, send_context: MetricContext, success: bool):
        if send_context is None:
            logger.warn(
                "metrics do send after exception. send_context must not be none."
            )
            return

        if send_context.metric_type != MessageMetricType.SEND:
            logger.warn(
                f"metric type must be MessageMetricType.SEND. current send_context type is {send_context.metric_type}"
            )
            return

        if send_context.get_attr("send_stopwatch") is None:
            logger.warn(
                "metrics do send after exception. send_stopwatch must not be none."
            )
            return

        if send_context.get_attr("topic") is None:
            logger.warn("metrics do send after exception. topic must not be none.")
            return

        if send_context.get_attr("client_id") is None:
            send_context.put_attr("client_id", self.__client_id)

        # record send RT and result
        start_timestamp = send_context.get_attr("send_stopwatch")
        cost = round(time.time() * 1000, 2) - start_timestamp
        send_context.put_attr("invocation_status", "success" if success else "failure")
        send_context.remove_attr("send_stopwatch")
        self.__record_send_success_cost_time(send_context, cost)

    """ private """

    def __satisfy(self, metric: Metric):
        if metric.endpoints is None:
            return True
        # if metrics endpoints changed, return False
        if (
            self.__enabled
            and metric.on
            and self.__endpoints == RpcEndpoints(metric.endpoints)
        ):
            return True
        return not self.__enabled and not metric.on

    def __meter_provider_shutdown(self):
        if self.__meter_provider is not None:
            try:
                self.__meter_provider.shutdown()
                self.__meter_provider = None
            except Exception as e:
                logger.error(f"meter provider shutdown exception:{e}")

    def __meter_provider_start(self):
        if self.__endpoints is None:
            logger.warn(
                f"client:{self.__client_id} can't create meter provider, because endpoints is none."
            )
            return

        try:
            # setup OTLP exporter
            exporter = OTLPMetricExporter(
                endpoint=self.__endpoints.__str__(),
                insecure=True,
                timeout=ClientMetrics.METRIC_EXPORTER_RPC_TIMEOUT,
            )
            # create a metric reader and set the export interval
            reader = PeriodicExportingMetricReader(
                exporter, export_interval_millis=ClientMetrics.METRIC_READER_INTERVAL
            )
            # create an empty resource
            resource = Resource.get_empty()
            # create view
            send_cost_time_view = View(
                instrument_type=Histogram,
                instrument_name=HistogramEnum.SEND_COST_TIME.histogram_name,
                aggregation=ExplicitBucketHistogramAggregation(
                    HistogramEnum.SEND_COST_TIME.buckets
                ),
            )
            # create MeterProvider
            self.__meter_provider = MeterProvider(
                metric_readers=[reader], resource=resource, views=[send_cost_time_view]
            )
            # define the histogram instruments
            self.__send_success_cost_time_instrument = self.__meter_provider.get_meter(
                ClientMetrics.METRIC_INSTRUMENTATION_NAME
            ).create_histogram(HistogramEnum.SEND_COST_TIME.histogram_name)
        except Exception as e:
            logger.error(
                f"client:{self.__client_id} start meter provider exception: {e}"
            )

    def __record_send_success_cost_time(self, context, amount):
        if self.__enabled:
            try:
                # record send message cost time and result
                self.__send_success_cost_time_instrument.record(
                    amount, context.attributes
                )
            except Exception as e:
                logger.error(f"record send message cost time exception, e:{e}")
