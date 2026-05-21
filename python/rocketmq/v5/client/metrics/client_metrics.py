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

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import \
    OTLPMetricExporter
from opentelemetry.metrics import Histogram
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.measurement import Measurement
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (ExplicitBucketHistogramAggregation,
                                            View)
from opentelemetry.sdk.resources import Resource
from rocketmq.grpc_protocol import Metric
from rocketmq.v5.client.connection import RpcEndpoints
from rocketmq.v5.log import logger
from rocketmq.v5.model import HistogramEnum, MessageMetricType, MetricContext
from rocketmq.v5.util import Misc, Signature


class ClientMetrics:
    METRIC_EXPORTER_RPC_TIMEOUT = 5
    METRIC_READER_INTERVAL = 60000  # 1 minute
    METRIC_NAME = "org.apache.rocketmq.message"
    SEND_STOPWATCH_KEY = "send_stopwatch"
    CONSUME_STOPWATCH_KEY = "consume_stopwatch"
    INVOCATION_STATUS = "invocation_status"

    def __init__(self, client_id, configuration):
        self.__enabled = False
        self.__endpoints = None
        self.__client_id = client_id
        self.__client_configuration = configuration
        self.__send_success_cost_time_instrument = None
        self.__receive_latency_instrument = None
        self.__consume_await_instrument = None
        self.__consume_process_time_instrument = None
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
                self.__receive_latency_instrument = None
                self.__consume_await_instrument = None
                self.__consume_process_time_instrument = None
                return

            self.__enabled = metric.on
            self.__endpoints = RpcEndpoints(metric.endpoints)
            self.__meter_provider_start()
            logger.info(f"client:{self.__client_id} start metric provider success.")

    def create_push_consumer_process_queue_observable_gauge(self, name, callback_func):
        def gauge_callback(callback_options):
            try:
                results = callback_func() or []
                measurements = []
                for item in results:
                    measurements.append(Measurement(value=item["value"], attributes=item["attributes"],
                                                    time_unix_nano=0, instrument=None, context=None)) # noqa
                return measurements
            except Exception as e:
                logger.error(f"failed to observe metric '{name}': {e}")
                return []

        meter = self.__meter_provider.get_meter(ClientMetrics.METRIC_NAME)
        meter.create_observable_gauge(
            name=name,
            callbacks=[gauge_callback],
        )

    """ send metric """

    def send_before(self, topic):
        send_context = MetricContext(MessageMetricType.SEND)
        # record send message time
        start_timestamp = Misc.current_mills()
        send_context.put_attr(ClientMetrics.SEND_STOPWATCH_KEY, start_timestamp)
        send_context.put_attr("topic", topic)
        send_context.put_attr("client_id", self.__client_id)
        return send_context

    def send_after(self, send_context: MetricContext, success: bool):
        if send_context is None:
            logger.warn(
                "metrics do send_after exception. send_context must not be none."
            )
            return

        if send_context.metric_type != MessageMetricType.SEND:
            logger.warn(
                f"metric type must be MessageMetricType.SEND. current send_context type is {send_context.metric_type}"
            )
            return

        if send_context.get_attr(ClientMetrics.SEND_STOPWATCH_KEY) is None:
            logger.warn(
                "metrics do send_after exception. send_stopwatch must not be none."
            )
            return

        if send_context.get_attr("topic") is None:
            logger.warn("metrics do send_after exception. topic must not be none.")
            return

        if send_context.get_attr("client_id") is None:
            send_context.put_attr("client_id", self.__client_id)

        # record send RT and result
        start_timestamp = send_context.get_attr(ClientMetrics.SEND_STOPWATCH_KEY)
        cost = float(Misc.current_mills() - start_timestamp)
        send_context.put_attr(ClientMetrics.INVOCATION_STATUS, "success" if success else "failure")
        send_context.remove_attr(ClientMetrics.SEND_STOPWATCH_KEY)
        self.__record_send_success_cost_time(send_context, cost)

    """ receive metric """

    def receive_after(self, consumer_group, messages):
        if not consumer_group or not messages or len(messages) == 0:
            return

        receive_context = MetricContext(MessageMetricType.RECEIVE)
        receive_context.put_attr("consumer_group", consumer_group)
        receive_context.put_attr("topic", messages[0].topic)
        receive_context.put_attr("client_id", self.__client_id)
        current = Misc.current_mills()
        transport_delivery_timestamp = messages[0].transport_delivery_timestamp
        latency = current - transport_delivery_timestamp
        if 0 > latency:
            logger.debug(f"latency is negative, latency: {latency}ms, current_time: {current}, delivery_timestamp: {messages[0].transport_delivery_timestamp}")
            return
        self.__record_receive_latency(receive_context, latency)

    """ consume metric """

    def consume_before(self, consumer_group, message):
        if not consumer_group or not message:
            return None

        consume_context = MetricContext(MessageMetricType.CONSUME)
        decode_message_timestamp = message.decode_message_timestamp
        consume_context.put_attr("consumer_group", consumer_group)
        consume_context.put_attr("topic", message.topic)
        consume_context.put_attr("client_id", self.__client_id)
        current = Misc.current_mills()
        latency = current - decode_message_timestamp
        self.__record_consume_await_time(consume_context, latency)
        consume_context.put_attr(ClientMetrics.CONSUME_STOPWATCH_KEY, current)
        return consume_context

    def consume_after(self, consume_context, success: bool):
        if consume_context is None:
            logger.warn(
                "metrics do consume_after exception. consume_after must not be none."
            )
            return

        if consume_context.metric_type != MessageMetricType.CONSUME:
            logger.warn(
                f"metric type must be MessageMetricType.CONSUME. current consume_context type is {consume_context.metric_type}"
            )
            return

        if consume_context.get_attr(ClientMetrics.CONSUME_STOPWATCH_KEY) is None:
            logger.warn(
                "metrics do consume_after exception. consume_stopwatch must not be none."
            )
            return

        if consume_context.get_attr("topic") is None:
            logger.warn("metrics do consume_after exception. topic must not be none.")
            return

        if consume_context.get_attr("consumer_group") is None:
            logger.warn("metrics do consume_after exception. consumer_group must not be none.")
            return

        if consume_context.get_attr("client_id") is None:
            consume_context.put_attr("client_id", self.__client_id)
        # record consume RT and result
        current = Misc.current_mills()
        process_time = float(current - consume_context.get_attr(ClientMetrics.CONSUME_STOPWATCH_KEY))
        if 0 > process_time:
            logger.warn(f"process_time is negative, latency: {process_time}ms, current time: {current}, consume_stopwatch: {consume_context.get_attr(ClientMetrics.CONSUME_STOPWATCH_KEY)}")
            return
        consume_context.put_attr(ClientMetrics.INVOCATION_STATUS, "success" if success else "failure")
        consume_context.remove_attr(ClientMetrics.CONSUME_STOPWATCH_KEY)
        self.__record_consume_process_time(consume_context, process_time)

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
                headers=Signature.metadata(self.__client_configuration, self.__client_id)
            )
            # create a metric reader and set the export interval
            reader = PeriodicExportingMetricReader(
                exporter, export_interval_millis=ClientMetrics.METRIC_READER_INTERVAL
            )
            # create an empty resource
            resource = Resource.get_empty()
            # create view、MeterProvider
            self.__meter_provider = MeterProvider(
                metric_readers=[reader], resource=resource, views=self.__create_views()
            )
            # define the histogram instruments
            self.__create_instrument()
        except Exception as e:
            logger.error(
                f"client:{self.__client_id} start meter provider exception: {e}"
            )

    def __create_views(self): # noqa
        return [
            View(
                instrument_type=Histogram,
                instrument_name=hist_enum.histogram_name,
                aggregation=ExplicitBucketHistogramAggregation(hist_enum.buckets),
            )
            for hist_enum in [
                HistogramEnum.SEND_COST_TIME,
                HistogramEnum.DELIVERY_LATENCY,
                HistogramEnum.AWAIT_TIME,
                HistogramEnum.PROCESS_TIME,
            ]
        ]

    def __create_instrument(self):
        if not self.__meter_provider:
            return
        self.__send_success_cost_time_instrument = self.__meter_provider.get_meter(
            ClientMetrics.METRIC_NAME
        ).create_histogram(HistogramEnum.SEND_COST_TIME.histogram_name)

        self.__receive_latency_instrument = self.__meter_provider.get_meter(
            ClientMetrics.METRIC_NAME
        ).create_histogram(HistogramEnum.DELIVERY_LATENCY.histogram_name)

        self.__consume_await_instrument = self.__meter_provider.get_meter(
            ClientMetrics.METRIC_NAME
        ).create_histogram(HistogramEnum.AWAIT_TIME.histogram_name)

        self.__consume_process_time_instrument = self.__meter_provider.get_meter(
            ClientMetrics.METRIC_NAME
        ).create_histogram(HistogramEnum.PROCESS_TIME.histogram_name)

    def __record_send_success_cost_time(self, context, amount):
        self.__record_metric(self.__send_success_cost_time_instrument, context, amount)

    def __record_receive_latency(self, context, amount):
        self.__record_metric(self.__receive_latency_instrument, context, amount)

    def __record_consume_await_time(self, context, amount):
        self.__record_metric(self.__consume_await_instrument, context, amount)

    def __record_consume_process_time(self, context, amount):
        self.__record_metric(self.__consume_process_time_instrument, context, amount)

    def __record_metric(self, metric_instruments, context, amount: float):
        if not self.__enabled:
            return
        try:
            metric_instruments.record(amount, context.attributes)
        except Exception as e:
            logger.error(f"failed to record metric '{context.metric_type}': {e}")
