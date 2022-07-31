/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.metrics;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.AuthInterceptor;
import org.apache.rocketmq.client.java.rpc.IpNameResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientMeterProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientMeterProvider.class);

    private static final Duration METRIC_EXPORTER_RPC_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration METRIC_READER_INTERVAL = Duration.ofMinutes(1);
    private static final String METRIC_INSTRUMENTATION_NAME = "org.apache.rocketmq.message";

    private final ClientImpl client;
    private volatile ClientMeter clientMeter;
    private volatile MessageCacheObserver messageCacheObserver;

    public ClientMeterProvider(ClientImpl client) {
        this.client = client;
        this.client.registerMessageInterceptor(new MessageMeterInterceptor(this));
        this.clientMeter = ClientMeter.disabledInstance(client.clientId());
        this.messageCacheObserver = null;
    }

    public void setMessageCacheObserver(MessageCacheObserver messageCacheObserver) {
        this.messageCacheObserver = messageCacheObserver;
    }

    Optional<DoubleHistogram> getHistogramByEnum(HistogramEnum histogramEnum) {
        return clientMeter.getHistogramByEnum(histogramEnum);
    }

    public synchronized void reset(Metric metric) {
        final String clientId = client.clientId();
        try {
            if (clientMeter.satisfy(metric)) {
                LOGGER.info("Metric settings is satisfied by the current message meter, clientId={}", clientId);
                return;
            }
            if (!metric.isOn()) {
                LOGGER.info("Metric is off, clientId={}", clientId);
                clientMeter.shutdown();
                clientMeter = ClientMeter.disabledInstance(client.clientId());
                return;
            }
            final Endpoints endpoints = metric.getEndpoints();
            final SslContext sslContext = GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
            final NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(endpoints.getGrpcTarget())
                .sslContext(sslContext).intercept(new AuthInterceptor(client.getClientConfiguration(), clientId));
            final List<InetSocketAddress> socketAddresses = endpoints.toSocketAddresses();
            if (null != socketAddresses) {
                IpNameResolverFactory metricResolverFactory = new IpNameResolverFactory(socketAddresses);
                channelBuilder.nameResolverFactory(metricResolverFactory);
            }
            ManagedChannel channel = channelBuilder.build();
            OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel)
                .setTimeout(METRIC_EXPORTER_RPC_TIMEOUT)
                .build();

            InstrumentSelector sendSuccessCostTimeInstrumentSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM).setName(HistogramEnum.SEND_SUCCESS_COST_TIME.getName()).build();
            final View sendSuccessCostTimeView = View.builder()
                .setAggregation(HistogramEnum.SEND_SUCCESS_COST_TIME.getBucket()).build();

            InstrumentSelector deliveryLatencyInstrumentSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM).setName(HistogramEnum.DELIVERY_LATENCY.getName()).build();
            final View deliveryLatencyView = View.builder().setAggregation(HistogramEnum.DELIVERY_LATENCY.getBucket())
                .build();

            InstrumentSelector awaitTimeInstrumentSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM).setName(HistogramEnum.AWAIT_TIME.getName()).build();
            final View awaitTimeView = View.builder().setAggregation(HistogramEnum.AWAIT_TIME.getBucket()).build();

            InstrumentSelector processTimeInstrumentSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM).setName(HistogramEnum.PROCESS_TIME.getName()).build();
            final View processTimeView = View.builder().setAggregation(HistogramEnum.PROCESS_TIME.getBucket()).build();

            PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
                .setInterval(METRIC_READER_INTERVAL).build();

            final SdkMeterProvider provider = SdkMeterProvider.builder()
                .setResource(Resource.empty())
                .registerMetricReader(reader)
                .registerView(sendSuccessCostTimeInstrumentSelector, sendSuccessCostTimeView)
                .registerView(deliveryLatencyInstrumentSelector, deliveryLatencyView)
                .registerView(awaitTimeInstrumentSelector, awaitTimeView)
                .registerView(processTimeInstrumentSelector, processTimeView)
                .build();

            final OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(provider).build();
            Meter meter = openTelemetry.getMeter(METRIC_INSTRUMENTATION_NAME);

            // Reset message meter.
            ClientMeter existedClientMeter = clientMeter;
            clientMeter = new ClientMeter(meter, endpoints, provider, clientId);
            existedClientMeter.shutdown();
            LOGGER.info("Metrics is on, endpoints={}, clientId={}", endpoints, clientId);

            if (!(client instanceof PushConsumer)) {
                // No need for producer and simple consumer.
                return;
            }
            final String consumerGroup = ((PushConsumer) client).getConsumerGroup();
            meter.gaugeBuilder(GaugeEnum.CONSUMER_CACHED_MESSAGES.getName()).buildWithCallback(measurement -> {
                final Map<String, Long> cachedMessageCountMap = messageCacheObserver.getCachedMessageCount();
                for (Map.Entry<String, Long> entry : cachedMessageCountMap.entrySet()) {
                    final String topic = entry.getKey();
                    Attributes attributes = Attributes.builder()
                        .put(MetricLabels.TOPIC, topic)
                        .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
                        .put(MetricLabels.CLIENT_ID, clientId).build();
                    measurement.record(entry.getValue(), attributes);
                }
            });
            meter.gaugeBuilder(GaugeEnum.CONSUMER_CACHED_BYTES.getName()).buildWithCallback(measurement -> {
                final Map<String, Long> cachedMessageBytesMap = messageCacheObserver.getCachedMessageBytes();
                for (Map.Entry<String, Long> entry : cachedMessageBytesMap.entrySet()) {
                    final String topic = entry.getKey();
                    Attributes attributes = Attributes.builder()
                        .put(MetricLabels.TOPIC, topic)
                        .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
                        .put(MetricLabels.CLIENT_ID, clientId).build();
                    measurement.record(entry.getValue(), attributes);
                }
            });
        } catch (Throwable t) {
            LOGGER.error("Exception raised when resetting message meter, clientId={}", clientId, t);
        }
    }

    public boolean isEnabled() {
        return clientMeter.isEnabled();
    }

    public ClientImpl getClient() {
        return client;
    }
}
