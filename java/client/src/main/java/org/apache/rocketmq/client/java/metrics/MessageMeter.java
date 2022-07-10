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
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.AuthInterceptor;
import org.apache.rocketmq.client.java.rpc.IpNameResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageMeter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageMeter.class);

    private static final Duration METRIC_EXPORTER_RPC_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration METRIC_READER_INTERVAL = Duration.ofMinutes(1);
    private static final String METRIC_INSTRUMENTATION_NAME = "org.apache.rocketmq.message";

    private final ClientImpl client;

    private volatile Meter meter;
    private volatile Endpoints metricEndpoints;
    private volatile SdkMeterProvider provider;

    private volatile MessageCacheObserver messageCacheObserver;

    private final ConcurrentMap<MetricName, DoubleHistogram> histogramMap;

    public MessageMeter(ClientImpl client) {
        this.client = client;
        this.histogramMap = new ConcurrentHashMap<>();
        this.client.registerMessageInterceptor(new MetricMessageInterceptor(this));
        this.messageCacheObserver = null;
    }

    public void setMessageCacheObserver(MessageCacheObserver messageCacheObserver) {
        this.messageCacheObserver = messageCacheObserver;
    }

    DoubleHistogram getHistogramByName(MetricName metricName) {
        return histogramMap.computeIfAbsent(metricName, name -> meter.histogramBuilder(name.getName()).build());
    }

    public synchronized void refresh(Metric metric) {
        final String clientId = client.getClientId();
        try {
            if (!metric.isOn()) {
                LOGGER.info("Skip metric refresh because metric is off, clientId={}", clientId);
                shutdown();
                return;
            }
            final Optional<Endpoints> optionalEndpoints = metric.tryGetMetricEndpoints();
            if (!optionalEndpoints.isPresent()) {
                LOGGER.error("[Bug] Metric switch is on but endpoints is not filled, clientId={}",
                    clientId);
                return;
            }
            final Endpoints existedEndpoints = metricEndpoints;
            final Endpoints newMetricEndpoints = optionalEndpoints.get();
            if (newMetricEndpoints.equals(metricEndpoints)) {
                LOGGER.debug("Message metric exporter endpoints remains the same, clientId={}, endpoints={}",
                    clientId, newMetricEndpoints);
                return;
            }
            this.reset(newMetricEndpoints);
            LOGGER.info("Message meter endpoints is updated, clientId={}, {} => {}", clientId, existedEndpoints,
                newMetricEndpoints);
        } catch (Throwable t) {
            LOGGER.error("Exception raised while refreshing message meter, clientId={}", clientId, t);
        }
    }

    public synchronized void shutdown() {
        if (null == provider) {
            return;
        }
        final String clientId = client.getClientId();
        LOGGER.info("Begin to shutdown the message meter, clientId={}", clientId);
        final CountDownLatch latch = new CountDownLatch(1);
        provider.shutdown().whenComplete(latch::countDown);
        try {
            latch.await();
        } catch (Throwable t) {
            LOGGER.error("Exception raised while waiting for the shutdown of meter, clientId={}", clientId);
        }
        LOGGER.info("Shutdown the message meter, clientId={}", clientId);
        // Clear endpoints.
        metricEndpoints = null;
        // Clear meter.
        meter = null;
        // Clear provider.
        provider = null;
    }

    public Meter getMeter() {
        return meter;
    }

    public ClientImpl getClient() {
        return client;
    }

    @SuppressWarnings("deprecation")
    private void reset(Endpoints newMetricEndpoints) throws SSLException {
        final String clientId = client.getClientId();
        final SslContext sslContext = GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build();
        final NettyChannelBuilder channelBuilder = NettyChannelBuilder.forTarget(newMetricEndpoints.getGrpcTarget())
            .sslContext(sslContext).intercept(new AuthInterceptor(client.getClientConfiguration(), clientId));
        final List<InetSocketAddress> socketAddresses = newMetricEndpoints.toSocketAddresses();
        if (null != socketAddresses) {
            IpNameResolverFactory metricResolverFactory = new IpNameResolverFactory(socketAddresses);
            channelBuilder.nameResolverFactory(metricResolverFactory);
        }
        ManagedChannel channel = channelBuilder.build();

        OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel)
            .setTimeout(METRIC_EXPORTER_RPC_TIMEOUT)
            .setAggregationTemporalitySelector(AggregationTemporalitySelector.deltaPreferred())
            .build();

        InstrumentSelector sendSuccessCostTimeInstrumentSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM).setName(MetricName.SEND_SUCCESS_COST_TIME.getName()).build();
        final View sendSuccessCostTimeView = View.builder()
            .setAggregation(HistogramBuckets.SEND_SUCCESS_COST_TIME_BUCKET).build();

        InstrumentSelector deliveryLatencyInstrumentSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM).setName(MetricName.DELIVERY_LATENCY.getName()).build();
        final View deliveryLatencyView = View.builder().setAggregation(HistogramBuckets.DELIVERY_LATENCY_BUCKET)
            .build();

        InstrumentSelector awaitTimeInstrumentSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM).setName(MetricName.AWAIT_TIME.getName()).build();
        final View awaitTimeView = View.builder().setAggregation(HistogramBuckets.AWAIT_TIME_BUCKET).build();

        InstrumentSelector processTimeInstrumentSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM).setName(MetricName.PROCESS_TIME.getName()).build();
        final View processTimeView = View.builder().setAggregation(HistogramBuckets.PROCESS_TIME_BUCKET).build();

        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
            .setInterval(METRIC_READER_INTERVAL).build();

        final SdkMeterProvider newProvider = SdkMeterProvider.builder().registerMetricReader(reader)
            .registerView(sendSuccessCostTimeInstrumentSelector, sendSuccessCostTimeView)
            .registerView(deliveryLatencyInstrumentSelector, deliveryLatencyView)
            .registerView(awaitTimeInstrumentSelector, awaitTimeView)
            .registerView(processTimeInstrumentSelector, processTimeView)
            .build();

        final OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(newProvider).build();
        meter = openTelemetry.getMeter(METRIC_INSTRUMENTATION_NAME);
        shutdown();
        // Force clean existed histogram.
        histogramMap.clear();
        final ClientImpl client = this.getClient();
        if (!(client instanceof PushConsumer)) {
            // No need for producer and simple consumer.
            return;
        }
        final String consumerGroup = ((PushConsumer) client).getConsumerGroup();
        meter.gaugeBuilder(MetricName.CONSUMER_CACHED_MESSAGES.getName()).buildWithCallback(measurement -> {
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
        meter.gaugeBuilder(MetricName.CONSUMER_CACHED_BYTES.getName()).buildWithCallback(measurement -> {
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
        this.provider = newProvider;
        this.metricEndpoints = newMetricEndpoints;
    }
}
