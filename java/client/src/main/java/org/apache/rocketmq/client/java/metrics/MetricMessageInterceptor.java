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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.message.MessageCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricMessageInterceptor implements MessageInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricMessageInterceptor.class);

    private final MessageMeterProvider messageMeterProvider;

    public MetricMessageInterceptor(MessageMeterProvider messageMeterProvider) {
        this.messageMeterProvider = messageMeterProvider;
    }

    private void doAfterSendMessage(List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        final Optional<DoubleHistogram> optionalHistogram =
            messageMeterProvider.getHistogramByName(MetricName.SEND_SUCCESS_COST_TIME);
        if (!optionalHistogram.isPresent()) {
            return;
        }
        final DoubleHistogram histogram = optionalHistogram.get();
        for (MessageCommon messageCommon : messageCommons) {
            InvocationStatus invocationStatus = MessageHookPointsStatus.OK.equals(status) ? InvocationStatus.SUCCESS :
                InvocationStatus.FAILURE;
            Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, messageCommon.getTopic())
                .put(MetricLabels.CLIENT_ID, messageMeterProvider.getClient().getClientId())
                .put(MetricLabels.INVOCATION_STATUS, invocationStatus.getName()).build();
            histogram.record(duration.toMillis(), attributes);
        }
    }

    private void doAfterReceive(List<MessageCommon> messageCommons) {
        if (messageCommons.isEmpty()) {
            return;
        }
        final ClientImpl client = messageMeterProvider.getClient();
        String consumerGroup = null;
        if (client instanceof PushConsumer) {
            consumerGroup = ((PushConsumer) client).getConsumerGroup();
        }
        if (client instanceof SimpleConsumer) {
            consumerGroup = ((SimpleConsumer) client).getConsumerGroup();
        }
        if (null == consumerGroup) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", client.getClientId());
            return;
        }
        final MessageCommon messageCommon = messageCommons.iterator().next();
        final Optional<Timestamp> optionalDeliveryTimestampFromRemote = messageCommon.getDeliveryTimestampFromRemote();
        if (!optionalDeliveryTimestampFromRemote.isPresent()) {
            return;
        }
        final Timestamp deliveryTimestampFromRemote = optionalDeliveryTimestampFromRemote.get();
        final long latency = System.currentTimeMillis() - Timestamps.toMillis(deliveryTimestampFromRemote);
        final Optional<DoubleHistogram> optionalHistogram =
            messageMeterProvider.getHistogramByName(MetricName.DELIVERY_LATENCY);
        if (!optionalHistogram.isPresent()) {
            return;
        }
        final DoubleHistogram histogram = optionalHistogram.get();
        final Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, messageCommon.getTopic())
            .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
            .put(MetricLabels.CLIENT_ID, client.getClientId()).build();
        histogram.record(latency, attributes);
    }

    private void doBeforeConsumeMessage(List<MessageCommon> messageCommons) {
        final ClientImpl client = messageMeterProvider.getClient();
        String consumerGroup = null;
        if (client instanceof PushConsumer) {
            consumerGroup = ((PushConsumer) client).getConsumerGroup();
        }
        if (null == consumerGroup) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", client.getClientId());
            return;
        }
        final MessageCommon messageCommon = messageCommons.iterator().next();
        final Optional<Duration> optionalDurationAfterDecoding = messageCommon.getDurationAfterDecoding();
        if (!optionalDurationAfterDecoding.isPresent()) {
            return;
        }
        final Duration durationAfterDecoding = optionalDurationAfterDecoding.get();
        Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, messageCommon.getTopic())
            .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
            .put(MetricLabels.CLIENT_ID, client.getClientId()).build();
        final Optional<DoubleHistogram> optionalHistogram =
            messageMeterProvider.getHistogramByName(MetricName.AWAIT_TIME);
        if (!optionalHistogram.isPresent()) {
            return;
        }
        final DoubleHistogram histogram = optionalHistogram.get();
        histogram.record(durationAfterDecoding.toMillis(), attributes);
    }

    private void doAfterProcessMessage(List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        final ClientImpl client = messageMeterProvider.getClient();
        if (!(client instanceof PushConsumer)) {
            // Should never reach here.
            LOGGER.error("[Bug] current client is not push consumer, clientId={}", client.getClientId());
            return;
        }
        PushConsumer pushConsumer = (PushConsumer) client;
        for (MessageCommon messageCommon : messageCommons) {
            InvocationStatus invocationStatus = MessageHookPointsStatus.OK.equals(status) ? InvocationStatus.SUCCESS :
                InvocationStatus.FAILURE;
            Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, messageCommon.getTopic())
                .put(MetricLabels.CONSUMER_GROUP, pushConsumer.getConsumerGroup())
                .put(MetricLabels.CLIENT_ID, messageMeterProvider.getClient().getClientId())
                .put(MetricLabels.INVOCATION_STATUS, invocationStatus.getName())
                .build();
            final Optional<DoubleHistogram> optionalHistogram =
                messageMeterProvider.getHistogramByName(MetricName.PROCESS_TIME);
            if (!optionalHistogram.isPresent()) {
                return;
            }
            final DoubleHistogram histogram = optionalHistogram.get();
            histogram.record(duration.toMillis(), attributes);
        }
    }

    @Override
    public void doBefore(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons) {
        if (!messageMeterProvider.isEnabled()) {
            return;
        }
        if (MessageHookPoints.CONSUME.equals(messageHookPoints)) {
            doBeforeConsumeMessage(messageCommons);
        }
    }

    @Override
    public void doAfter(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        if (!messageMeterProvider.isEnabled()) {
            return;
        }
        switch (messageHookPoints) {
            case SEND:
                doAfterSendMessage(messageCommons, duration, status);
                break;
            case RECEIVE:
                doAfterReceive(messageCommons);
                break;
            case CONSUME:
                doAfterProcessMessage(messageCommons, duration, status);
                break;
            default:
                break;
        }
    }
}
