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

import com.google.common.base.Stopwatch;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.java.hook.Attribute;
import org.apache.rocketmq.client.java.hook.AttributeKey;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.impl.Client;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageMeterInterceptor implements MessageInterceptor {
    static final AttributeKey<Stopwatch> SEND_STOPWATCH_KEY = AttributeKey.create("send_stopwatch");
    static final AttributeKey<Stopwatch> CONSUME_STOPWATCH_KEY = AttributeKey.create("consume_stopwatch");

    private static final Logger log = LoggerFactory.getLogger(MessageMeterInterceptor.class);

    private final Client client;
    private final ClientMeterManager meterManager;

    public MessageMeterInterceptor(Client client, ClientMeterManager meterManager) {
        this.client = client;
        this.meterManager = meterManager;
    }

    private void doBeforeSendMessage(MessageInterceptorContext context) {
        // Record the time before sending message.
        context.putAttribute(SEND_STOPWATCH_KEY, Attribute.create(Stopwatch.createStarted()));
    }

    private void doAfterSendMessage(MessageInterceptorContext context, List<GeneralMessage> messages) {
        final Attribute<Stopwatch> stopwatchAttr = context.getAttribute(SEND_STOPWATCH_KEY);
        if (null == stopwatchAttr) {
            // Should never reach here.
            return;
        }
        for (GeneralMessage message : messages) {
            final Duration duration = stopwatchAttr.get().elapsed();
            InvocationStatus status = MessageHookPointsStatus.OK.equals(context.getStatus()) ?
                InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
            Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
                .put(MetricLabels.CLIENT_ID, client.getClientId().toString())
                .put(MetricLabels.INVOCATION_STATUS, status.getName()).build();
            meterManager.record(HistogramEnum.SEND_COST_TIME, attributes, duration.toMillis());
        }
    }

    private void doAfterReceiveMessage(List<GeneralMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        String consumerGroup = null;
        if (client instanceof PushConsumer) {
            consumerGroup = ((PushConsumer) client).getConsumerGroup();
        }
        if (client instanceof SimpleConsumer) {
            consumerGroup = ((SimpleConsumer) client).getConsumerGroup();
        }
        if (null == consumerGroup) {
            log.error("[Bug] consumerGroup is not recognized, clientId={}", client.getClientId());
            return;
        }
        final GeneralMessage message = messages.iterator().next();
        final Optional<Long> optionalTransportDeliveryTimestamp = message.getTransportDeliveryTimestamp();
        if (!optionalTransportDeliveryTimestamp.isPresent()) {
            return;
        }
        final long transportDeliveryTimestamp = optionalTransportDeliveryTimestamp.get();
        final long currentTimeMillis = System.currentTimeMillis();
        final long latency = currentTimeMillis - transportDeliveryTimestamp;
        if (0 > latency) {
            log.debug("latency is negative, latency={}ms, currentTimeMillis={}, transportDeliveryTimestamp={}",
                latency, currentTimeMillis, transportDeliveryTimestamp);
            return;
        }
        final Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
            .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
            .put(MetricLabels.CLIENT_ID, client.getClientId().toString()).build();
        meterManager.record(HistogramEnum.DELIVERY_LATENCY, attributes, latency);
    }

    private void doBeforeConsumeMessage(MessageInterceptorContext context, List<GeneralMessage> messages) {
        if (messages.isEmpty()) {
            // Should never reach here.
            return;
        }
        String consumerGroup = null;
        if (client instanceof PushConsumer) {
            consumerGroup = ((PushConsumer) client).getConsumerGroup();
        }
        if (null == consumerGroup) {
            log.error("[Bug] consumerGroup is not recognized, clientId={}", client.getClientId());
            return;
        }
        final GeneralMessage message = messages.iterator().next();
        final Optional<Long> optionalDecodeTimestamp = message.getDecodeTimestamp();
        if (!optionalDecodeTimestamp.isPresent()) {
            return;
        }
        final long decodeTimestamp = optionalDecodeTimestamp.get();
        Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
            .put(MetricLabels.CONSUMER_GROUP, consumerGroup)
            .put(MetricLabels.CLIENT_ID, client.getClientId().toString()).build();
        final long latency = System.currentTimeMillis() - decodeTimestamp;
        meterManager.record(HistogramEnum.AWAIT_TIME, attributes, latency);
        // Record the time before consuming message.
        context.putAttribute(CONSUME_STOPWATCH_KEY, Attribute.create(Stopwatch.createStarted()));
    }

    private void doAfterConsumeMessage(MessageInterceptorContext context, List<GeneralMessage> messages) {
        if (!(client instanceof PushConsumer)) {
            // Should never reach here.
            log.error("[Bug] current client is not push consumer, clientId={}", client.getClientId());
            return;
        }
        final Attribute<Stopwatch> stopwatchAttr = context.getAttribute(CONSUME_STOPWATCH_KEY);
        if (null == stopwatchAttr) {
            // Should never reach here.
            return;
        }
        PushConsumer pushConsumer = (PushConsumer) client;
        final MessageHookPointsStatus status = context.getStatus();
        for (GeneralMessage message : messages) {
            InvocationStatus invocationStatus = MessageHookPointsStatus.OK.equals(status) ? InvocationStatus.SUCCESS :
                InvocationStatus.FAILURE;
            Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
                .put(MetricLabels.CONSUMER_GROUP, pushConsumer.getConsumerGroup())
                .put(MetricLabels.CLIENT_ID, client.getClientId().toString())
                .put(MetricLabels.INVOCATION_STATUS, invocationStatus.getName())
                .build();
            final Duration duration = stopwatchAttr.get().elapsed();
            meterManager.record(HistogramEnum.PROCESS_TIME, attributes, duration.toMillis());
        }
    }

    @Override
    public void doBefore(MessageInterceptorContext context, List<GeneralMessage> messages) {
        if (!meterManager.isEnabled()) {
            return;
        }
        final MessageHookPoints hookPoints = context.getMessageHookPoints();
        switch (hookPoints) {
            case SEND: {
                doBeforeSendMessage(context);
                break;
            }
            case CONSUME: {
                doBeforeConsumeMessage(context, messages);
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void doAfter(MessageInterceptorContext context, List<GeneralMessage> messages) {
        if (!meterManager.isEnabled()) {
            return;
        }
        final MessageHookPoints hookPoints = context.getMessageHookPoints();
        switch (hookPoints) {
            case SEND: {
                doAfterSendMessage(context, messages);
                break;
            }
            case RECEIVE: {
                doAfterReceiveMessage(messages);
                break;
            }
            case CONSUME: {
                doAfterConsumeMessage(context, messages);
                break;
            }
            default:
                break;
        }
    }
}
