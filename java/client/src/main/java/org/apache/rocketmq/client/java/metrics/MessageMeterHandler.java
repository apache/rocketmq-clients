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
import org.apache.rocketmq.client.java.hook.MessageHandler;
import org.apache.rocketmq.client.java.hook.MessageHandlerContext;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageMeterHandler implements MessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageMeterHandler.class);

    private static final AttributeKey<Stopwatch> MESSAGE_SEND_STOPWATCH_KEY = AttributeKey.create(
        "message_send_stopwatch");
    private static final AttributeKey<Stopwatch> MESSAGE_CONSUME_STOPWATCH_KEY = AttributeKey.create(
        "message_consume_stopwatch");

    private final ClientImpl client;
    private final ClientMeterProvider meterProvider;

    public MessageMeterHandler(ClientImpl client, ClientMeterProvider meterProvider) {
        this.client = client;
        this.meterProvider = meterProvider;
    }

    private void doBeforeSendMessage(MessageHandlerContext context) {
        // Record the time before sending message.
        context.putAttribute(MESSAGE_SEND_STOPWATCH_KEY, Attribute.create(Stopwatch.createStarted()));
    }

    private void doAfterSendMessage(MessageHandlerContext context, List<GeneralMessage> messages) {
        final Attribute<Stopwatch> stopwatchAttr = context.getAttribute(MESSAGE_SEND_STOPWATCH_KEY);
        if (null == stopwatchAttr) {
            // Should never reach here.
            return;
        }
        for (GeneralMessage message : messages) {
            final Duration duration = stopwatchAttr.get().elapsed();
            InvocationStatus status = MessageHookPointsStatus.OK.equals(context.getStatus()) ?
                InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
            Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
                .put(MetricLabels.CLIENT_ID, client.clientId())
                .put(MetricLabels.INVOCATION_STATUS, status.getName()).build();
            meterProvider.record(HistogramEnum.SEND_SUCCESS_COST_TIME, attributes, duration.toMillis());
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
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", client.clientId());
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
            LOGGER.debug("latency is negative, latency={}ms, currentTimeMillis={}, transportDeliveryTimestamp={}",
                latency, currentTimeMillis, transportDeliveryTimestamp);
            return;
        }
        final Attributes attributes = Attributes.builder().put(MetricLabels.TOPIC, message.getTopic())
            .put(MetricLabels.CONSUMER_GROUP, consumerGroup).put(MetricLabels.CLIENT_ID, client.clientId()).build();
        meterProvider.record(HistogramEnum.DELIVERY_LATENCY, attributes, latency);
    }

    private void doBeforeConsumeMessage(MessageHandlerContext context, List<GeneralMessage> messages) {
        String consumerGroup = null;
        if (client instanceof PushConsumer) {
            consumerGroup = ((PushConsumer) client).getConsumerGroup();
        }
        if (null == consumerGroup) {
            LOGGER.error("[Bug] consumerGroup is not recognized, clientId={}", client.clientId());
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
            .put(MetricLabels.CLIENT_ID, client.clientId()).build();
        final long latency = System.currentTimeMillis() - decodeTimestamp;
        meterProvider.record(HistogramEnum.AWAIT_TIME, attributes, latency);
        // Record the time before consuming message.
        context.putAttribute(MESSAGE_CONSUME_STOPWATCH_KEY, Attribute.create(Stopwatch.createStarted()));
    }

    private void doAfterConsumeMessage(MessageHandlerContext context, List<GeneralMessage> messages) {
        if (!(client instanceof PushConsumer)) {
            // Should never reach here.
            LOGGER.error("[Bug] current client is not push consumer, clientId={}", client.clientId());
            return;
        }
        final Attribute<Stopwatch> stopwatchAttr = context.getAttribute(MESSAGE_CONSUME_STOPWATCH_KEY);
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
                .put(MetricLabels.CLIENT_ID, client.clientId())
                .put(MetricLabels.INVOCATION_STATUS, invocationStatus.getName())
                .build();
            final Duration duration = stopwatchAttr.get().elapsed();
            meterProvider.record(HistogramEnum.PROCESS_TIME, attributes, duration.toMillis());
        }
    }

    @Override
    public void doBefore(MessageHandlerContext context, List<GeneralMessage> messages) {
        if (!meterProvider.isEnabled()) {
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
    public void doAfter(MessageHandlerContext context, List<GeneralMessage> messages) {
        if (!meterProvider.isEnabled()) {
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
