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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.impl.Client;
import org.apache.rocketmq.client.java.impl.ClientImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class MessageMeterHandlerTest extends TestBase {

    @Test
    public void testSendMessageWithSuccess() {
        final ClientImpl producer = Mockito.mock(ClientImpl.class);
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(producer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(producer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        messageList.add(message);
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
        meterHandler.doBefore(context, messageList);
        assertNotNull(context.getAttributes().get(MessageMeterInterceptor.SEND_STOPWATCH_KEY));
        context.setStatus(MessageHookPointsStatus.OK);
        meterHandler.doAfter(context, messageList);
        ArgumentCaptor<Attributes> attributesArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.SEND_COST_TIME), attributesArgumentCaptor.capture(), Mockito.anyDouble());
        final Attributes attributes = attributesArgumentCaptor.getValue();
        assertEquals(topic, attributes.get(MetricLabels.TOPIC));
        assertEquals(clientId.toString(), attributes.get(MetricLabels.CLIENT_ID));
        assertEquals(InvocationStatus.SUCCESS.getName(), attributes.get(MetricLabels.INVOCATION_STATUS));
    }

    @Test
    public void testSendMessageWithUnset() {
        final ClientImpl producer = Mockito.mock(ClientImpl.class);
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(producer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(producer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        messageList.add(message);
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
        meterHandler.doBefore(context, messageList);
        context.setStatus(MessageHookPointsStatus.UNSET);
        meterHandler.doAfter(context, messageList);
        ArgumentCaptor<Attributes> attributesArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.SEND_COST_TIME), attributesArgumentCaptor.capture(), Mockito.anyDouble());
        final Attributes attributes = attributesArgumentCaptor.getValue();
        assertEquals(topic, attributes.get(MetricLabels.TOPIC));
        assertEquals(clientId.toString(), attributes.get(MetricLabels.CLIENT_ID));
        assertEquals(InvocationStatus.FAILURE.getName(), attributes.get(MetricLabels.INVOCATION_STATUS));
    }

    @Test
    public void testSendMessageWithFailure() {
        final ClientImpl producer = Mockito.mock(ClientImpl.class);
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(producer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(producer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        messageList.add(message);
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
        meterHandler.doBefore(context, messageList);
        context.setStatus(MessageHookPointsStatus.ERROR);
        meterHandler.doAfter(context, messageList);
        ArgumentCaptor<Attributes> attributesArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.SEND_COST_TIME), attributesArgumentCaptor.capture(), Mockito.anyDouble());
        final Attributes attributes = attributesArgumentCaptor.getValue();
        assertEquals(topic, attributes.get(MetricLabels.TOPIC));
        assertEquals(clientId.toString(), attributes.get(MetricLabels.CLIENT_ID));
        assertEquals(InvocationStatus.FAILURE.getName(), attributes.get(MetricLabels.INVOCATION_STATUS));
    }

    interface MyPushConsumer extends Client, PushConsumer {
    }

    interface MySimpleConsumer extends Client, SimpleConsumer {
    }

    @Test
    public void testReceiveMessageForPushConsumer() {
        final MyPushConsumer pushConsumer = Mockito.mock(MyPushConsumer.class);
        String consumerGroup = FAKE_CONSUMER_GROUP_0;
        Mockito.doReturn(consumerGroup).when(pushConsumer).getConsumerGroup();
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(pushConsumer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(pushConsumer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        messageList.add(message);
        final Optional<Long> optionalTransportDeliveryTimestamp = Optional.of(System.currentTimeMillis());
        Mockito.doReturn(optionalTransportDeliveryTimestamp).when(message).getTransportDeliveryTimestamp();
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
        meterHandler.doAfter(context, messageList);
        ArgumentCaptor<Attributes> attributesArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.DELIVERY_LATENCY), attributesArgumentCaptor.capture(),
                Mockito.anyDouble());
        final Attributes attributes = attributesArgumentCaptor.getValue();
        assertEquals(topic, attributes.get(MetricLabels.TOPIC));
        assertEquals(consumerGroup, attributes.get(MetricLabels.CONSUMER_GROUP));
        assertNotNull(attributes.get(MetricLabels.CLIENT_ID));
    }

    @Test
    public void testReceiveMessageForPushConsumerWithEmptyMessage() {
        final MyPushConsumer pushConsumer = Mockito.mock(MyPushConsumer.class);
        Mockito.doReturn(FAKE_CONSUMER_GROUP_0).when(pushConsumer).getConsumerGroup();
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(pushConsumer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(pushConsumer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        Mockito.doReturn(FAKE_TOPIC_0).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        final Optional<Long> optionalTransportDeliveryTimestamp = Optional.of(System.currentTimeMillis());
        Mockito.doReturn(optionalTransportDeliveryTimestamp).when(message).getTransportDeliveryTimestamp();
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
        meterHandler.doAfter(context, messageList);
        Mockito.verify(meterManager, Mockito.never())
            .record(any(HistogramEnum.class), any(Attributes.class), Mockito.anyDouble());
    }

    @Test
    public void testReceiveMessageForSimpleConsumer() {
        final MySimpleConsumer simpleConsumer = Mockito.mock(MySimpleConsumer.class);
        String consumerGroup = FAKE_CONSUMER_GROUP_0;
        Mockito.doReturn(consumerGroup).when(simpleConsumer).getConsumerGroup();
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(simpleConsumer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(simpleConsumer, meterManager);
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        List<GeneralMessage> messageList = new ArrayList<>();
        messageList.add(message);
        final Optional<Long> optionalTransportDeliveryTimestamp = Optional.of(System.currentTimeMillis());
        Mockito.doReturn(optionalTransportDeliveryTimestamp).when(message).getTransportDeliveryTimestamp();
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
        meterHandler.doAfter(context, messageList);
        ArgumentCaptor<Attributes> attributesArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.DELIVERY_LATENCY), attributesArgumentCaptor.capture(),
                Mockito.anyDouble());
        final Attributes attributes = attributesArgumentCaptor.getValue();
        assertEquals(topic, attributes.get(MetricLabels.TOPIC));
        assertEquals(consumerGroup, attributes.get(MetricLabels.CONSUMER_GROUP));
        assertNotNull(attributes.get(MetricLabels.CLIENT_ID));
    }

    @Test
    public void testConsumeMessage() {
        final MyPushConsumer pushConsumer = Mockito.mock(MyPushConsumer.class);
        String consumerGroup = FAKE_CONSUMER_GROUP_0;
        Mockito.doReturn(consumerGroup).when(pushConsumer).getConsumerGroup();
        final ClientMeterManager meterManager = Mockito.mock(ClientMeterManager.class);
        Mockito.doReturn(true).when(meterManager).isEnabled();
        ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(pushConsumer).getClientId();
        final MessageMeterInterceptor meterHandler = new MessageMeterInterceptor(pushConsumer, meterManager);
        List<GeneralMessage> generalMessages = new ArrayList<>();
        final GeneralMessage message = Mockito.mock(GeneralMessage.class);
        generalMessages.add(message);
        String topic = FAKE_TOPIC_0;
        Mockito.doReturn(topic).when(message).getTopic();
        long awaitTimeMills = 3000;
        long decodeTimestamp = System.currentTimeMillis() - awaitTimeMills;
        Mockito.doReturn(Optional.of(decodeTimestamp)).when(message).getDecodeTimestamp();
        final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
        meterHandler.doBefore(context, generalMessages);
        ArgumentCaptor<Attributes> attributes0ArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        final ArgumentCaptor<Double> awaitTimeArgumentCaptor = ArgumentCaptor.forClass(Double.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.AWAIT_TIME), attributes0ArgumentCaptor.capture(),
                awaitTimeArgumentCaptor.capture());
        assertEquals(awaitTimeMills, awaitTimeArgumentCaptor.getValue(), 100);
        final Attributes attributes0 = attributes0ArgumentCaptor.getValue();
        assertEquals(topic, attributes0.get(MetricLabels.TOPIC));
        assertEquals(consumerGroup, attributes0.get(MetricLabels.CONSUMER_GROUP));
        assertEquals(clientId.toString(), attributes0.get(MetricLabels.CLIENT_ID));
        assertNotNull(context.getAttributes().get(MessageMeterInterceptor.CONSUME_STOPWATCH_KEY));

        context.setStatus(MessageHookPointsStatus.OK);
        meterHandler.doAfter(context, generalMessages);
        ArgumentCaptor<Attributes> attributes1ArgumentCaptor = ArgumentCaptor.forClass(Attributes.class);
        Mockito.verify(meterManager, Mockito.times(1))
            .record(Mockito.eq(HistogramEnum.PROCESS_TIME), attributes1ArgumentCaptor.capture(),
                awaitTimeArgumentCaptor.capture());
        final Attributes attributes1 = attributes1ArgumentCaptor.getValue();
        assertEquals(topic, attributes1.get(MetricLabels.TOPIC));
        assertEquals(consumerGroup, attributes1.get(MetricLabels.CONSUMER_GROUP));
        assertEquals(clientId.toString(), attributes1.get(MetricLabels.CLIENT_ID));
        assertEquals(InvocationStatus.SUCCESS.getName(), attributes1.get(MetricLabels.INVOCATION_STATUS));
    }
}