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

package org.apache.rocketmq.client.java.impl.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.MessageType;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransactionImplTest extends TestBase {
    @Mock
    ProducerImpl producer;

    @Test
    public void testTryAddMessage() throws IOException {
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message);
        Assert.assertEquals(publishingMessage.getMessageType(), MessageType.TRANSACTION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTryAddExceededMessages() throws IOException {
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message0 = fakeMessage(FAKE_TOPIC_0);
        transaction.tryAddMessage(message0);
        final Message message1 = fakeMessage(FAKE_TOPIC_0);
        transaction.tryAddMessage(message1);
    }

    @Test
    public void testTryAddReceipt() throws IOException, ClientException, ExecutionException, InterruptedException {
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message);
        final SendReceiptImpl sendReceipt = fakeSendReceiptImpl(fakeMessageQueueImpl(FAKE_TOPIC_0));
        transaction.tryAddReceipt(publishingMessage, sendReceipt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTryAddReceiptNotContained() throws ClientException, ExecutionException, InterruptedException {
        PublishingMessageImpl publishingMessage = Mockito.mock(PublishingMessageImpl.class);
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final SendReceiptImpl sendReceipt = fakeSendReceiptImpl(fakeMessageQueueImpl(FAKE_TOPIC_0));
        transaction.tryAddReceipt(publishingMessage, sendReceipt);
    }

    @Test(expected = IllegalStateException.class)
    public void testCommitWithNoReceipts() throws ClientException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        transaction.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testRollbackWithNoReceipts() throws ClientException {
        final TransactionImpl transaction = new TransactionImpl(producer);
        transaction.rollback();
    }

    @Test
    public void testCommit() throws IOException, ClientException, ExecutionException, InterruptedException {
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message);
        final SendReceiptImpl sendReceipt = fakeSendReceiptImpl(fakeMessageQueueImpl(FAKE_TOPIC_0));
        transaction.tryAddReceipt(publishingMessage, sendReceipt);
        Mockito.doNothing().when(producer).endTransaction(any(Endpoints.class), any(GeneralMessage.class),
            any(MessageId.class), anyString(), any(TransactionResolution.class));
        transaction.commit();
    }

    @Test
    public void testRollback() throws IOException, ClientException, ExecutionException, InterruptedException {
        Set<String> set = new HashSet<>();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        final ProducerImpl producer = Mockito.spy(new ProducerImpl(clientConfiguration, set, 1, null));
        final TransactionImpl transaction = new TransactionImpl(producer);
        final Message message = fakeMessage(FAKE_TOPIC_0);
        final PublishingMessageImpl publishingMessage = transaction.tryAddMessage(message);
        final SendReceiptImpl sendReceipt = fakeSendReceiptImpl(fakeMessageQueueImpl(FAKE_TOPIC_0));
        transaction.tryAddReceipt(publishingMessage, sendReceipt);
        Mockito.doNothing().when(producer).endTransaction(any(Endpoints.class), any(GeneralMessage.class),
            any(MessageId.class), anyString(), any(TransactionResolution.class));
        transaction.rollback();
    }
}