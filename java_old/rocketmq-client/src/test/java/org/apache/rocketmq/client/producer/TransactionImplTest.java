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


package org.apache.rocketmq.client.producer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.impl.producer.ProducerImpl;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionImplTest extends TestBase {
    @Mock
    private ProducerImpl producer;

    @Mock
    private Message message;

    @Mock
    private SendResult sendResult;

    @InjectMocks
    private TransactionImpl transactionImpl;


    @BeforeMethod
    public void beforeMethod() throws ServerException, ClientException, InterruptedException, TimeoutException {
        MockitoAnnotations.initMocks(this);
        this.transactionImpl = new TransactionImpl(sendResult, message, producer);
        when(message.getMessageExt()).thenReturn(fakeMessageExt());
        when(sendResult.getEndpoints()).thenReturn(fakeEndpoints0());
        when(sendResult.getTransactionId()).thenReturn("fakeTransactionId");
        doNothing().when(producer).rollback(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<MessageExt>any(),
                                            anyString());
    }

    @Test
    public void testCommit() throws ServerException, ClientException, InterruptedException, TimeoutException {
        doNothing().when(producer).commit(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<MessageExt>any(),
                                          anyString());
        transactionImpl.commit();
        verify(producer, times(1)).commit(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<MessageExt>any(),
                                          anyString());
    }

    @Test
    public void testRollback() throws ServerException, ClientException, InterruptedException, TimeoutException {
        doNothing().when(producer).rollback(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<MessageExt>any(),
                                            anyString());
        transactionImpl.rollback();
        verify(producer, times(1)).rollback(ArgumentMatchers.<Endpoints>any(), ArgumentMatchers.<MessageExt>any(),
                                            anyString());
    }

    @Test
    public void testGetSendResult() {
        assertEquals(transactionImpl.getSendResult(), sendResult);
    }
}