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

package org.apache.rocketmq.client.java.impl.consumer;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageHandler;
import org.apache.rocketmq.client.java.hook.MessageHandlerContext;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class FifoConsumeServiceTest extends TestBase {

    @Test
    public void testDispatch() throws InterruptedException {
        final ProcessQueue processQueue0 = mock(ProcessQueue.class);
        final ProcessQueue processQueue1 = mock(ProcessQueue.class);

        final MessageQueueImpl messageQueue0 = fakeMessageQueueImpl0();
        final MessageQueueImpl messageQueue1 = fakeMessageQueueImpl1();

        ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable = new ConcurrentHashMap<>();
        processQueueTable.put(messageQueue0, processQueue0);
        processQueueTable.put(messageQueue1, processQueue1);

        final MessageViewImpl messageView00 = fakeMessageViewImpl(messageQueue0);
        final MessageViewImpl messageView01 = fakeMessageViewImpl(messageQueue0);
        List<MessageViewImpl> messageViewList0 = new ArrayList<>();
        messageViewList0.add(messageView00);
        messageViewList0.add(messageView01);

        final MessageViewImpl messageView10 = fakeMessageViewImpl(messageQueue1);
        List<MessageViewImpl> messageViewList1 = new ArrayList<>();
        messageViewList1.add(messageView10);

        when(processQueue0.tryTakeFifoMessages()).thenReturn(messageViewList0.iterator());
        when(processQueue1.tryTakeFifoMessages()).thenReturn(messageViewList1.iterator());

        MessageListener listener = messageView -> ConsumeResult.SUCCESS;
        MessageHandler interceptor = new MessageHandler() {
            @Override
            public void doBefore(MessageHandlerContext context, List<GeneralMessage> messages) {
            }

            @Override
            public void doAfter(MessageHandlerContext context, List<GeneralMessage> messages) {
            }
        };
        final FifoConsumeService service = new FifoConsumeService(FAKE_CLIENT_ID, processQueueTable, listener,
            SINGLE_THREAD_POOL_EXECUTOR, interceptor, SCHEDULER);
        service.dispatch();
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            verify(processQueue0, times(1)).tryTakeFifoMessages();
            verify(processQueue1, times(1)).tryTakeFifoMessages();
            verify(processQueue0, times(2)).eraseFifoMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
            verify(processQueue1, times(1)).eraseFifoMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
        });
    }
}