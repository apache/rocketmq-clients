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
import java.util.List;
import java.util.Optional;
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

public class StandardConsumeServiceTest extends TestBase {

    @Test
    public void testDispatch() throws InterruptedException {
        final ProcessQueue processQueue0 = mock(ProcessQueue.class);
        final ProcessQueue processQueue1 = mock(ProcessQueue.class);

        final MessageQueueImpl messageQueue0 = fakeMessageQueueImpl0();
        final MessageQueueImpl messageQueue1 = fakeMessageQueueImpl1();

        ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable = new ConcurrentHashMap<>();
        processQueueTable.put(messageQueue0, processQueue0);
        processQueueTable.put(messageQueue1, processQueue1);

        final MessageViewImpl messageView0 = fakeMessageViewImpl(messageQueue0);
        final MessageViewImpl messageView1 = fakeMessageViewImpl(messageQueue1);

        when(processQueue0.tryTakeMessage()).thenReturn(Optional.of(messageView0));
        when(processQueue1.tryTakeMessage()).thenReturn(Optional.of(messageView1));

        MessageListener listener = messageView -> ConsumeResult.SUCCESS;
        MessageHandler interceptor = new MessageHandler() {
            @Override
            public void doBefore(MessageHandlerContext context, List<GeneralMessage> messages) {

            }

            @Override
            public void doAfter(MessageHandlerContext context, List<GeneralMessage> messages) {

            }
        };
        final StandardConsumeService service = new StandardConsumeService(FAKE_CLIENT_ID, processQueueTable, listener,
            SINGLE_THREAD_POOL_EXECUTOR, interceptor, SCHEDULER);
        service.dispatch0();
        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> {
            verify(processQueue0, times(1)).tryTakeMessage();
            verify(processQueue1, times(1)).tryTakeMessage();
            verify(processQueue0, times(1)).eraseMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
            verify(processQueue1, times(1)).eraseMessage(any(MessageViewImpl.class), any(ConsumeResult.class));
        });
    }
}