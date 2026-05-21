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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.ConsumeResultSuspend;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;

public class LiteFifoConsumeService extends FifoConsumeService {

    public LiteFifoConsumeService(ClientId clientId, MessageListener messageListener,
        ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler, boolean enableFifoConsumeAccelerator) {
        super(clientId, messageListener, consumptionExecutor,
            messageInterceptor, scheduler, enableFifoConsumeAccelerator);
    }

    @Override
    protected String getMessageGroupKey(MessageViewImpl messageView) {
        return messageView.getLiteTopic().orElse(null);
    }

    @Override
    protected void consumeIteratively(ProcessQueue pq, Iterator<MessageViewImpl> iterator) {
        MessageViewImpl messageView = getNextValidMessage(pq, iterator);
        if (messageView == null) {
            return;
        }
        final ListenableFuture<ConsumeResult> future0 = consume(messageView);

        final AtomicReference<Iterator<MessageViewImpl>> iteratorRef = new AtomicReference<>(iterator);
        ListenableFuture<Void> future = Futures.transformAsync(future0, result -> {
            ListenableFuture<Void> returnFuture = pq.eraseFifoMessage(messageView, result);
            if (result instanceof ConsumeResultSuspend) {
                List<MessageViewImpl> newMsgList = new ArrayList<>();
                iterator.forEachRemaining(msgView -> {
                    boolean sameLiteTopic = messageView.getLiteTopic().equals(msgView.getLiteTopic());
                    if (sameLiteTopic) {
                        // Suspend all messages with the same liteTopic in this batch.
                        pq.eraseFifoMessage(msgView, result);
                    } else {
                        newMsgList.add(msgView);
                    }
                });
                iteratorRef.set(newMsgList.iterator());
            }
            return returnFuture;
        }, MoreExecutors.directExecutor());
        future.addListener(() -> consumeIteratively(pq, iteratorRef.get()), MoreExecutors.directExecutor());
    }
}
