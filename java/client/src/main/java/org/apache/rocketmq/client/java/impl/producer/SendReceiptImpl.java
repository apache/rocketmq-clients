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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import apache.rocketmq.v2.Status;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;

public class SendReceiptImpl implements SendReceipt {
    private final MessageId messageId;
    private final String transactionId;
    private final MessageQueueImpl messageQueue;
    private final long offset;

    private SendReceiptImpl(MessageId messageId, String transactionId, MessageQueueImpl messageQueue, long offset) {
        this.messageId = messageId;
        this.transactionId = transactionId;
        this.messageQueue = messageQueue;
        this.offset = offset;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    public MessageQueueImpl getMessageQueue() {
        return messageQueue;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Endpoints getEndpoints() {
        return messageQueue.getBroker().getEndpoints();
    }

    @SuppressWarnings("unused")
    public long getOffset() {
        return offset;
    }

    public static List<SendReceiptImpl> processResponseInvocation(MessageQueueImpl mq,
        RpcInvocation<SendMessageResponse> invocation) throws ClientException {
        final SendMessageResponse response = invocation.getResponse();
        Status status = response.getStatus();
        List<SendReceiptImpl> sendReceipts = new ArrayList<>();
        final List<SendResultEntry> entries = response.getEntriesList();
        // Filter abnormal status.
        final Optional<Status> abnormalStatus = entries.stream()
            .map(SendResultEntry::getStatus).filter((Predicate<Status>) s -> !s.getCode().equals(Code.OK)).findFirst();
        if (abnormalStatus.isPresent()) {
            status = abnormalStatus.get();
        }
        StatusChecker.check(status, invocation);
        for (SendResultEntry entry : entries) {
            final MessageId messageId = MessageIdCodec.getInstance().decode(entry.getMessageId());
            final String transactionId = entry.getTransactionId();
            final long offset = entry.getOffset();
            final SendReceiptImpl impl = new SendReceiptImpl(messageId, transactionId, mq, offset);
            sendReceipts.add(impl);
        }
        return sendReceipts;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", messageId)
            .toString();
    }
}
