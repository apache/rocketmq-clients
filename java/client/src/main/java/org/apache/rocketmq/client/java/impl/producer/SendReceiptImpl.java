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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ForbiddenException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.PayloadTooLargeException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.RequestHeaderFieldsTooLargeException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnauthorizedException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;
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

    public static List<SendReceiptImpl> processRespContext(MessageQueueImpl mq,
        RpcInvocation<SendMessageResponse> invocation) throws ClientException {
        final String requestId = invocation.getContext().getRequestId();
        final SendMessageResponse response = invocation.getResponse();
        final Status status = response.getStatus();
        List<SendReceiptImpl> sendReceipts = new ArrayList<>();
        final List<SendResultEntry> entries = response.getEntriesList();
        for (SendResultEntry entry : entries) {
            final Status entryStatus = entry.getStatus();
            final Code code = entryStatus.getCode();
            final int codeNumber = code.getNumber();
            final String statusMessage = status.getMessage();
            switch (code) {
                case OK:
                    final MessageId messageId = MessageIdCodec.getInstance().decode(entry.getMessageId());
                    final String transactionId = entry.getTransactionId();
                    final long offset = entry.getOffset();
                    final SendReceiptImpl impl = new SendReceiptImpl(messageId, transactionId, mq, offset);
                    sendReceipts.add(impl);
                    break;
                case ILLEGAL_TOPIC:
                case ILLEGAL_MESSAGE_TAG:
                case ILLEGAL_MESSAGE_KEY:
                case ILLEGAL_MESSAGE_GROUP:
                case ILLEGAL_MESSAGE_PROPERTY_KEY:
                case ILLEGAL_MESSAGE_ID:
                case ILLEGAL_DELIVERY_TIME:
                case MESSAGE_PROPERTY_CONFLICT_WITH_TYPE:
                case MESSAGE_CORRUPTED:
                case CLIENT_ID_REQUIRED:
                    throw new BadRequestException(codeNumber, requestId, statusMessage);
                case UNAUTHORIZED:
                    throw new UnauthorizedException(codeNumber, requestId, statusMessage);
                case FORBIDDEN:
                    throw new ForbiddenException(codeNumber, requestId, statusMessage);
                case NOT_FOUND:
                case TOPIC_NOT_FOUND:
                    throw new NotFoundException(codeNumber, requestId, statusMessage);
                case PAYLOAD_TOO_LARGE:
                case MESSAGE_BODY_TOO_LARGE:
                    throw new PayloadTooLargeException(codeNumber, requestId, statusMessage);
                case TOO_MANY_REQUESTS:
                    throw new TooManyRequestsException(codeNumber, requestId, statusMessage);
                case REQUEST_HEADER_FIELDS_TOO_LARGE:
                case MESSAGE_PROPERTIES_TOO_LARGE:
                    throw new RequestHeaderFieldsTooLargeException(codeNumber, requestId, statusMessage);
                case INTERNAL_ERROR:
                case INTERNAL_SERVER_ERROR:
                case HA_NOT_AVAILABLE:
                    throw new InternalErrorException(codeNumber, requestId, statusMessage);
                case PROXY_TIMEOUT:
                case MASTER_PERSISTENCE_TIMEOUT:
                case SLAVE_PERSISTENCE_TIMEOUT:
                    throw new ProxyTimeoutException(codeNumber, requestId, statusMessage);
                case UNSUPPORTED:
                default:
                    throw new UnsupportedException(codeNumber, requestId, statusMessage);
            }
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
