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

package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.consumer.ReceiveStatus;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.impl.ClientImpl;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConsumerImpl extends ClientImpl {
    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

    public ConsumerImpl(String group) {
        super(group);
    }

    protected ListenableFuture<Long> queryOffset(QueryOffsetRequest request, final Endpoints endpoints) {
        final SettableFuture<Long> future0 = SettableFuture.create();
        try {
            Metadata metadata = sign();
            ListenableFuture<QueryOffsetResponse> future =
                    clientManager.queryOffset(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
            return Futures.transformAsync(future, new AsyncFunction<QueryOffsetResponse, Long>() {
                @Override
                public ListenableFuture<Long> apply(QueryOffsetResponse response) throws Exception {
                    final Status status = response.getCommon().getStatus();
                    final Code code = Code.forNumber(status.getCode());
                    if (Code.OK != code) {
                        log.error("Failed to query offset, endpoints={}, code={}, status message=[{}]", endpoints, code,
                                  status.getMessage());
                        throw new ClientException(ErrorCode.SEEK_OFFSET_FAILURE, status.getMessage());
                    }
                    final long offset = response.getOffset();
                    future0.set(offset);
                    return future0;
                }
            });
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
    }

    public ListenableFuture<PullMessageResult> pullMessage(PullMessageRequest request, final Endpoints endpoints,
                                                           long timeoutMillis) {
        final SettableFuture<PullMessageResult> future0 = SettableFuture.create();
        try {
            Metadata metadata = sign();
            final ListenableFuture<PullMessageResponse> future =
                    clientManager.pullMessage(endpoints, metadata, request, timeoutMillis, TimeUnit.MILLISECONDS);
            return Futures.transform(future, new Function<PullMessageResponse, PullMessageResult>() {
                @Override
                public PullMessageResult apply(PullMessageResponse response) {
                    return processPullMessageResponse(endpoints, response);
                }
            });
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
    }

    public static PullMessageResult processPullMessageResponse(Endpoints endpoints, PullMessageResponse response) {
        PullStatus pullStatus;
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (code != null ? code : Code.UNKNOWN) {
            case OK:
                pullStatus = PullStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                pullStatus = PullStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, endpoints={}, status message=[{}]", endpoints,
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                pullStatus = PullStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, endpoints={}, status message=[{}]", endpoints, status.getMessage());
                break;
            case NOT_FOUND:
                pullStatus = PullStatus.NOT_FOUND;
                log.warn("Target partition does not exist, endpoints={}, status message=[{}]", endpoints,
                         status.getMessage());
                break;
            case OUT_OF_RANGE:
                pullStatus = PullStatus.OUT_OF_RANGE;
                log.warn("Pulled offset is out of range, endpoints={}, status message=[{}]", endpoints,
                         status.getMessage());
                break;
            default:
                pullStatus = PullStatus.INTERNAL;
                log.warn("Pull response indicated server-side error, endpoints={}, code={}, status message=[{}]",
                         endpoints, code, status.getMessage());
        }
        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (PullStatus.OK.equals(pullStatus)) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                MessageImpl messageImpl = ClientImpl.wrapMessageImpl(message);
                msgFoundList.add(new MessageExt(messageImpl));
            }
        }
        return new PullMessageResult(pullStatus, response.getNextOffset(), response.getMinOffset(),
                                     response.getMaxOffset(), msgFoundList);
    }

    protected ListenableFuture<ReceiveMessageResult> receiveMessage(ReceiveMessageRequest request,
                                                                    final Endpoints endpoints, long timeoutMillis) {
        final SettableFuture<ReceiveMessageResult> future0 = SettableFuture.create();
        try {
            Metadata metadata = sign();
            final ListenableFuture<ReceiveMessageResponse> future =
                    clientManager.receiveMessage(endpoints, metadata, request, timeoutMillis, TimeUnit.MILLISECONDS);
            return Futures.transform(future, new Function<ReceiveMessageResponse, ReceiveMessageResult>() {
                @Override
                public ReceiveMessageResult apply(ReceiveMessageResponse response) {
                    return processReceiveMessageResponse(endpoints, response);
                }
            });
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
    }

    public static ReceiveMessageResult processReceiveMessageResponse(Endpoints endpoints,
                                                                     ReceiveMessageResponse response) {
        ReceiveStatus receiveStatus;
        final Status status = response.getCommon().getStatus();
        final Code code = Code.forNumber(status.getCode());
        switch (null != code ? code : Code.UNKNOWN) {
            case OK:
                receiveStatus = ReceiveStatus.OK;
                break;
            case RESOURCE_EXHAUSTED:
                receiveStatus = ReceiveStatus.RESOURCE_EXHAUSTED;
                log.warn("Too many request in server, endpoints={}, status message=[{}]", endpoints,
                         status.getMessage());
                break;
            case DEADLINE_EXCEEDED:
                receiveStatus = ReceiveStatus.DEADLINE_EXCEEDED;
                log.warn("Gateway timeout, endpoints={}, status message=[{}]", endpoints,
                         status.getMessage());
                break;
            default:
                receiveStatus = ReceiveStatus.INTERNAL;
                log.warn("Receive response indicated server-side error, endpoints={}, code={}, status message=[{}]",
                         endpoints, code, status.getMessage());
        }

        List<MessageExt> msgFoundList = new ArrayList<MessageExt>();
        if (ReceiveStatus.OK.equals(receiveStatus)) {
            final List<Message> messageList = response.getMessagesList();
            for (Message message : messageList) {
                MessageImpl messageImpl;
                messageImpl = ClientImpl.wrapMessageImpl(message);
                messageImpl.getSystemAttribute().setAckEndpoints(endpoints);
                msgFoundList.add(new MessageExt(messageImpl));
            }
        }

        return new ReceiveMessageResult(endpoints, receiveStatus, Timestamps.toMillis(response.getDeliveryTimestamp()),
                                        Durations.toMillis(response.getInvisibleDuration()), msgFoundList);
    }
}



