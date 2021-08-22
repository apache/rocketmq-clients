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

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.OffsetQuery;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullConsumerImpl extends ConsumerImpl {
    private static final Logger log = LoggerFactory.getLogger(PullConsumerImpl.class);

    private final ThreadPoolExecutor pullCallbackExecutor;

    public PullConsumerImpl(String group) {
        super(group);
        this.pullCallbackExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("PullCallbackWorker"));
    }

    public ListenableFuture<List<MessageQueue>> getQueuesFor(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicRouteData, List<MessageQueue>>() {
            @Override
            public ListenableFuture<List<MessageQueue>> apply(TopicRouteData topicRouteData) throws Exception {
                // TODO: polish code
                List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
                final List<Partition> partitions = topicRouteData.getPartitions();
                for (Partition partition : partitions) {
                    if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                        continue;
                    }
                    if (partition.getPermission().isReadable()) {
                        final MessageQueue messageQueue = new MessageQueue(partition);
                        messageQueues.add(messageQueue);
                    }
                }
                if (messageQueues.isEmpty()) {
                    throw new ClientException(ErrorCode.NO_PERMISSION);
                }
                SettableFuture<List<MessageQueue>> future0 = SettableFuture.create();
                future0.set(messageQueues);
                return future0;
            }
        });
    }

    private QueryOffsetRequest wrapQueryOffsetRequest(OffsetQuery offsetQuery) {
        final QueryOffsetRequest.Builder builder = QueryOffsetRequest.newBuilder();
        final QueryOffsetPolicy queryOffsetPolicy = offsetQuery.getQueryOffsetPolicy();
        switch (queryOffsetPolicy) {
            case END:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.END);
                break;
            case TIME_POINT:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.TIME_POINT);
                builder.setTimePoint(Timestamps.fromMillis(offsetQuery.getTimePoint()));
                break;
            case BEGINNING:
            default:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.BEGINNING);
        }
        final MessageQueue messageQueue = offsetQuery.getMessageQueue();
        Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(messageQueue.getTopic()).build();
        int partitionId = messageQueue.getPartition().getId();
        final apache.rocketmq.v1.Broker broker =
                apache.rocketmq.v1.Broker.newBuilder().setName(messageQueue.getBrokerName()).build();
        final apache.rocketmq.v1.Partition partition = apache.rocketmq.v1.Partition.newBuilder()
                                                                                   .setBroker(broker)
                                                                                   .setTopic(topicResource)
                                                                                   .setId(partitionId)
                                                                                   .build();
        return builder.setPartition(partition).build();
    }

    private PullMessageRequest wrapPullMessageRequest(PullMessageQuery pullMessageQuery) {
        final MessageQueue messageQueue = pullMessageQuery.getMessageQueue();
        final long queueOffset = pullMessageQuery.getQueueOffset();
        final long awaitTimeMillis = pullMessageQuery.getAwaitTimeMillis();
        final int batchSize = pullMessageQuery.getBatchSize();
        final String arn = this.getArn();
        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();

        final Resource topicResource = Resource.newBuilder().setArn(arn).setName(messageQueue.getTopic()).build();
        final apache.rocketmq.v1.Broker broker =
                apache.rocketmq.v1.Broker.newBuilder().setName(messageQueue.getBrokerName()).build();
        final apache.rocketmq.v1.Partition partition =
                apache.rocketmq.v1.Partition.newBuilder()
                                            .setTopic(topicResource)
                                            .setId(messageQueue.getPartition().getId())
                                            .setBroker(broker)
                                            .build();
        final PullMessageRequest.Builder requestBuilder =
                PullMessageRequest.newBuilder().setClientId(clientId)
                                  .setAwaitTime(Durations.fromMillis(awaitTimeMillis))
                                  .setBatchSize(batchSize)
                                  .setGroup(groupResource)
                                  .setPartition(partition)
                                  .setOffset(queueOffset);

        final FilterExpression filterExpression = pullMessageQuery.getFilterExpression();
        apache.rocketmq.v1.FilterExpression.Builder filterExpressionBuilder =
                apache.rocketmq.v1.FilterExpression.newBuilder();
        switch (filterExpression.getExpressionType()) {
            case TAG:
                filterExpressionBuilder.setType(FilterType.TAG);
                break;
            case SQL92:
            default:
                filterExpressionBuilder.setType(FilterType.SQL);
        }
        filterExpressionBuilder.setExpression(filterExpression.getExpression());
        requestBuilder.setFilterExpression(filterExpressionBuilder.build());

        return requestBuilder.build();
    }

    public ListenableFuture<Long> queryOffset(OffsetQuery offsetQuery) {
        final QueryOffsetRequest request = wrapQueryOffsetRequest(offsetQuery);
        final Endpoints endpoints = offsetQuery.getMessageQueue().getPartition().getBroker().getEndpoints();
        return queryOffset(request, endpoints);
    }

    public ListenableFuture<PullMessageResult> pullMessage(PullMessageQuery pullMessageQuery) {
        final PullMessageRequest request = wrapPullMessageRequest(pullMessageQuery);
        final Endpoints endpoints = pullMessageQuery.getMessageQueue().getPartition().getBroker().getEndpoints();
        final long timeoutMillis = pullMessageQuery.getTimeoutMillis();
        return pullMessage(request, endpoints, timeoutMillis);
    }

    public void pull(PullMessageQuery pullMessageQuery, final PullCallback pullCallback) {
        final PullMessageRequest request = wrapPullMessageRequest(pullMessageQuery);
        final Endpoints endpoints = pullMessageQuery.getMessageQueue().getPartition().getBroker().getEndpoints();
        final ListenableFuture<PullMessageResult> future = pullMessage(request, endpoints,
                                                                       pullMessageQuery.getTimeoutMillis());
        Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
            @Override
            public void onSuccess(final PullMessageResult pullMessageResult) {
                try {
                    pullCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                pullCallback.onSuccess(pullMessageResult);
                            } catch (Throwable t) {
                                log.error("Exception occurs in PullCallback#onSuccess", t);
                            }
                        }
                    });
                } catch (Throwable t) {
                    log.error("Exception occurs while submitting task to pull callback executor", t);
                }
            }

            @Override
            public void onFailure(final Throwable t) {
                try {
                    pullCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                pullCallback.onException(t);
                            } catch (Throwable t) {
                                log.error("Exception occurs in PullCallback#onException", t);
                            }
                        }
                    });
                } catch (Throwable t0) {
                    log.error("Exception occurs while submitting task to pull callback executor", t0);
                }
            }
        });
    }

    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.warn("Begin to start the rocketmq pull consumer.");
            super.start();
            if (this.isStarted()) {
                log.info("The rocketmq pull consumer starts successfully.");
            }
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq pull consumer.");
            super.shutdown();
            if (this.isStopped()) {
                pullCallbackExecutor.shutdown();
                if (!pullCallbackExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                    log.error("[Bug] Failed to shutdown the pull executor.");
                }
                log.info("Shutdown the rocketmq pull consumer successfully.");
            }
        }
    }


    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        final Resource groupResource =
                Resource.newBuilder().setArn(this.getArn()).setName(group).build();
        final ConsumerGroup consumerGroup = ConsumerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder().setClientId(clientId)
                             .setConsumerGroup(consumerGroup).build();
    }

    @Override
    public void doStats() {

    }

    @Override
    public ClientResourceBundle wrapClientResourceBundle() {
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(getProtoGroup());
        return builder.build();
    }
}



