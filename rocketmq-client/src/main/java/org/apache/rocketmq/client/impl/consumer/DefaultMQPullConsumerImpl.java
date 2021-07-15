package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.Resource;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.OffsetQuery;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;

@Slf4j
public class DefaultMQPullConsumerImpl extends ClientBaseImpl {
    private final ClientInstance clientInstance;
    private final AtomicReference<ServiceState> state;

    public DefaultMQPullConsumerImpl(String group) {
        super(group);
        this.clientInstance = ClientManager.getInstance().getClientInstance(this);

        this.state = new AtomicReference<ServiceState>(ServiceState.READY);
        //        this.defaultMQPullConsumer = defaultMQPullConsumer;
        //        // TODO
        //        this.clientInstance = ClientManager.getInstance().getClientInstance(null);
        //        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
    }

    public long queryOffset(OffsetQuery offsetQuery) throws MQServerException, MQClientException {
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
        final Partition partition = messageQueue.getPartition();
        Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(messageQueue.getTopic()).build();
        QueryOffsetRequest request = QueryOffsetRequest
                .newBuilder()
                .setPartition(apache.rocketmq.v1.Partition.newBuilder()
                                                          .setTopic(topicResource)
                                                          .setId(partition.getId())
                                                          .build())
                .build();
        throw new UnsupportedOperationException();
    }

    public ListenableFuture<PullResult> pull(PullMessageQuery pullMessageQuery) throws MQClientException {
        final MessageQueue messageQueue = pullMessageQuery.getMessageQueue();
        final long queueOffset = pullMessageQuery.getQueueOffset();
        final long awaitTimeMillis = pullMessageQuery.getAwaitTimeMillis();
        final int batchSize = pullMessageQuery.getBatchSize();
        final String arn = this.getArn();
        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(group).build();

        final Resource topicResource = Resource.newBuilder().setArn(arn).setName(messageQueue.getTopic()).build();
        final apache.rocketmq.v1.Partition partition =
                apache.rocketmq.v1.Partition.newBuilder()
                                            .setTopic(topicResource)
                                            .setId(messageQueue.getPartition().getId())
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

        final PullMessageRequest request = requestBuilder.build();
        final RpcTarget target = messageQueue.getPartition().getTarget();

        final SettableFuture<PullResult> future = SettableFuture.create();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            future.setException(t);
            return future;
        }
        final ListenableFuture<PullMessageResponse> responseFuture =
                clientInstance.pullMessage(target, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        return Futures.transformAsync(responseFuture, new AsyncFunction<PullMessageResponse, PullResult>() {
            @Override
            public ListenableFuture<PullResult> apply(PullMessageResponse response) {
                final PullResult pullResult = processPullMessageResponse(target, response);
                future.set(pullResult);
                return future;
            }
        });
    }

    public boolean hasBeenStarted() {
        final ServiceState serviceState = state.get();
        return ServiceState.READY != serviceState;
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    public void pull(PullMessageQuery pullMessageQuery, PullCallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() throws MQClientException {
        synchronized (this) {
            log.warn("Begin to start the rocketmq pull consumer.");
            if (!state.compareAndSet(ServiceState.CREATED, ServiceState.READY)) {
                log.warn("The rocketmq pull consumer has been started before.");
                return;
            }
            super.start();
            state.compareAndSet(ServiceState.READY, ServiceState.STARTED);
            log.info("The rocketmq pull consumer starts successfully.");
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq pull consumer.");
            if (!state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING)) {
                log.warn("The rocketmq pull consumer has not been started before");
                return;
            }
            super.shutdown();
            state.compareAndSet(ServiceState.STOPPING, ServiceState.READY);
            log.info("Shutdown the rocketmq pull consumer successfully.");
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
    public void logStats() {

    }
}



