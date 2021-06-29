package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.FilterType;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.Resource;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.OffsetQuery;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.consumer.filter.FilterExpression;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.MQServerException;
import org.apache.rocketmq.client.impl.ClientInstance;
import org.apache.rocketmq.client.impl.ClientObserver;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.RpcTarget;
import org.apache.rocketmq.client.route.Partition;

@Slf4j
public class DefaultMQPullConsumerImpl implements ClientObserver {
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private final ClientInstance clientInstance;
    private final AtomicReference<ServiceState> state;

    public DefaultMQPullConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.clientInstance = new ClientInstance(defaultMQPullConsumer, this);
        this.state = new AtomicReference<ServiceState>(ServiceState.CREATED);
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
        return clientInstance.queryOffset(partition.getRpcTarget(), request);
    }

    public PullResult pull(PullMessageQuery pullMessageQuery) throws MQClientException {
        final MessageQueue messageQueue = pullMessageQuery.getMessageQueue();
        final long queueOffset = pullMessageQuery.getQueueOffset();
        final long awaitTimeMillis = pullMessageQuery.getAwaitTimeMillis();
        final int batchSize = pullMessageQuery.getBatchSize();
        final String arn = this.getArn();
        final Resource groupResource =
                Resource.newBuilder().setArn(arn).setName(defaultMQPullConsumer.getConsumerGroup()).build();

        final Resource topicResource = Resource.newBuilder().setArn(arn).setName(messageQueue.getTopic()).build();
        final apache.rocketmq.v1.Partition partition =
                apache.rocketmq.v1.Partition.newBuilder()
                                            .setTopic(topicResource)
                                            .setId(messageQueue.getPartition().getId())
                                            .build();
        final PullMessageRequest.Builder requestBuilder =
                PullMessageRequest.newBuilder().setClientId(defaultMQPullConsumer.getClientId())
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

        final RpcTarget rpcTarget = messageQueue.getPartition().getRpcTarget();
        return clientInstance.pullMessage(rpcTarget, request,
                                          awaitTimeMillis + ClientInstance.RPC_DEFAULT_TIMEOUT_MILLIS,
                                          TimeUnit.MILLISECONDS);
    }

    public boolean hasBeenStarted() {
        final ServiceState serviceState = state.get();
        return ServiceState.CREATED != serviceState;
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    public void pull(PullMessageQuery pullMessageQuery, PullCallback callback) {
        throw new UnsupportedOperationException();
    }

    public void start() throws MQClientException {
        final String consumerGroup = defaultMQPullConsumer.getConsumerGroup();
        if (!state.compareAndSet(ServiceState.CREATED, ServiceState.STARTING)) {
            throw new MQClientException(
                    "The pull consumer has attempted to be started before, consumerGroup=" + consumerGroup);
        }

        clientInstance.start();
        state.compareAndSet(ServiceState.STARTING, ServiceState.STARTED);
        log.info("Start DefaultMQPullConsumerImpl successfully.");
    }

    public void shutdown() throws MQClientException {
        state.compareAndSet(ServiceState.STARTING, ServiceState.STOPPING);
        state.compareAndSet(ServiceState.STARTED, ServiceState.STOPPING);
        final ServiceState serviceState = state.get();
        if (ServiceState.STOPPING == serviceState) {
            clientInstance.shutdown();
            if (state.compareAndSet(ServiceState.STOPPING, ServiceState.STOPPED)) {
                log.info("Shutdown DefaultMQPullConsumerImpl successfully.");
                return;
            }
        }
        throw new MQClientException("Failed to shutdown consumer, state=" + state.get());
    }

    private String getArn() {
        return defaultMQPullConsumer.getArn();
    }

    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        final Resource groupResource =
                Resource.newBuilder().setArn(this.getArn()).setName(defaultMQPullConsumer.getConsumerGroup()).build();
        final ConsumerGroup consumerGroup = ConsumerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder().setClientId(defaultMQPullConsumer.getClientId())
                             .setConsumerGroup(consumerGroup).build();
    }

    @Override
    public void logStats() {

    }
}



