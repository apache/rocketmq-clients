package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.Resource;
import com.google.protobuf.util.Timestamps;
import org.apache.rocketmq.client.OffsetQuery;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.message.MessageQueue;

public class DefaultMQPullConsumerImpl {
    private final DefaultMQPullConsumer defaultMQPullConsumer;

    public DefaultMQPullConsumerImpl(DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    public long queryOffset(OffsetQuery offsetQuery) {
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
        //        Broker broker = Broker.newBuilder().setName()
        //        Partition partition = Partition.newBuilder().setTopic(topicResource).setBroker()
        throw new UnsupportedOperationException();
    }

    public PullResult pull(PullMessageQuery pullMessageQuery) {
        throw new UnsupportedOperationException();
    }

    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeoutMillis) {
        throw new UnsupportedOperationException();
    }

    public void pull(PullMessageQuery pullMessageQuery, PullCallback callback) {
    }

    public void start() {
    }

    public void shutdown() {
    }

    private String getArn() {
        return defaultMQPullConsumer.getArn();
    }
}



