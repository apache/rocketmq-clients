package org.apache.rocketmq.client.consumer;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.AccessCredential;

@Getter
@Setter
public class DefaultMQPullConsumer {
    private final DefaultMQPullConsumerImpl impl;

    public DefaultMQPullConsumer(final String consumerGroup) {
        this.impl = new DefaultMQPullConsumerImpl(consumerGroup);
    }

    public void setConsumerGroup(String group) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setGroup(group);
        }
    }

    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    public void start() throws ClientException {
        this.impl.start();
    }

    public void shutdown() throws ClientException {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            this.impl.setNamesrvAddr(namesrvAddr);
        }
    }

    public ListenableFuture<List<MessageQueue>> queuesFor(String topic) {
        return this.impl.getQueuesFor(topic);
    }

    public ListenableFuture<Long> queryOffset(OffsetQuery offsetQuery) {
        return this.impl.queryOffset(offsetQuery);
    }

    public ListenableFuture<PullResult> pull(PullMessageQuery pullMessageQuery) {
        return this.impl.pull(pullMessageQuery);
    }

    public void pull(PullMessageQuery pullMessageQuery, final PullCallback callback) {
        this.impl.pull(pullMessageQuery, callback);
    }

    public void setArn(String arn) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setArn(arn);
        }
    }

    public void setAccessCredential(AccessCredential accessCredential) throws ClientException {
        synchronized (impl) {
            if (ServiceState.READY != impl.getState()) {
                throw new ClientException(ErrorCode.NOT_SUPPORTED_OPERATION);
            }
            impl.setAccessCredential(accessCredential);
        }
    }
}
