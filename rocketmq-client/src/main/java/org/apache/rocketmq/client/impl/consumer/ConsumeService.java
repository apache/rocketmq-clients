package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.rocketmq.client.consumer.listener.ConsumeStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.route.Partition;

public interface ConsumeService {
    void start() throws MQClientException;

    void shutdown();

    void dispatch(ProcessQueue processQueue);

    ListenableFuture<ConsumeStatus> verifyMessageConsumption(MessageExt messageExt, Partition partition);
}
