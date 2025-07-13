package org.apache.rocketmq.client.apis.consumer;

import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumer extends PushConsumer {

    PushConsumer addInterest(String liteTopic) throws ClientException;

    PushConsumer removeInterest(String liteTopic) throws ClientException;

}
