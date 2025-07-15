package org.apache.rocketmq.client.apis.consumer;

import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumer extends PushConsumer {

    LitePushConsumer subscribeLite(String liteTopic) throws ClientException;

    LitePushConsumer unsubscribeLite(String liteTopic) throws ClientException;

}
