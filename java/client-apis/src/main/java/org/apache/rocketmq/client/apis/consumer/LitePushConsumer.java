package org.apache.rocketmq.client.apis.consumer;

import java.util.Collection;
import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumer extends PushConsumer {

    LitePushConsumer subscribeLite(Collection<String> liteTopics) throws ClientException;

    void subscribeLite(String liteTopic) throws ClientException;

    LitePushConsumer unsubscribeLite(String liteTopic) throws ClientException;

}
