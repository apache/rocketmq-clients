package org.apache.rocketmq.client.apis.consumer;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientException;

public interface LitePushConsumer extends PushConsumer {

    LitePushConsumer subscribeLite(Collection<String> liteTopics) throws ClientException;

    LitePushConsumer subscribeLite(String liteTopic) throws ClientException;

    LitePushConsumer unsubscribeLite(String liteTopic) throws ClientException;

}
