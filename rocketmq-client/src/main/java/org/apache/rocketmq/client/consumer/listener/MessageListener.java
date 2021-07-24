package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import org.apache.rocketmq.client.message.MessageExt;

public interface MessageListener {
    ConsumeStatus consume(List<MessageExt> messages, ConsumeContext context);

    MessageListenerType getListenerType();
}
