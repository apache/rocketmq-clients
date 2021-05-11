package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import org.apache.rocketmq.client.message.MessageExt;

public interface MessageListenerConcurrently {

    ConsumeConcurrentlyStatus consumeMessage(
            final List<MessageExt> messages, final ConsumeConcurrentlyContext context);
}
