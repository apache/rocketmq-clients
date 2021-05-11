package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import org.apache.rocketmq.client.message.MessageExt;

public interface MessageListenerOrderly {
    ConsumeOrderlyStatus consumeMessage(
            final List<MessageExt> messages, final ConsumeOrderlyContext context);
}
