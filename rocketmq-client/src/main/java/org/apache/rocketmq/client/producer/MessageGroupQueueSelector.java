package org.apache.rocketmq.client.producer;

import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageQueue;

@AllArgsConstructor
public class MessageGroupQueueSelector implements MessageQueueSelector {
    private final String messageGroup;

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        final int hashCode = messageGroup.hashCode();
        return mqs.get(Math.abs(hashCode % mqs.size()));
    }
}
