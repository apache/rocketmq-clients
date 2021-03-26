package org.apache.rocketmq.client.producer;

import java.util.List;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageQueue;

public interface MessageQueueSelector {
  MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
