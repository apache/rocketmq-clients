package org.apache.rocketmq.client.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.client.constant.TopicPrefix;

@AllArgsConstructor
public class MessageBatch extends Message implements Iterable<Message> {
  private final List<Message> messages;

  public byte[] encode() {
    return MessageCodec.encodeMessages(messages);
  }

  @Override
  public Iterator<Message> iterator() {
    return messages.iterator();
  }

  public static MessageBatch generateFromList(Collection<Message> messages) {
    assert messages != null;
    assert messages.size() > 0;
    List<Message> messageList = new ArrayList<Message>(messages.size());
    Message first = null;
    for (Message message : messages) {
      if (message.getDelayTimeLevel() > 0) {
        throw new UnsupportedOperationException("TimeDelayLevel in not supported for batching");
      }
      if (message.getTopic().startsWith(TopicPrefix.RETRY_TOPIC_PREFIX.getTopicPrefix())) {
        throw new UnsupportedOperationException("Retry topic is not supported for batching");
      }
      if (first == null) {
        first = message;
      } else {
        if (!first.getTopic().equals(message.getTopic())) {
          throw new UnsupportedOperationException(
              "The topic of the messages in one batch should be the " + "same");
        }
        if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
          throw new UnsupportedOperationException(
              "The waitStoreMsgOK of the messages in one batch should " + "the same");
        }
      }
      messageList.add(message);
    }
    MessageBatch messageBatch = new MessageBatch(messageList);

    messageBatch.setTopic(first.getTopic());
    messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
    return messageBatch;
  }
}
