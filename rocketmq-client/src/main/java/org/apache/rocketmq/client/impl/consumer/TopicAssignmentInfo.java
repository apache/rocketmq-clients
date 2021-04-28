package org.apache.rocketmq.client.impl.consumer;

import java.util.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.proto.MessageQueueAssignment;

@Slf4j
@ToString
@EqualsAndHashCode
public class TopicAssignmentInfo {
  private static final ThreadLocal<Integer> queryBrokerIndex = new ThreadLocal<Integer>();

  @Getter private final List<Assignment> assignmentList;

  static {
    queryBrokerIndex.set(Math.abs(new Random().nextInt()));
  }

  public static int getNextQueryBrokerIndex() {
    Integer index = queryBrokerIndex.get();
    if (null == index) {
      index = -1;
    }
    index += 1;
    index = Math.abs(index);
    queryBrokerIndex.set(index);
    return index;
  }

  public TopicAssignmentInfo(List<MessageQueueAssignment> messageQueueAssignmentList) {
    this.assignmentList = new ArrayList<Assignment>();

    for (MessageQueueAssignment item : messageQueueAssignmentList) {
      MessageQueue messageQueue =
          new MessageQueue(
              item.getMessageQueue().getTopic(),
              item.getMessageQueue().getBrokerName(),
              item.getMessageQueue().getQueueId());

      MessageRequestMode mode = MessageRequestMode.POP;
      switch (item.getMode()) {
        case PULL:
          mode = MessageRequestMode.PULL;
          break;
        case POP:
          mode = MessageRequestMode.POP;
          break;
        default:
          log.warn("Unknown message request mode={}, default to pop.", item.getMode());
      }
      Map<String, String> attachments = new HashMap<String, String>(item.getAttachmentsMap());
      assignmentList.add(new Assignment(messageQueue, mode, attachments));
    }
  }
}
