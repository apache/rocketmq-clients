package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.message.MessageExt;

@Slf4j
@AllArgsConstructor
public class ConsumeConcurrentlyTask implements Runnable {
  final ConsumeConcurrentlyService consumeConcurrentlyService;
  final ProcessQueue processQueue;
  final List<MessageExt> cachedMessages;

  @Override
  public void run() {
    if (processQueue.isDropped()) {
      log.debug(
          "Would not consume message because of the drop of ProcessQueue, mq={}",
          processQueue.getMessageQueue().simpleName());
      return;
    }
    ConsumeConcurrentlyContext context =
        new ConsumeConcurrentlyContext(processQueue.getMessageQueue());
    ConsumeConcurrentlyStatus status;
    try {
      status =
          consumeConcurrentlyService
              .getMessageListenerConcurrently()
              .consumeMessage(cachedMessages, context);
    } catch (Throwable t) {
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
      log.error("Business callback raised an exception while consuming message.", t);
    }

    processQueue.removeCachedMessages(cachedMessages);

    for (MessageExt messageExt : cachedMessages) {
      switch (status) {
        case CONSUME_SUCCESS:
          try {
            processQueue.getConsumerImpl().consumeSuccessNum.incrementAndGet();
            processQueue.ackMessage(messageExt);
          } catch (Throwable t) {
            log.warn(
                "Failed to ACK message, mq={}, msgId={}",
                processQueue.getMessageQueue().simpleName(),
                messageExt.getMsgId(),
                t);
          }
          break;
        case RECONSUME_LATER:
        default:
          try {
            processQueue.getConsumerImpl().consumeFailureNum.incrementAndGet();
            processQueue.negativeAckMessage(messageExt);
          } catch (Throwable t) {
            log.warn(
                "Failed to NACK message, mq={}, msgId={}",
                processQueue.getMessageQueue().simpleName(),
                messageExt.getMsgId(),
                t);
          }
      }
    }
  }
}
