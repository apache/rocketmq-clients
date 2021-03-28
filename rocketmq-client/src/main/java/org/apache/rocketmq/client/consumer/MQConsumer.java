package org.apache.rocketmq.client.consumer;

import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RemotingException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public interface MQConsumer {
  /**
   * If consuming failure, message will be send back to the brokers, and delay consuming some time.
   */
  @Deprecated
  void sendMessageBack(final MessageExt msg, final int delayLevel)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

  /**
   * If consuming failure, message will be send back to the broker, and delay consuming some time.
   */
  void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
      throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

  /**
   * Fetch message queues from consumer cache according to the topic
   *
   * @param topic message topic
   * @return queue set
   */
  Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
