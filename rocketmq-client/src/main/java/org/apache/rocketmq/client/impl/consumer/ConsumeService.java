package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public interface ConsumeService {
  void start() throws MQClientException;

  void shutdown() throws MQClientException;

  void dispatch(ProcessQueue processQueue);

  void submitConsumeTask(
      final List<MessageExt> messageExtList,
      final ProcessQueue processQueue,
      final MessageQueue messageQueue);

  boolean hasConsumeRateLimiter(String topic);

  RateLimiter rateLimiter(String topic);
}
