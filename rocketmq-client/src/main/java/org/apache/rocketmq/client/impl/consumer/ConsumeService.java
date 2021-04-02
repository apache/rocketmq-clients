package org.apache.rocketmq.client.impl.consumer;

import com.google.common.util.concurrent.RateLimiter;
import java.util.List;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageQueue;

public interface ConsumeService {
  void start();

  void shutdown();

  void submitConsumeTask(
      final List<MessageExt> messageExtList,
      final ProcessQueue processQueue,
      final MessageQueue messageQueue);

  boolean hasConsumeRateLimiter(String topic);

  RateLimiter rateLimiter(String topic);
}
