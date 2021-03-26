package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.proto.ConsumeData;

public interface ConsumerObserver {
  ConsumeData prepareHeartbeatData();
}
