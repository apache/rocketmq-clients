package org.apache.rocketmq.client.consumer.listener;

import lombok.Getter;

public abstract class MessageListenerConcurrently implements MessageListener {
    @Getter
    private final MessageListenerType listenerType = MessageListenerType.CONCURRENTLY;
}
