package org.apache.rocketmq.client.message;

public class MessageAccessor {
    private MessageAccessor() {
    }

    public static MessageImpl getMessageImpl(MessageExt messageExt) {
        return messageExt.impl;
    }
}
