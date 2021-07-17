package org.apache.rocketmq.client.message;

public enum MessageHookPoint {
    PRE_SEND_MESSAGE,
    POST_SEND_MESSAGE,

    PRE_PULL_MESSAGE,
    POST_PULL_MESSAGE,

    PRE_MESSAGE_CONSUMPTION,
    POST_MESSAGE_CONSUMPTION,

    PRE_END_MESSAGE,
    POST_END_MESSAGE;

    public enum PointStatus {
        UNSET,
        OK,
        ERROR;
    }
}
