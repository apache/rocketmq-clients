package org.apache.rocketmq.client.consumer;

public interface PullCallback {
    void onSuccess(final PullMessageResult pullMessageResult);

    void onException(final Throwable e);
}
