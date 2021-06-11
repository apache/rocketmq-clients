package org.apache.rocketmq.client.consumer;

public interface PullCallback {
    void onSuccess(final PullResult pullResult);

    void onException(final Throwable e);
}
