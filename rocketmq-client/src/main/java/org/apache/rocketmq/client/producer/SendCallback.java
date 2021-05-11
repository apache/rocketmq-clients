package org.apache.rocketmq.client.producer;

public interface SendCallback {
    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
