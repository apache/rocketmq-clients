package org.apache.rocketmq.client.message;


public interface MessageInterceptor {
    void intercept(MessageHookPoint hookPoint, MessageExt messageExt, MessageInterceptorContext context);
}
