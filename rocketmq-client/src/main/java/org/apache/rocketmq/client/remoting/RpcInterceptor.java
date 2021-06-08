package org.apache.rocketmq.client.remoting;

import org.apache.rocketmq.client.message.Message;

public interface RpcInterceptor {
    void doBeforeRequest(Message request);

    void doAfterResponse(Message response);
}
