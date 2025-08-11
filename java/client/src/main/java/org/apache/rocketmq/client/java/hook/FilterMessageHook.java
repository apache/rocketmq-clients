package org.apache.rocketmq.client.java.hook;

import com.google.common.annotations.Beta;
import org.apache.rocketmq.client.java.impl.consumer.ReceiveMessageResult;

@Beta
public interface FilterMessageHook {

    void filterMessage(MessageInterceptorContext context, ReceiveMessageResult result);
}
