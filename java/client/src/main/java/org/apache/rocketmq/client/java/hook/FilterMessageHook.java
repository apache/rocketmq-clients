package org.apache.rocketmq.client.java.hook;

import java.util.List;
import com.google.common.annotations.Beta;
import org.apache.rocketmq.client.java.impl.consumer.ReceiveMessageResult;
import org.apache.rocketmq.client.java.message.MessageViewImpl;

@Beta
public interface FilterMessageHook {

    void filterMessage(MessageInterceptorContext context, ReceiveMessageResult result,
        List<MessageViewImpl> filteredMessages);
}
