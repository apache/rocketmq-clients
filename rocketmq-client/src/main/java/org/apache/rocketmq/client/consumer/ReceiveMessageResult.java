package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.remoting.Endpoints;

@AllArgsConstructor
@Getter
public class ReceiveMessageResult {
    private final Endpoints endpoints;
    private final ReceiveStatus receiveStatus;

    private final long receiveTimestamp;
    private final long invisibleDuration;

    private final List<MessageExt> messagesFound;
}
