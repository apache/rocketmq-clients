package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;

@Getter
public class PullMessageResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private final List<MessageExt> messagesFound;

    public PullMessageResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                             List<MessageExt> messagesFound) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messagesFound = messagesFound;
    }
}
