package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;

@Getter
public class PullResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private final List<MessageExt> msgFoundList;

    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                      List<MessageExt> msgFoundList) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }
}
