package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;

@AllArgsConstructor
@Getter
public class PopResult {
    private final String target;
    private final PopStatus popStatus;

    private final String requestId;
    private final long popTimestamp;
    private final long invisibleDuration;

    private final List<MessageExt> msgFoundList;

    // TODO: Fix termId here.
    public long getTermId() {
        return 1;
        //        if (requestId.isEmpty()) {
        //            return 0;
        //        }
        //        return Long.parseLong(requestId, 16);
    }
}
