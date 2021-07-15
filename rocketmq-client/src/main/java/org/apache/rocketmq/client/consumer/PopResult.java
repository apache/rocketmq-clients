package org.apache.rocketmq.client.consumer;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.remoting.RpcTarget;

@AllArgsConstructor
@Getter
public class PopResult {
    private final RpcTarget target;
    private final PopStatus popStatus;

    private final long popTimestamp;
    private final long invisibleDuration;

    private final List<MessageExt> msgFoundList;
}
