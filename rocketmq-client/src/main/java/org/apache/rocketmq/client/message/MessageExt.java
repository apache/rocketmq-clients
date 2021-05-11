package org.apache.rocketmq.client.message;

import java.net.SocketAddress;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class MessageExt extends Message {
    private String brokerName;

    private int queueId;

    private int storeSize;

    private long queueOffset;
    private int sysFlag;
    private long bornTimestamp;
    private SocketAddress bornHost;

    private long storeTimestamp;
    private SocketAddress storeHost;
    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes;

    private long preparedTransactionOffset;

    private long decodedTimestamp;
    private long expiredTimestamp;

    private MessageVersion version;

    public boolean isExpired(long tolerance) {
        return System.currentTimeMillis() + tolerance > expiredTimestamp;
    }
}
