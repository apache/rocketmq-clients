package org.apache.rocketmq.client.message.protocol;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.client.remoting.RpcTarget;

@Data
public class SystemAttribute {
    private String tag;
    private final List<String> keys;
    private String messageId;
    private Digest digest;
    private Encoding bodyEncoding;
    private MessageType messageType;
    private long bornTimestamp;
    private String bornHost;
    private long deliveryTimestamp = 0;
    private int delayLevel = 0;
    private String receiptHandle;
    private int partitionId;
    private long partitionOffset;
    private long invisiblePeriod;
    private int deliveryCount;
    private String producerGroup;
    private String transactionId;
    private String traceContext;
    // Would set after receiving the message.
    private long decodedTimestamp;
    private RpcTarget ackTarget;

    public SystemAttribute() {
        this.keys = new ArrayList<String>();
    }

    public void setKeys(List<String> keys) {
        this.keys.clear();
        this.keys.addAll(keys);
    }

    public void setKey(String key) {
        this.keys.clear();
        this.keys.add(key);
    }
}
