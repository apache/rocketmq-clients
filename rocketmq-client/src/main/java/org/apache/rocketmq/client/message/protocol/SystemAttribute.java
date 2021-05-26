package org.apache.rocketmq.client.message.protocol;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class SystemAttribute {
    private String tag;
    private final List<String> keys;
    private String messageId;
    private Digest digest;
    private Encoding bodyEncoding;
    private MessageType messageType;
    private TransactionPhase transactionPhase;
    private long bornTimestamp;
    private String bornHost;
    private long deliveryTimestamp;
    private int delayLevel;
    private String receiptHandle;
    private int partitionId;
    private long partitionOffset;
    private long invisiblePeriod;
    private int deliveryCount;
    private String publisherGroup;
    private String traceContext;

    private long decodedTimestamp;

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
