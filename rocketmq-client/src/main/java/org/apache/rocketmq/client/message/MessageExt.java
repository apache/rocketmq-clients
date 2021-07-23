package org.apache.rocketmq.client.message;


import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.remoting.Endpoints;

@EqualsAndHashCode
@ToString
public class MessageExt {
    protected final MessageImpl impl;

    public MessageExt(MessageImpl impl) {
        this.impl = impl;
    }

    public byte[] getBody() {
        return this.impl.getBody();
    }

    public String getTopic() {
        return this.impl.getTopic();
    }

    public String getTags() {
        return this.impl.getSystemAttribute().getTag();
    }

    public String getKeys() {
        StringBuilder keys = new StringBuilder();
        for (String key : this.impl.getSystemAttribute().getKeys()) {
            keys.append(key).append(MessageConst.KEY_SEPARATOR);
        }
        return keys.toString().trim();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public long getDeliveryTimestamp() {
        return this.impl.getSystemAttribute().getDeliveryTimestamp();
    }

    public int getQueueId() {
        return this.impl.getSystemAttribute().getPartitionId();
    }

    public long getBornTimestamp() {
        return this.impl.getSystemAttribute().getBornTimestamp();
    }

    public String getBornHost() {
        return this.impl.getSystemAttribute().getBornHost();
    }

    public boolean isExpired(long tolerance) {
        throw new UnsupportedOperationException();
    }

    public long getQueueOffset() {
        return this.impl.getSystemAttribute().getPartitionOffset();
    }

    public String getMsgId() {
        return this.impl.getSystemAttribute().getMessageId();
    }

    public long getDecodedTimestamp() {
        return this.impl.getSystemAttribute().getDecodedTimestamp();
    }

    public int getReconsumeTimes() {
        return this.impl.getSystemAttribute().getDeliveryCount();
    }

    public String getReceiptHandle() {
        return this.impl.getSystemAttribute().getReceiptHandle();
    }

    // TODO: hide targetEndpoint here.
    public Endpoints getAckEndpoints() {
        return this.impl.getSystemAttribute().getAckEndpoints();
    }

    public String getTraceContext() {
        return this.impl.getSystemAttribute().getTraceContext();
    }

    public Map<String, String> getUserProperties() {
        return this.impl.getUserAttribute();
    }

    public MessageType getMsgType() {
        switch (this.impl.getSystemAttribute().getMessageType()) {
            case FIFO:
                return MessageType.FIFO;
            case DELAY:
                return MessageType.DELAY;
            case TRANSACTION:
                return MessageType.TRANSACTION;
            default:
                return MessageType.NORMAL;
        }
    }

    public String getTransactionId() {
        return this.impl.getSystemAttribute().getTransactionId();
    }
}
