package org.apache.rocketmq.client.message;


import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class MessageExt {
    private final MessageImpl impl;

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
        throw new UnsupportedOperationException();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public int getQueueId() {
        return this.impl.getSystemAttribute().getPartitionId();
    }

    public long getBornTimestamp() {
        return this.impl.getSystemAttribute().getBornTimestamp();
    }

    public long getBornHost() {
        return this.impl.getSystemAttribute().getBornTimestamp();
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
}
