package org.apache.rocketmq.client.message;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.client.message.protocol.MessageType;
import org.apache.rocketmq.client.message.protocol.SystemAttribute;
import org.apache.rocketmq.client.misc.MixAll;


@EqualsAndHashCode
@ToString
public class Message {
    final MessageImpl impl;
    @Getter
    private final MessageExt messageExt;

    public Message(String topic, String tag, byte[] body) {
        final SystemAttribute systemAttribute = new SystemAttribute();
        final ConcurrentHashMap<String, String> userAttribute = new ConcurrentHashMap<String, String>();
        systemAttribute.setBornTimeMillis(System.currentTimeMillis());
        systemAttribute.setTag(tag);
        this.impl = new MessageImpl(topic, systemAttribute, userAttribute, body);
        this.messageExt = new MessageExt(impl);
    }

    public Message(MessageImpl impl) {
        this.impl = impl;
        this.messageExt = new MessageExt(impl);
    }

    public void setTopic(String topic) {
        this.impl.setTopic(topic);
    }

    public String getTopic() {
        return this.impl.getTopic();
    }

    public String getTag() {
        return this.impl.getSystemAttribute().getTag();
    }

    public void putUserProperty(final String name, final String value) {
        this.impl.getUserAttribute().put(name, value);
    }

    public String getUserProperty(final String name) {
        return this.impl.getUserAttribute().get(name);
    }

    public void setKeys(String keys) {
        final List<String> keyList = this.impl.getSystemAttribute().getKeys();
        keyList.clear();
        keyList.add(keys.trim());
    }

    public void setKeys(Collection<String> keys) {
        final List<String> keyList = this.impl.getSystemAttribute().getKeys();
        keyList.clear();
        for (String key : keys) {
            keyList.add(key.trim());
        }
    }

    public String getKeys() {
        StringBuilder keys = new StringBuilder();
        for (String key : this.impl.getSystemAttribute().getKeys()) {
            keys.append(key).append(MixAll.MESSAGE_KEY_SEPARATOR);
        }
        return keys.toString().trim();
    }

    public List<String> getKeysList() {
        return this.impl.getSystemAttribute().getKeys();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public void setDelayTimeLevel(int level) {
        this.impl.getSystemAttribute().setDelayLevel(level);
    }

    public void setDeliveryTimestamp(long deliveryTimestamp) {
        final SystemAttribute systemAttribute = this.impl.getSystemAttribute();
        systemAttribute.setDeliveryTimeMillis(deliveryTimestamp);
        systemAttribute.setMessageType(MessageType.DELAY);
    }

    public long getDelayTimeMillis() {
        return this.impl.getSystemAttribute().getDeliveryTimeMillis();
    }

    public void setBody(byte[] body) {
        this.impl.setBody(body);
    }

    public byte[] getBody() {
        return this.impl.getBody();
    }

    public Map<String, String> getUserProperties() {
        return this.impl.getUserAttribute();
    }

    public String getMessageGroup() {
        return this.impl.getSystemAttribute().getMessageGroup();
    }

    public String getMsgId() {
        return this.impl.getSystemAttribute().getMessageId();
    }

    public long getBornTimeMillis() {
        return this.impl.getSystemAttribute().getBornTimeMillis();
    }
}
