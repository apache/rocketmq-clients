package org.apache.rocketmq.client.message;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;


@EqualsAndHashCode
@ToString
public class Message {
    final MessageImpl impl;
    @Getter
    final MessageExt messageExt;

    public Message(String topic, String tags, byte[] body) {
        this.impl = new MessageImpl(topic);
        this.impl.setBody(body);
        this.impl.getSystemAttribute().setTag(tags);
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

    public void setTags(String tags) {
        String[] split = tags.split("\\|\\|");
        for (int i = 0; i < split.length; i++) {
            split[i] = split[i].trim();
        }
        this.impl.getSystemAttribute().setTag(StringUtils.join(split, "||"));
    }

    public String getTags() {
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
            keys.append(key).append(MessageConst.KEY_SEPARATOR);
        }
        return keys.toString().trim();
    }

    public int getDelayTimeLevel() {
        return this.impl.getSystemAttribute().getDelayLevel();
    }

    public void setDelayTimeLevel(int level) {
        this.impl.getSystemAttribute().setDelayLevel(level);
    }

    public void setDeliveryTimestamp(long deliveryTimestamp) {
        this.impl.getSystemAttribute().setDeliveryTimestamp(deliveryTimestamp);
    }

    public long getDeliveryTimestamp() {
        return this.impl.getSystemAttribute().getDeliveryTimestamp();
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
}
