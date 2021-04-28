package org.apache.rocketmq.client.message;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.Getter;

@Data
public class Message {
  private String topic;
  private int flag;
  private byte[] body;

  @Getter private final Map<String, String> properties;

  public Message() {
    this.properties = new HashMap<String, String>();
  }

  public Message(
      String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
    this.topic = topic;
    this.flag = flag;
    this.body = body;

    this.properties = new HashMap<String, String>();

    if (tags != null && tags.length() > 0) {
      this.setTags(tags);
    }

    if (keys != null && keys.length() > 0) {
      this.setKeys(keys);
    }

    this.setWaitStoreMsgOK(waitStoreMsgOK);
  }

  public Message(String topic, String tags, byte[] body) {
    this(topic, tags, "", 0, body, true);
  }

  public void putProperty(final String name, final String value) {
    this.properties.put(name, value);
  }

  public String getProperty(final String name) {
    return this.properties.get(name);
  }

  public void setTags(String tags) {
    this.putProperty(MessageConst.PROPERTY_TAGS, tags);
  }

  public void setKeys(String keys) {
    this.putProperty(MessageConst.PROPERTY_KEYS, keys);
  }

  public void setKeys(Collection<String> keys) {
    StringBuilder sb = new StringBuilder();
    for (String k : keys) {
      sb.append(k);
      sb.append(MessageConst.KEY_SEPARATOR);
    }

    this.setKeys(sb.toString().trim());
  }

  public int getDelayTimeLevel() {
    String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
    if (null != t) {
      return Integer.parseInt(t);
    }
    return 0;
  }

  public boolean isWaitStoreMsgOK() {
    String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
    if (null == result) {
      return true;
    }
    return Boolean.parseBoolean(result);
  }

  public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
    this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
  }
}
