package org.apache.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.filter.ExpressionType;

@Getter
public class MessageSelector {
  private final ExpressionType expressionType;
  private final String expression;
  private final Map<String, String> properties;

  public MessageSelector(ExpressionType expressionType, String expression) {
    this.expressionType = expressionType;
    this.expression = expression;
    this.properties = new HashMap<String, String>();
  }

  /**
   * Use SLQ92 to select message.
   *
   * @param sql if null or empty, will be treated as select all message.
   */
  public static MessageSelector bySql(String sql) {
    return new MessageSelector(ExpressionType.SQL92, sql);
  }

  /**
   * Use tag to select message.
   *
   * @param tag if null or empty or "*", will be treated as select all message.
   */
  public static MessageSelector byTag(String tag) {
    return new MessageSelector(ExpressionType.TAG, tag);
  }

  public void putProperty(String key, String value) {
    if (StringUtils.isBlank(key)) {
      throw new IllegalArgumentException("Key can not be null or empty string!");
    }
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException("Value can not be null or empty string!");
    }
    this.properties.put(key, value);
  }
}
