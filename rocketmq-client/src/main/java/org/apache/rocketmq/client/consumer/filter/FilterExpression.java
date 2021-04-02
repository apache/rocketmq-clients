package org.apache.rocketmq.client.consumer.filter;

public class FilterExpression {
  private String expression;
  private ExpressionType expressionType;
  private long version;

  public FilterExpression(String expression, ExpressionType expressionType) {
    this.expression = expression;
    this.expressionType = expressionType;
    this.version = System.currentTimeMillis();
  }

  public FilterExpression(String expression) {
    this(expression, ExpressionType.TAG);
  }
}
