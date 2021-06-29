package org.apache.rocketmq.client.consumer.filter;

import lombok.Getter;

@Getter
public class FilterExpression {
    public static final String TAG_EXPRESSION_SUB_ALL = "*";
    private static final String TAG_EXPRESSION_SPLIT_PATTERN = "\\|\\|";

    private final String expression;
    private final ExpressionType expressionType;
    private final long version;

    public FilterExpression() {
        this(TAG_EXPRESSION_SUB_ALL);
    }

    public FilterExpression(String expression) {
        this(expression, ExpressionType.TAG);
    }

    public FilterExpression(String expression, ExpressionType expressionType) {
        if (ExpressionType.TAG == expressionType) {
            if (null == expression || expression.isEmpty()) {
                this.expression = TAG_EXPRESSION_SUB_ALL;
            } else {
                this.expression = expression.trim();
            }
        } else {
            this.expression = expression;
        }
        this.expressionType = expressionType;
        this.version = System.currentTimeMillis();
    }

    public boolean verifyExpression() {
        if (ExpressionType.TAG == expressionType) {
            final String[] split = expression.split(TAG_EXPRESSION_SPLIT_PATTERN);
            return split.length > 0;
        }
        return true;
    }
}
