package org.apache.rocketmq.client.consumer.filter;

import lombok.Getter;
import org.apache.rocketmq.client.message.MessageExt;

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

    // TODO: if client connect to broker bypass the proxy, the message received may not be filter totally.
    public boolean accept(MessageExt messageExt) {
        final String[] split = expression.split(TAG_EXPRESSION_SPLIT_PATTERN);
        final String messageTag = messageExt.getTag();
        for (String tag : split) {
            if (tag.equals(messageTag)) {
                return true;
            }
        }
        return false;
    }
}
