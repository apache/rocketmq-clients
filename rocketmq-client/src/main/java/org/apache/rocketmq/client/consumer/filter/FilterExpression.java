/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.consumer.filter;

import org.apache.rocketmq.client.message.MessageExt;

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

    public String getExpression() {
        return this.expression;
    }

    public ExpressionType getExpressionType() {
        return this.expressionType;
    }

    public long getVersion() {
        return this.version;
    }
}
