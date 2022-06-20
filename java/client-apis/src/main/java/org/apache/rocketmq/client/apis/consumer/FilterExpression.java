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

package org.apache.rocketmq.client.apis.consumer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

/**
 * Filter expression is an efficient way to filter message for {@link SimpleConsumer} and {@link PushConsumer}.
 * The consumer who applied the filter expression only can receive the filtered messages.
 */
public class FilterExpression {
    private static final String TAG_EXPRESSION_SUB_ALL = "*";
    public static final FilterExpression SUB_ALL = new FilterExpression(TAG_EXPRESSION_SUB_ALL);

    private final String expression;
    private final FilterExpressionType filterExpressionType;

    public FilterExpression(String expression, FilterExpressionType filterExpressionType) {
        this.expression = checkNotNull(expression, "expression should not be null");
        this.filterExpressionType = checkNotNull(filterExpressionType, "filterExpressionType should not be null");
    }

    /**
     * If the {@link FilterExpressionType} is not specified, the type is {@link FilterExpressionType#TAG}.
     *
     * @param expression tag filter expression.
     */
    public FilterExpression(String expression) {
        this(expression, FilterExpressionType.TAG);
    }

    /**
     * Default constructor, which means that messages are not filtered.
     */
    public FilterExpression() {
        this(TAG_EXPRESSION_SUB_ALL);
    }

    public String getExpression() {
        return expression;
    }

    public FilterExpressionType getFilterExpressionType() {
        return filterExpressionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilterExpression that = (FilterExpression) o;
        return expression.equals(that.expression) && filterExpressionType == that.filterExpressionType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expression, filterExpressionType);
    }
}
