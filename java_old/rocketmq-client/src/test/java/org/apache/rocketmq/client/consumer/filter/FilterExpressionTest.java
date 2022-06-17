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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.rocketmq.client.message.MessageExt;
import org.apache.rocketmq.client.message.MessageImpl;
import org.apache.rocketmq.client.message.MessageImplAccessor;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FilterExpressionTest extends TestBase {

    @Test
    public void testTagSubAllExpression() {
        {
            final FilterExpression filterExpression = new FilterExpression("");
            assertEquals(filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
        {
            final FilterExpression filterExpression = new FilterExpression(null);
            assertEquals(filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
        {
            final FilterExpression filterExpression = new FilterExpression(FilterExpression.TAG_EXPRESSION_SUB_ALL);
            assertEquals(filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
    }

    @Test
    public void testTagSubExpression() {
        {
            String expression = "abc ";
            final FilterExpression filterExpression = new FilterExpression(expression);
            Assert.assertNotEquals(filterExpression.getExpression(), expression);
            assertEquals(filterExpression.getExpression(), expression.trim());
        }
        {
            String expression = "abc";
            final FilterExpression filterExpression = new FilterExpression(expression);
            assertEquals(filterExpression.getExpression(), expression);
        }
    }

    @Test
    public void testIllegalTagSubExpression() {
        {
            String expression = "||";
            final FilterExpression filterExpression = new FilterExpression(expression);
            assertEquals(filterExpression.getExpression(), expression);
        }
    }

    @Test
    public void testAccept() {
        final MessageExt messageExt = fakeMessageExt();
        FilterExpression filterExpression = new FilterExpression();
        assertTrue(filterExpression.accept(messageExt));
        final MessageImpl messageImpl = MessageImplAccessor.getMessageImpl(messageExt);
        messageImpl.getSystemAttribute().setTag("tagA");
        filterExpression = new FilterExpression("tagA");
        assertTrue(filterExpression.accept(messageExt));
        messageImpl.getSystemAttribute().setTag("tagB");
        assertFalse(filterExpression.accept(messageExt));
    }
}
