package org.apache.rocketmq.client.consumer.filter;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FilterExpressionTest {

    @Test
    public void testTagSubAllExpression() {
        {
            final FilterExpression filterExpression = new FilterExpression("");
            Assert.assertEquals(
                    filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            Assert.assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
        {
            final FilterExpression filterExpression = new FilterExpression(null);
            Assert.assertEquals(
                    filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            Assert.assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
        {
            final FilterExpression filterExpression =
                    new FilterExpression(FilterExpression.TAG_EXPRESSION_SUB_ALL);
            Assert.assertEquals(
                    filterExpression.getExpression(), FilterExpression.TAG_EXPRESSION_SUB_ALL);
            Assert.assertEquals(filterExpression.getExpressionType(), ExpressionType.TAG);
        }
    }

    @Test
    public void testTagSubExpression() {
        {
            String expression = "abc ";
            final FilterExpression filterExpression = new FilterExpression(expression);
            Assert.assertNotEquals(filterExpression.getExpression(), expression);
            Assert.assertEquals(filterExpression.getExpression(), expression.trim());
        }
        {
            String expression = "abc";
            final FilterExpression filterExpression = new FilterExpression(expression);
            Assert.assertEquals(filterExpression.getExpression(), expression);
        }
    }

    @Test
    public void testIllegalTagSubExpression() {
        {
            String expression = "||";
            final FilterExpression filterExpression = new FilterExpression(expression);
            Assert.assertEquals(filterExpression.getExpression(), expression);
        }
    }
}
