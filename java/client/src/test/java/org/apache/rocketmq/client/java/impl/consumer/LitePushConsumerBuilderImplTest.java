package org.apache.rocketmq.client.java.impl.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LitePushConsumerBuilder;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class LitePushConsumerBuilderImplTest extends TestBase {

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithBlank() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic("  ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBindTopicWithEmpty() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.bindTopic("");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSubscriptionExpressions() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        Map<String, FilterExpression> subscriptionExpressions = new HashMap<>();
        subscriptionExpressions.put("topic", new FilterExpression());
        builder.setSubscriptionExpressions(subscriptionExpressions);
    }

    @Test(expected = NullPointerException.class)
    public void testSetClientConfigurationWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setClientConfiguration(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetConsumerGroupWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumerGroup(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetMessageListenerWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMessageListener(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageCount() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMaxCacheMessageCount(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeMaxCacheMessageSizeInBytes() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setMaxCacheMessageSizeInBytes(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNegativeConsumptionThreadCount() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumptionThreadCount(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutClientConfiguration() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutConsumerGroup() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutMessageListener() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .bindTopic("test-topic")
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testBuildWithoutBindTopic() throws ClientException {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        ClientConfiguration clientConfiguration =
            ClientConfiguration.newBuilder().setEndpoints(FAKE_ENDPOINTS).build();
        builder.setClientConfiguration(clientConfiguration)
            .setConsumerGroup(FAKE_CONSUMER_GROUP_0)
            .setMessageListener(messageView -> ConsumeResult.SUCCESS)
            .build();
    }

    @Test
    public void testBindTopic() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        String topic = "test-topic";
        builder.bindTopic(topic);

        assertTrue(builder.subscriptionExpressions.containsKey(topic));
        assertEquals(new FilterExpression(), builder.subscriptionExpressions.get(topic));
    }

    @Test(expected = NullPointerException.class)
    public void testSetInvisibleDurationWithNull() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        builder.setInvisibleDuration(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvisibleDurationLessThanMinimum() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        Duration lessThanMin = Duration.ofSeconds(30);
        builder.setInvisibleDuration(lessThanMin);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvisibleDurationGreaterThanMaximum() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();
        Duration greaterThanMax = Duration.ofHours(2);
        builder.setInvisibleDuration(greaterThanMax);
    }

    @Test
    public void testSetInvisibleDurationWithinRange() {
        final LitePushConsumerBuilderImpl builder = new LitePushConsumerBuilderImpl();

        // Test boundary value - minimum
        Duration minDuration = LitePushConsumerBuilderImpl.MIN_INVISIBLE_DURATION;
        LitePushConsumerBuilder result1 = builder.setInvisibleDuration(minDuration);
        assertEquals(minDuration, builder.invisibleDuration);
        assertEquals(builder, result1);

        // Test boundary value - maximum
        Duration maxDuration = LitePushConsumerBuilderImpl.MAX_INVISIBLE_DURATION;
        LitePushConsumerBuilder result2 = builder.setInvisibleDuration(maxDuration);
        assertEquals(maxDuration, builder.invisibleDuration);
        assertEquals(builder, result2);

        // Test intermediate value
        Duration midDuration = Duration.ofMinutes(5);
        LitePushConsumerBuilder result3 = builder.setInvisibleDuration(midDuration);
        assertEquals(midDuration, builder.invisibleDuration);
        assertEquals(builder, result3);
    }
}
