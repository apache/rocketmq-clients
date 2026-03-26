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

package org.apache.rocketmq.client.java.impl.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.NotifyUnsubscribeLiteCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import com.google.common.util.concurrent.ListenableFuture;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.OffsetOption;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.apache.rocketmq.client.java.impl.ClientManager;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LiteSubscriptionManagerTest {

    @Mock
    private ConsumerImpl consumerImpl;

    @Mock
    private ClientConfiguration clientConfiguration;

    @Mock
    private ClientManager clientManager;

    @Mock
    private Endpoints endpoints;

    @Mock
    private RpcFuture<SyncLiteSubscriptionRequest, SyncLiteSubscriptionResponse> rpcFuture;

    private Resource bindTopic;
    private Resource group;

    @Before
    public void setUp() throws Exception {
        bindTopic = new Resource("test-namespace", "test-bind-topic");

        group = new Resource("test-namespace", "test-consumer-group");

        when(consumerImpl.getClientConfiguration()).thenReturn(clientConfiguration);
        when(clientConfiguration.getRequestTimeout()).thenReturn(Duration.ofSeconds(30));
        when(consumerImpl.getClientManager()).thenReturn(clientManager);
        when(consumerImpl.getEndpoints()).thenReturn(endpoints);

        // Mock successful response
        SyncLiteSubscriptionResponse successResponse = SyncLiteSubscriptionResponse.newBuilder()
            .setStatus(Status.newBuilder().setCode(Code.OK).build())
            .build();
        lenient().when(rpcFuture.get()).thenReturn(successResponse);
        lenient().when(clientManager.syncLiteSubscription(any(), any(), any())).thenReturn(rpcFuture);
    }

    @Test
    public void testConstructor() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        assertThat(liteSubscriptionManager).isNotNull();
        assertThat(liteSubscriptionManager.getBindTopicName()).isEqualTo("test-bind-topic");
        assertThat(liteSubscriptionManager.getConsumerGroupName()).isEqualTo("test-consumer-group");
    }

    @Test
    public void testGetBindTopicName() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        assertThat(liteSubscriptionManager.getBindTopicName()).isEqualTo("test-bind-topic");
    }

    @Test
    public void testGetConsumerGroupName() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        assertThat(liteSubscriptionManager.getConsumerGroupName()).isEqualTo("test-consumer-group");
    }

    @Test
    public void testGetLiteTopicSet() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        final Set<String> initialTopicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(initialTopicSet).isEmpty();
    }

    @Test
    public void testSyncWithSubscription() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .setLiteSubscriptionQuota(10)
                .setMaxLiteTopicSize(128)
                .build())
            .build();

        liteSubscriptionManager.sync(settings);

        // Verify quota and size were updated
        assertThat(liteSubscriptionManager.liteSubscriptionQuota).isEqualTo(10);
        assertThat(liteSubscriptionManager.maxLiteTopicSize).isEqualTo(128);
    }

    @Test
    public void testSyncWithoutSubscription() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        final Settings settings = Settings.newBuilder()
            .build();

        liteSubscriptionManager.sync(settings);

        // Verify defaults remained
        assertThat(liteSubscriptionManager.liteSubscriptionQuota).isEqualTo(0);
        assertThat(liteSubscriptionManager.maxLiteTopicSize).isEqualTo(64);
    }

    @Test
    public void testSubscribeLiteSuccess() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).contains("test-topic");
        verify(clientManager, times(1)).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testSubscribeLiteAlreadySubscribed() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Subscribe twice
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).contains("test-topic");
        assertThat(topicSet).hasSize(1);
        // RPC should only be called once
        verify(clientManager, times(1)).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testSubscribeLiteWithOffsetOption() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        OffsetOption offsetOption = OffsetOption.ofOffset(100L);
        liteSubscriptionManager.subscribeLite("test-topic", offsetOption);

        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).contains("test-topic");
        verify(clientManager, times(1)).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testSubscribeLiteThrowsClientException() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Mock failing subscription
        ClientException expectedException = new ClientException("Subscription failed");
        when(clientManager.syncLiteSubscription(any(), any(), any())).thenReturn(rpcFuture);
        doThrow(expectedException).when(consumerImpl).handleClientFuture(any(ListenableFuture.class));

        assertThatThrownBy(() -> liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET))
            .isInstanceOf(ClientException.class)
            .hasMessage("Subscription failed");

        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).doesNotContain("test-topic");
    }

    @Test
    public void testSubscribeLiteBlankTopic() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        assertThatThrownBy(() -> liteSubscriptionManager.subscribeLite("", OffsetOption.LAST_OFFSET))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("liteTopic is blank");

        assertThatThrownBy(() -> liteSubscriptionManager.subscribeLite("   ", OffsetOption.LAST_OFFSET))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("liteTopic is blank");
    }

    @Test
    public void testSubscribeLiteTopicTooLong() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        String tooLongTopic = StringUtils.repeat('a', 65);

        assertThatThrownBy(() -> liteSubscriptionManager.subscribeLite(tooLongTopic, OffsetOption.LAST_OFFSET))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("liteTopic length exceeded max length");
    }

    @Test
    public void testSubscribeLiteQuotaExceeded() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to 0
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(0).build())
            .build();
        liteSubscriptionManager.sync(settings);

        assertThatThrownBy(() -> liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET))
            .isInstanceOf(LiteSubscriptionQuotaExceededException.class)
            .hasMessageContaining("Lite subscription quota exceeded");
    }

    @Test
    public void testUnsubscribeLiteSuccess() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // First subscribe to add topic
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        // Then unsubscribe
        liteSubscriptionManager.unsubscribeLite("test-topic");

        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).doesNotContain("test-topic");
        verify(clientManager, times(2)).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testUnsubscribeLiteNotSubscribed() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Unsubscribe from topic that was never subscribed
        liteSubscriptionManager.unsubscribeLite("non-existent-topic");

        // Should not throw exception and should not affect anything
        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).isEmpty();

        // Verify no RPC calls since topic wasn't actually subscribed
        verify(clientManager, never()).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testUnsubscribeLiteThrowsClientException() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // First subscribe to add topic
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        // Then mock failing unsubscribe
        ClientException expectedException = new ClientException("Unsubscription failed");
        when(clientManager.syncLiteSubscription(any(), any(), any())).thenReturn(rpcFuture);
        doThrow(expectedException).when(consumerImpl).handleClientFuture(any(ListenableFuture.class));

        assertThatThrownBy(() -> liteSubscriptionManager.unsubscribeLite("test-topic"))
            .isInstanceOf(ClientException.class)
            .hasMessage("Unsubscription failed");

        // Topic should still be in set because operation failed
        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).contains("test-topic");
    }

    @Test
    public void testSyncAllLiteSubscription() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Add some topics
        liteSubscriptionManager.subscribeLite("topic1", OffsetOption.LAST_OFFSET);
        liteSubscriptionManager.subscribeLite("topic2", OffsetOption.LAST_OFFSET);

        // Call syncAll
        liteSubscriptionManager.syncAllLiteSubscription();

        // Verify RPC was called with all topics
        verify(clientManager, times(3)).syncLiteSubscription(
            eq(endpoints),
            any(SyncLiteSubscriptionRequest.class),
            eq(Duration.ofSeconds(30))
        );
    }

    @Test
    public void testSyncAllLiteSubscriptionHandlesException() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Mock exception during sync
        RuntimeException runtimeException = new RuntimeException("RPC failed");
        when(clientManager.syncLiteSubscription(any(), any(), any())).thenReturn(rpcFuture);
        doThrow(runtimeException).when(consumerImpl).handleClientFuture(any(ListenableFuture.class));

        // This should not throw exception due to try-catch in implementation
        liteSubscriptionManager.syncAllLiteSubscription();

        // But should still handle the exception internally
        verify(consumerImpl, times(1)).handleClientFuture(any(ListenableFuture.class));
    }

    @Test
    public void testOnNotifyUnsubscribeLiteCommand() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // First subscribe to add topic
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        // Create notify command
        NotifyUnsubscribeLiteCommand command = NotifyUnsubscribeLiteCommand.newBuilder()
            .setLiteTopic("test-topic")
            .build();

        // Process notification
        liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);

        // Verify topic was removed from set
        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).doesNotContain("test-topic");
    }

    @Test
    public void testOnNotifyUnsubscribeLiteCommandWithEmptyTopic() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // First subscribe to add topic
        liteSubscriptionManager.subscribeLite("test-topic", OffsetOption.LAST_OFFSET);

        // Create notify command with empty topic
        NotifyUnsubscribeLiteCommand command = NotifyUnsubscribeLiteCommand.newBuilder()
            .setLiteTopic("")
            .build();

        // Process notification - should not remove anything
        liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);

        // Topic should still be in set
        final Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).contains("test-topic");
    }

    @Test
    public void testValidateLiteTopicValid() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // These should not throw exceptions
        liteSubscriptionManager.validateLiteTopic("valid-topic", 128);
        liteSubscriptionManager.validateLiteTopic("topic-with-dashes_and_underscores", 128);
        liteSubscriptionManager.validateLiteTopic("topic.with.dots", 128);
    }

    @Test
    public void testValidateLiteTopicBlank() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        assertThatThrownBy(() -> liteSubscriptionManager.validateLiteTopic("", 128))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("liteTopic is blank");

        assertThatThrownBy(() -> liteSubscriptionManager.validateLiteTopic("   ", 128))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("liteTopic is blank");
    }

    @Test
    public void testValidateLiteTopicTooLong() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        String tooLongTopic = StringUtils.repeat('a', 129);

        assertThatThrownBy(() -> liteSubscriptionManager.validateLiteTopic(tooLongTopic, 128))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("liteTopic length exceeded max length");
    }

    @Test
    public void testCheckLiteSubscriptionQuotaWithinLimit() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow 2 subscriptions
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(2).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // This should not throw exception as adding 1 to existing 0 is within quota
        liteSubscriptionManager.checkLiteSubscriptionQuota(1);
    }

    @Test
    public void testCheckLiteSubscriptionQuotaExceeded() {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to 0 - no quota allowed
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(0).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Try to add a topic - should exceed quota (0 + 1 > 0)
        assertThatThrownBy(() -> liteSubscriptionManager.checkLiteSubscriptionQuota(1))
            .isInstanceOf(LiteSubscriptionQuotaExceededException.class)
            .hasMessageContaining("Lite subscription quota exceeded");
    }

    @Test
    public void testCheckLiteSubscriptionQuotaExactLimit() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to 1 and add one topic
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(1).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Adding 0 should be fine (used for syncAll)
        liteSubscriptionManager.checkLiteSubscriptionQuota(0);
    }

    @Test
    public void testMultipleOperationsSequence() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(10).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Sequence of operations
        liteSubscriptionManager.subscribeLite("topic1", OffsetOption.LAST_OFFSET);
        liteSubscriptionManager.subscribeLite("topic2", OffsetOption.MIN_OFFSET);
        liteSubscriptionManager.subscribeLite("topic1", OffsetOption.LAST_OFFSET);

        Set<String> topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).containsExactlyInAnyOrder("topic1", "topic2");

        liteSubscriptionManager.unsubscribeLite("topic1");

        topicSet = liteSubscriptionManager.getLiteTopicSet();
        assertThat(topicSet).containsExactly("topic2");

        // Final sync
        liteSubscriptionManager.syncAllLiteSubscription();

        // Verify total RPC calls - should be 4 times (2 subscribe + 1 unsubscribe + 1 syncAll)
        verify(clientManager, times(4)).syncLiteSubscription(any(), any(), any());
    }

    @Test
    public void testLargeNumberOfTopics() throws Exception {
        final LiteSubscriptionManager liteSubscriptionManager = new LiteSubscriptionManager(consumerImpl, bindTopic,
            group);

        // Set quota to allow subscription first
        final Settings settings = Settings.newBuilder()
            .setSubscription(Subscription.newBuilder().setLiteSubscriptionQuota(100).build())
            .build();
        liteSubscriptionManager.sync(settings);

        // Add many topics
        final Set<String> expectedTopics = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            final String topic = "topic-" + i;
            expectedTopics.add(topic);
            liteSubscriptionManager.subscribeLite(topic, OffsetOption.LAST_OFFSET);
        }

        final Set<String> actualTopics = liteSubscriptionManager.getLiteTopicSet();
        assertThat(actualTopics).containsAll(expectedTopics);

        // Verify each topic was processed
        verify(clientManager, times(50)).syncLiteSubscription(any(), any(), any());
    }
}