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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class LiteSimpleConsumerTest
    {
        private const string BindTopic = "lite-parent-topic";
        private const string ConsumerGroup = "lite-simple-consumer-group";

        [TestMethod]
        public void TestBuildWithoutClientConfig()
        {
            var builder = new LiteSimpleConsumer.Builder().SetConsumerGroup(ConsumerGroup).SetBindTopic(BindTopic);
            Assert.ThrowsExactly<ArgumentException>(() => builder.SetClientConfig(null));
        }

        [TestMethod]
        public void TestBuildWithInvalidConsumerGroup()
        {
            Assert.ThrowsExactly<ArgumentException>(() =>
                new LiteSimpleConsumer.Builder().SetConsumerGroup("invalid group!"));
            Assert.ThrowsExactly<ArgumentException>(() =>
                new LiteSimpleConsumer.Builder().SetConsumerGroup(null));
        }

        [TestMethod]
        public void TestBuildWithBlankBindTopic()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new LiteSimpleConsumer.Builder().SetBindTopic(""));
        }

        [TestMethod]
        public void TestSetNonPositiveAwaitDuration()
        {
            Assert.ThrowsExactly<ArgumentException>(() =>
                new LiteSimpleConsumer.Builder().SetAwaitDuration(TimeSpan.Zero));
        }

        [TestMethod]
        public void TestBuildWithoutRequiredOptions()
        {
            var builder = new LiteSimpleConsumer.Builder();
            Assert.ThrowsExactly<ArgumentException>(() => builder.Build().GetAwaiter().GetResult());
        }

        [TestMethod]
        public void TestConstructorRejectsBlankBindTopic()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:8081").Build();
            Assert.ThrowsExactly<ArgumentException>(() =>
                new LiteSimpleConsumer(clientConfig, ConsumerGroup, TimeSpan.FromSeconds(5), "  "));
        }

        [TestMethod]
        public void TestSettingsUseLiteSimpleConsumerClientType()
        {
            var consumer = CreateConsumer(out _);
            var settings = consumer.GetSettings().ToProtobuf();
            Assert.AreEqual(Proto.ClientType.LiteSimpleConsumer, settings.ClientType);
            Assert.IsTrue(consumer.IsLiteConsumer());
        }

        [TestMethod]
        public void TestHeartbeatUsesLiteSimpleConsumerClientType()
        {
            var consumer = CreateConsumer(out _);
            Assert.AreEqual(Proto.ClientType.LiteSimpleConsumer, consumer.WrapHeartbeatRequest().ClientType);
        }

        [TestMethod]
        public async Task TestSubscribeLiteBeforeStartup()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:8081").Build();
            var consumer = new LiteSimpleConsumer(clientConfig, ConsumerGroup, TimeSpan.FromSeconds(5), BindTopic);
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
                await consumer.SubscribeLite("lite-topic-1"));
        }

        [TestMethod]
        public async Task TestSubscribeLiteSyncsPartialAdd()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            var invocations = SetupSyncLiteSubscription(mockClientManager);

            await consumer.SubscribeLite("lite-topic-1", OffsetOption.MinOffset);

            Assert.AreEqual(1, invocations.Count);
            var request = invocations[0];
            Assert.AreEqual(Proto.LiteSubscriptionAction.PartialAdd, request.Action);
            CollectionAssert.Contains(request.LiteTopicSet.ToList(), "lite-topic-1");
            Assert.AreEqual(BindTopic, request.Topic.Name);
            Assert.AreEqual(ConsumerGroup, request.Group.Name);
            Assert.AreEqual(Proto.OffsetOption.Types.Policy.Min, request.OffsetOption.Policy);
            Assert.IsTrue(consumer.GetLiteTopicSet().Contains("lite-topic-1"));
        }

        [TestMethod]
        public async Task TestDuplicateSubscribeLiteSkipsRpc()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            var invocations = SetupSyncLiteSubscription(mockClientManager);

            await consumer.SubscribeLite("lite-topic-1");
            await consumer.SubscribeLite("lite-topic-1");

            Assert.AreEqual(1, invocations.Count);
        }

        [TestMethod]
        public async Task TestUnsubscribeLiteSyncsPartialRemove()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            var invocations = SetupSyncLiteSubscription(mockClientManager);

            await consumer.SubscribeLite("lite-topic-1");
            await consumer.UnsubscribeLite("lite-topic-1");

            Assert.AreEqual(2, invocations.Count);
            Assert.AreEqual(Proto.LiteSubscriptionAction.PartialRemove, invocations[1].Action);
            Assert.AreEqual(0, consumer.GetLiteTopicSet().Count);
        }

        [TestMethod]
        public async Task TestUnsubscribeUnknownLiteTopicIsNoop()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            var invocations = SetupSyncLiteSubscription(mockClientManager);

            await consumer.UnsubscribeLite("unknown-lite-topic");

            Assert.AreEqual(0, invocations.Count);
        }

        [TestMethod]
        public async Task TestSubscribeLiteWithBlankTopic()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            SetupSyncLiteSubscription(mockClientManager);

            await Assert.ThrowsExactlyAsync<ArgumentException>(async () => await consumer.SubscribeLite("  "));
        }

        [TestMethod]
        public async Task TestOnNotifyUnsubscribeLiteCommand()
        {
            var consumer = CreateConsumer(out var mockClientManager);
            SetupSyncLiteSubscription(mockClientManager);
            await consumer.SubscribeLite("lite-topic-1");

            consumer.OnNotifyUnsubscribeLiteCommand(null, new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "lite-topic-1" });
            Assert.AreEqual(0, consumer.GetLiteTopicSet().Count);

            await consumer.SubscribeLite("lite-topic-2");
            consumer.OnNotifyUnsubscribeLiteCommand(null, new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "" });
            Assert.IsTrue(consumer.GetLiteTopicSet().Contains("lite-topic-2"));
        }

        [TestMethod]
        public void TestAckRequestCarriesLiteTopic()
        {
            var consumer = CreateConsumer(out _);
            var messageView = CreateMessageView("lite-topic-1");

            var request = consumer.WrapAckMessageRequest(messageView);

            Assert.AreEqual(1, request.Entries.Count);
            Assert.AreEqual("lite-topic-1", request.Entries[0].LiteTopic);
            Assert.AreEqual(BindTopic, request.Topic.Name);
        }

        [TestMethod]
        public void TestAckRequestWithoutLiteTopic()
        {
            var consumer = CreateConsumer(out _);
            var messageView = CreateMessageView(null);

            var request = consumer.WrapAckMessageRequest(messageView);

            Assert.IsFalse(request.Entries[0].HasLiteTopic);
        }

        [TestMethod]
        public void TestChangeInvisibleDurationCarriesLiteTopic()
        {
            var consumer = CreateConsumer(out _);
            var messageView = CreateMessageView("lite-topic-1");

            var request = consumer.WrapChangeInvisibleDuration(messageView, TimeSpan.FromSeconds(10));

            Assert.AreEqual("lite-topic-1", request.LiteTopic);
        }

        [TestMethod]
        public void TestUpdateSubscriptionLoadBalancerKeepsOnlyFirstReadableMasterQueue()
        {
            var consumer = CreateConsumer(out _);
            var routeData = new TopicRouteData(new List<Proto.MessageQueue>
            {
                CreateMessageQueue(0, 0, Proto.Permission.ReadWrite),
                CreateMessageQueue(1, 0, Proto.Permission.ReadWrite),
            });

            var loadBalancer = consumer.UpdateSubscriptionLoadBalancer(BindTopic, routeData);

            Assert.IsNotNull(loadBalancer);
            var mq = loadBalancer.TakeMessageQueue();
            Assert.AreEqual(0, mq.QueueId);
        }

        [TestMethod]
        public void TestUpdateSubscriptionLoadBalancerWithoutReadableMasterQueue()
        {
            var consumer = CreateConsumer(out _);
            var routeData = new TopicRouteData(new List<Proto.MessageQueue>
            {
                CreateMessageQueue(0, 1, Proto.Permission.ReadWrite),
            });

            var loadBalancer = consumer.UpdateSubscriptionLoadBalancer(BindTopic, routeData);

            Assert.ThrowsExactly<NotFoundException>(() => loadBalancer.TakeMessageQueue());
        }

        private static LiteSimpleConsumer CreateConsumer(out Mock<IClientManager> mockClientManager)
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:8081").Build();
            var consumer = new LiteSimpleConsumer(clientConfig, ConsumerGroup, TimeSpan.FromSeconds(5), BindTopic);
            mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);
            consumer.State = State.Running;
            // The lite subscription quota is pushed by the server through the settings command.
            consumer.OnSettingsCommand(null, new Proto.Settings
            {
                Subscription = new Proto.Subscription
                {
                    LiteSubscriptionQuota = 64,
                    MaxLiteTopicSize = 64
                }
            });
            return consumer;
        }

        private static List<Proto.SyncLiteSubscriptionRequest> SetupSyncLiteSubscription(Mock<IClientManager> mockClientManager)
        {
            var invocations = new List<Proto.SyncLiteSubscriptionRequest>();
            mockClientManager.Setup(cm => cm.SyncLiteSubscription(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.SyncLiteSubscriptionRequest>(), It.IsAny<TimeSpan>()))
                .Returns((Endpoints _, Proto.SyncLiteSubscriptionRequest request, TimeSpan _) =>
                {
                    invocations.Add(request.Clone());
                    var response = new Proto.SyncLiteSubscriptionResponse
                    {
                        Status = new Proto.Status { Code = Proto.Code.Ok }
                    };
                    return Task.FromResult(
                        new RpcInvocation<Proto.SyncLiteSubscriptionRequest, Proto.SyncLiteSubscriptionResponse>(
                            request, response, null));
                });
            return invocations;
        }

        private static MessageView CreateMessageView(string liteTopic)
        {
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BodyDigest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" },
                BodyEncoding = Proto.Encoding.Identity,
                BornHost = "127.0.0.1",
                BornTimestamp = new Timestamp(),
                ReceiptHandle = "fake-receipt-handle",
            };
            if (null != liteTopic)
            {
                systemProperties.LiteTopic = liteTopic;
            }

            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = BindTopic },
                Body = ByteString.CopyFrom("foobar", Encoding.UTF8)
            };
            return MessageView.FromProtobuf(message);
        }

        private static Proto.MessageQueue CreateMessageQueue(int queueId, int brokerId, Proto.Permission permission)
        {
            return new Proto.MessageQueue
            {
                Id = queueId,
                Permission = permission,
                Broker = new Proto.Broker
                {
                    Id = brokerId,
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8081 } }
                    }
                },
                Topic = new Proto.Resource { Name = BindTopic },
                AcceptMessageTypes = { Proto.MessageType.Lite }
            };
        }
    }
}
