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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ProcessQueueTest
    {
        private PushConsumer CreateAndSetupPushConsumer()
        {
            var pushConsumer = CreatePushConsumer("testTopic", 8, 1024, 4);
            pushConsumer.State = State.Running;
            return pushConsumer;
        }

        private Mock<IClientManager> SetupMockClientManager(PushConsumer pushConsumer)
        {
            var mockClientManager = new Mock<IClientManager>();
            pushConsumer.SetClientManager(mockClientManager.Object);
            return mockClientManager;
        }

        private static Proto.MessageQueue CreateMessageQueue()
        {
            return new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = "127.0.0.1",
                                Port = 8080
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
        }

        [TestMethod]
        public void TestExpired()
        {
            var pushConsumer = CreateAndSetupPushConsumer();
            var processQueue = CreateProcessQueue(pushConsumer);
            Assert.IsFalse(processQueue.Expired());
        }

        [TestMethod]
        public async Task TestReceiveMessageImmediately()
        {
            var pushConsumer = CreateAndSetupPushConsumer();
            var processQueue = CreateProcessQueue(pushConsumer);
            var mockClientManager = SetupMockClientManager(pushConsumer);

            var message = CreateMessage();
            var receiveMessageResponses = new List<Proto.ReceiveMessageResponse>
            {
                new Proto.ReceiveMessageResponse { Status = new Proto.Status { Code = Proto.Code.Ok } },
                new Proto.ReceiveMessageResponse { Message = message }
            };

            MockReceiveMessage(mockClientManager, pushConsumer, receiveMessageResponses);

            await Task.Delay(3000);
            processQueue.FetchMessageImmediately();

            Assert.AreEqual(processQueue.GetCachedMessageCount(), 1);
        }

        [TestMethod]
        public async Task TestEraseMessageWithConsumeOk()
        {
            var pushConsumer = CreateAndSetupPushConsumer();
            var messageView = CreateMessageView();
            var processQueue = CreateProcessQueue(pushConsumer);
            var mockClientManager = SetupMockClientManager(pushConsumer);

            MockAckMessage(mockClientManager, pushConsumer, Proto.Code.Ok);

            processQueue.CacheMessages(new List<MessageView> { messageView });

            processQueue.EraseMessage(messageView, ConsumeResult.SUCCESS);

            mockClientManager.Verify(cm => cm.AckMessage(It.IsAny<Endpoints>(), It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()), Times.Once);
        }

        [TestMethod]
        public async Task TestEraseMessageWithAckFailure()
        {
            var pushConsumer = CreateAndSetupPushConsumer();
            var messageView = CreateMessageView();
            var processQueue = CreateProcessQueue(pushConsumer);
            var mockClientManager = SetupMockClientManager(pushConsumer);

            MockAckMessage(mockClientManager, pushConsumer, Proto.Code.InternalServerError);

            processQueue.CacheMessages(new List<MessageView> { messageView });

            var ackTimes = 3;

            processQueue.EraseMessage(messageView, ConsumeResult.SUCCESS);
            await Task.Delay(ProcessQueue.AckMessageFailureBackoffDelay * ackTimes);

            mockClientManager.Verify(cm => cm.AckMessage(It.IsAny<Endpoints>(), It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()), Times.AtLeast(ackTimes));
        }

        private void MockReceiveMessage(Mock<IClientManager> mockClientManager, PushConsumer pushConsumer, List<Proto.ReceiveMessageResponse> responses)
        {
            var metadata = pushConsumer.Sign();
            var invocation = new RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>(null, responses, metadata);

            mockClientManager.Setup(cm => cm.ReceiveMessage(It.IsAny<Endpoints>(), It.IsAny<Proto.ReceiveMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(invocation));
        }

        private void MockAckMessage(Mock<IClientManager> mockClientManager, PushConsumer pushConsumer, Proto.Code responseCode)
        {
            var metadata = pushConsumer.Sign();
            var response = new Proto.AckMessageResponse { Status = new Proto.Status { Code = responseCode } };

            var invocation = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null, response, metadata);

            mockClientManager.Setup(cm => cm.AckMessage(It.IsAny<Endpoints>(), It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(invocation));
        }

        private MessageView CreateMessageView()
        {
            return MessageView.FromProtobuf(CreateMessage(), new MessageQueue(CreateMessageQueue()));
        }

        private static ProcessQueue CreateProcessQueue(PushConsumer pushConsumer)
        {
            var processQueue = new ProcessQueue(pushConsumer, new MessageQueue(CreateMessageQueue()),
                pushConsumer.GetSubscriptionExpressions()["testTopic"], new CancellationTokenSource(),
                new CancellationTokenSource(), new CancellationTokenSource(),
                new CancellationTokenSource());
            return processQueue;
        }

        private Proto.Message CreateMessage()
        {
            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = "127.0.0.1",
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = "testTopic" },
                Body = body
            };
            return message;
        }

        private PushConsumer CreatePushConsumer(string topic, int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount)
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:8080").Build();
            var subscription = new Dictionary<string, FilterExpression> { { topic, new FilterExpression("*") } };
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(subscription), new TestMessageListener(),
                maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        }

        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView) => ConsumeResult.SUCCESS;
        }
    }
}