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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using Moq;

namespace tests
{
    [TestClass]
    public class ProducerTest
    {
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestSendBeforeStartup()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            var publishingTopics = new ConcurrentDictionary<string, bool>();
            publishingTopics.TryAdd("testTopic", true);
            var producer = new Producer(clientConfig, publishingTopics, 1, null);
            var message = new Message.Builder().SetTopic("testTopic").SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            await producer.Send(message);
        }

        [TestMethod]
        public async Task TestSendWithTopic()
        {
            var producer = CreateTestClient();
            producer.State = State.Running;
            var message = new Message.Builder().SetTopic("testTopic").SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            var metadata = producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                },
                Entries = { sendResultEntry }
            };
            var sendMessageInvocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null,
                sendMessageResponse, metadata);
            var mockClientManager = new Mock<IClientManager>();
            producer.SetClientManager(mockClientManager.Object);
            mockClientManager.Setup(cm => cm.SendMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.SendMessageRequest>(), It.IsAny<TimeSpan>())).Returns(Task.FromResult(sendMessageInvocation));
            await producer.Send(message);
            mockClientManager.Verify(cm => cm.SendMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.SendMessageRequest>(), It.IsAny<TimeSpan>()), Times.Once);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task TestSendFailureWithTopic()
        {
            var producer = CreateTestClient();
            producer.State = State.Running;
            var message = new Message.Builder().SetTopic("testTopic").SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            var mockClientManager = new Mock<IClientManager>();
            producer.SetClientManager(mockClientManager.Object);
            var exception = new ArgumentException();
            mockClientManager.Setup(cm => cm.SendMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.SendMessageRequest>(), It.IsAny<TimeSpan>())).Throws(exception);
            await producer.Send(message);
            var maxAttempts = producer.PublishingSettings.GetRetryPolicy().GetMaxAttempts();
            mockClientManager.Verify(cm => cm.SendMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.SendMessageRequest>(), It.IsAny<TimeSpan>()), Times.Exactly(maxAttempts));
        }

        private Producer CreateTestClient()
        {
            const string host0 = "127.0.0.1";
            var mqs = new List<Proto.MessageQueue>();
            var mq0 = new Proto.MessageQueue
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
                                Host = host0,
                                Port = 80
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "testTopic",
                },
                AcceptMessageTypes = { Proto.MessageType.Normal }
            };
            mqs.Add(mq0);
            var topicRouteData = new TopicRouteData(mqs);
            var publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            var producer = new Producer(clientConfig, new ConcurrentDictionary<string, bool>(),
                1, null);
            producer._publishingRouteDataCache.TryAdd("testTopic", publishingLoadBalancer);
            return producer;
        }
    }
}