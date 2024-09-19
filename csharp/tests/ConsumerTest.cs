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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

[assembly: InternalsVisibleTo("tests")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace tests
{
    [TestClass]
    public class ConsumerTest
    {
        [TestMethod]
        public async Task TestReceiveMessage()
        {
            var maxCacheMessageCount = 8;
            var maxCacheMessageSizeInBytes = 1024;
            var consumptionThreadCount = 4;

            var consumer =
                CreateTestClient(maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);

            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "00000000" };
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
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "testNamespace",
                    Name = "testTopic"
                },
                Body = body
            };
            var receiveMessageResponse0 = new Proto.ReceiveMessageResponse
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.Ok
                }
            };
            var receiveMessageResponse1 = new Proto.ReceiveMessageResponse
            {
                Message = message
            };
            var metadata = consumer.Sign();
            var receiveMessageResponseList = new List<Proto.ReceiveMessageResponse>
                { receiveMessageResponse0, receiveMessageResponse1 };
            var receiveMessageInvocation =
                new RpcInvocation<Proto.ReceiveMessageRequest, List<Proto.ReceiveMessageResponse>>(null,
                    receiveMessageResponseList, metadata);
            mockClientManager.Setup(cm => cm.ReceiveMessage(It.IsAny<Endpoints>(),
                It.IsAny<Proto.ReceiveMessageRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(receiveMessageInvocation));

            var receivedMessageCount = 1;
            var mq = new Proto.MessageQueue
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
            var request = consumer.WrapReceiveMessageRequest(1, new MessageQueue(mq), new FilterExpression("*"),
                TimeSpan.FromSeconds(15), Guid.NewGuid().ToString());
            var receiveMessageResult = await consumer.ReceiveMessage(request, new MessageQueue(mq),
                TimeSpan.FromSeconds(15));
            Assert.AreEqual(receiveMessageResult.Messages.Count, receivedMessageCount);
        }

        private PushConsumer CreateTestClient(int maxCacheMessageCount, int maxCacheMessageSizeInBytes,
            int consumptionThreadCount)
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:9876")
                .Build();
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(), new TestMessageListener(),
                maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount);
        }

        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}