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
    public class SimpleConsumerTest
    {
        // Helper method to mock the client manager and assert exceptions
        private async Task MockAndAssertAckException<TException>(SimpleConsumer consumer, MessageView messageView, Proto.Code code) where TException : Exception
        {
            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);

            var metadata = consumer.Sign();
            var response = new Proto.AckMessageResponse
            {
                Status = new Proto.Status { Code = code }
            };
            var invocation = new RpcInvocation<Proto.AckMessageRequest, Proto.AckMessageResponse>(null, response, metadata);
            mockClientManager.Setup(cm =>
                    cm.AckMessage(It.IsAny<Endpoints>(), It.IsAny<Proto.AckMessageRequest>(), It.IsAny<TimeSpan>()))
                    .Returns(Task.FromResult(invocation));
            try
            {
                await consumer.Ack(messageView);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(TException));
            }
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestReceiveWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            await consumer.Receive(16, TimeSpan.FromSeconds(15));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestAckWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            var messageView = MessageView.FromProtobuf(CreateMessage());
            await consumer.Ack(messageView);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestSubscribeWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            await consumer.Subscribe("testTopic", new FilterExpression("*"));
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestUnsubscribeWithoutStart()
        {
            var consumer = CreateSimpleConsumer();
            consumer.Unsubscribe("testTopic");
        }

        [TestMethod]
        [ExpectedException(typeof(InternalErrorException))]
        public async Task TestReceiveWithZeroMaxMessageNum()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
            await consumer.Receive(0, TimeSpan.FromSeconds(15));
        }

        [TestMethod]
        public async Task TestAck()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
            var messageView = CreateMessageView();

            await MockAndAssertAckException<BadRequestException>(consumer, messageView, Proto.Code.BadRequest);
            await MockAndAssertAckException<BadRequestException>(consumer, messageView, Proto.Code.IllegalTopic);
            await MockAndAssertAckException<BadRequestException>(consumer, messageView,
                Proto.Code.IllegalConsumerGroup);
            await MockAndAssertAckException<BadRequestException>(consumer, messageView,
                Proto.Code.InvalidReceiptHandle);
            await MockAndAssertAckException<BadRequestException>(consumer, messageView, Proto.Code.ClientIdRequired);
            await MockAndAssertAckException<UnauthorizedException>(consumer, messageView, Proto.Code.Unauthorized);
            await MockAndAssertAckException<ForbiddenException>(consumer, messageView, Proto.Code.Forbidden);
            await MockAndAssertAckException<NotFoundException>(consumer, messageView, Proto.Code.NotFound);
            await MockAndAssertAckException<NotFoundException>(consumer, messageView, Proto.Code.TopicNotFound);
            await MockAndAssertAckException<TooManyRequestsException>(consumer, messageView,
                Proto.Code.TooManyRequests);
            await MockAndAssertAckException<InternalErrorException>(consumer, messageView, Proto.Code.InternalError);
            await MockAndAssertAckException<InternalErrorException>(consumer, messageView,
                Proto.Code.InternalServerError);
            await MockAndAssertAckException<ProxyTimeoutException>(consumer, messageView, Proto.Code.ProxyTimeout);
            await MockAndAssertAckException<UnsupportedException>(consumer, messageView, Proto.Code.Unsupported);
        }

        [TestMethod]
        public async Task TestChangeInvisibleDuration()
        {
            var consumer = CreateSimpleConsumer();
            consumer.State = State.Running;
            var messageView = CreateMessageView();
            var invisibleDuration = TimeSpan.FromSeconds(3);

            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);
            var metadata = consumer.Sign();

            var response = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok }
            };
            var invocation =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    response, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(invocation));
            await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);

            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.BadRequest);
            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.IllegalTopic);
            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.IllegalConsumerGroup);
            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.IllegalInvisibleTime);
            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.InvalidReceiptHandle);
            await MockAndAssertChangeInvisibleDurationException<BadRequestException>(consumer, messageView,
                invisibleDuration, Proto.Code.ClientIdRequired);
            await MockAndAssertChangeInvisibleDurationException<UnauthorizedException>(consumer, messageView,
                invisibleDuration, Proto.Code.Unauthorized);
            await MockAndAssertChangeInvisibleDurationException<NotFoundException>(consumer, messageView,
                invisibleDuration, Proto.Code.NotFound);
            await MockAndAssertChangeInvisibleDurationException<NotFoundException>(consumer, messageView,
                invisibleDuration, Proto.Code.TopicNotFound);
            await MockAndAssertChangeInvisibleDurationException<TooManyRequestsException>(consumer, messageView,
                invisibleDuration, Proto.Code.TooManyRequests);
            await MockAndAssertChangeInvisibleDurationException<InternalErrorException>(consumer, messageView,
                invisibleDuration, Proto.Code.InternalError);
            await MockAndAssertChangeInvisibleDurationException<InternalErrorException>(consumer, messageView,
                invisibleDuration, Proto.Code.InternalServerError);
            await MockAndAssertChangeInvisibleDurationException<ProxyTimeoutException>(consumer, messageView,
                invisibleDuration, Proto.Code.ProxyTimeout);
            await MockAndAssertChangeInvisibleDurationException<UnsupportedException>(consumer, messageView,
                invisibleDuration, Proto.Code.Unsupported);
        }

        private async Task MockAndAssertChangeInvisibleDurationException<TException>(SimpleConsumer consumer,
            MessageView messageView, TimeSpan invisibleDuration, Proto.Code code) where TException : Exception
        {
            var mockClientManager = new Mock<IClientManager>();
            consumer.SetClientManager(mockClientManager.Object);

            var metadata = consumer.Sign();
            var response = new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status { Code = code }
            };
            var invocation =
                new RpcInvocation<Proto.ChangeInvisibleDurationRequest, Proto.ChangeInvisibleDurationResponse>(null,
                    response, metadata);
            mockClientManager.Setup(cm => cm.ChangeInvisibleDuration(It.IsAny<Endpoints>(),
                    It.IsAny<Proto.ChangeInvisibleDurationRequest>(), It.IsAny<TimeSpan>()))
                .Returns(Task.FromResult(invocation));
            try
            {
                await consumer.ChangeInvisibleDuration(messageView, invisibleDuration);
            }
            catch (Exception e)
            {
                Assert.IsInstanceOfType(e, typeof(TException));
            }
        }

        private SimpleConsumer CreateSimpleConsumer()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            var subscription = new Dictionary<string, FilterExpression> { { "testTopic", new FilterExpression("*") } };
            var consumer =
                new SimpleConsumer(clientConfig, "testConsumerGroup", TimeSpan.FromSeconds(15), subscription);
            return consumer;
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

        private MessageView CreateMessageView()
        {
            var message = CreateMessage();
            var messageQueue = new MessageQueue(new Proto.MessageQueue
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
                Topic = new Proto.Resource { ResourceNamespace = "testNamespace", Name = "testTopic" }
            });
            return MessageView.FromProtobuf(message, messageQueue);
        }
    }
}