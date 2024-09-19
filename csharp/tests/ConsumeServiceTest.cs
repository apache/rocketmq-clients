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
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ConsumeServiceTest
    {
        private Proto.Digest _digest;
        private Proto.SystemProperties _systemProperties;
        private ByteString _body;
        private Proto.Message _message;
        private MessageView _messageView;

        [TestInitialize]
        public void SetUp()
        {
            _digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" };
            _systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = "127.0.0.1:8080",
                BodyDigest = _digest,
                BornTimestamp = new Timestamp()
            };
            _body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            _message = new Proto.Message
            {
                SystemProperties = _systemProperties,
                Topic = new Proto.Resource { Name = "testTopic" },
                Body = _body
            };
            _messageView = MessageView.FromProtobuf(_message);
        }

        [TestMethod]
        public void TestConsumeSuccess()
        {
            var messageListener = new TestSuccessMessageListener();
            var consumeService = CreateService(messageListener);
            Assert.AreEqual(ConsumeResult.SUCCESS, consumeService.Consume(_messageView).Result);
        }

        [TestMethod]
        public void TestConsumeFailure()
        {
            var messageListener = new TestFailureMessageListener();
            var consumeService = CreateService(messageListener);
            Assert.AreEqual(ConsumeResult.FAILURE, consumeService.Consume(_messageView).Result);
        }

        [TestMethod]
        public void TestConsumeWithException()
        {
            var messageListener = new TestExceptionMessageListener();
            var consumeService = CreateService(messageListener);
            Assert.AreEqual(ConsumeResult.FAILURE, consumeService.Consume(_messageView).Result);
        }

        [TestMethod]
        public void TestConsumeWithDelay()
        {
            var messageListener = new TestSuccessMessageListener();
            var consumeService = CreateService(messageListener);
            Assert.AreEqual(ConsumeResult.SUCCESS,
                consumeService.Consume(_messageView, TimeSpan.FromMilliseconds(500)).Result);
        }

        private TestConsumeService CreateService(IMessageListener messageListener)
        {
            return new TestConsumeService("testClientId", messageListener,
                new CurrentThreadTaskScheduler(), new CancellationToken());
        }

        private class TestSuccessMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView) => ConsumeResult.SUCCESS;
        }

        private class TestFailureMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView) => ConsumeResult.FAILURE;
        }

        private class TestExceptionMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView) => throw new Exception();
        }

        private class TestConsumeService : ConsumeService
        {
            public TestConsumeService(string clientId, IMessageListener messageListener,
                TaskScheduler consumptionTaskScheduler, CancellationToken consumptionCtsToken)
                : base(clientId, messageListener, consumptionTaskScheduler, consumptionCtsToken) { }

            public override void Consume(ProcessQueue pq, List<MessageView> messageViews) => Task.FromResult(0);
        }
    }

}