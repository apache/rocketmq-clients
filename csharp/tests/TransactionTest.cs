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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class TransactionTest
    {
        private const string FakeTag = "fakeTag";
        private const string FakeTopic = "fakeTopic";
        private const string FakeMsgKey = "fakeMsgKey";
        private const string BrokerName = "broker0";
        private const string Host = "127.0.0.1";
        private const int Port = 8080;
        private Producer _producer;
        private byte[] _bytes;

        [TestInitialize]
        public void SetUp()
        {
            _producer = CreateTestClient();
            _bytes = Encoding.UTF8.GetBytes("fakeBytes");
        }

        [TestMethod]
        public void TestTryAddMessage()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            var publishingMessage = transaction.TryAddMessage(message);
            Assert.AreEqual(MessageType.Transaction, publishingMessage.MessageType);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestTryAddExceededMessages()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            transaction.TryAddMessage(message);
            transaction.TryAddMessage(message);
        }

        [TestMethod]
        public void TestTryAddReceipt()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = CreateMessageQueue();

            var sendReceipt = CreateSendReceipt(mq0);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestTryAddReceiptNotContained()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            var publishingMessage = new PublishingMessage(message, new PublishingSettings("fakeNamespace",
                "fakeClientId", new Endpoints("fakeEndpoints"), new Mock<IRetryPolicy>().Object,
                TimeSpan.FromSeconds(10), new ConcurrentDictionary<string, bool>()), true);
            var mq0 = CreateMessageQueue();

            var sendReceipt = CreateSendReceipt(mq0);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestCommitWithNoReceipts()
        {
            var transaction = new Transaction(_producer);
            await transaction.Commit();
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task TestRollbackWithNoReceipts()
        {
            var transaction = new Transaction(_producer);
            await transaction.Rollback();
        }

        [TestMethod]
        public async Task TestCommit()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = CreateMessageQueue();

            var sendReceipt = CreateSendReceipt(mq0);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());

            var mockClientManager = new Mock<IClientManager>();
            _producer.SetClientManager(mockClientManager.Object);

            SetupCommitOrRollback(mockClientManager, true);

            await transaction.Commit();
        }

        [TestMethod]
        public async Task TestRollback()
        {
            var transaction = new Transaction(_producer);
            var message = CreateMessage();
            var publishingMessage = transaction.TryAddMessage(message);
            var mq0 = CreateMessageQueue();

            var sendReceipt = CreateSendReceipt(mq0);
            transaction.TryAddReceipt(publishingMessage, sendReceipt.First());

            var mockClientManager = new Mock<IClientManager>();
            _producer.SetClientManager(mockClientManager.Object);

            SetupCommitOrRollback(mockClientManager, false);

            await transaction.Rollback();
        }

        private Producer CreateTestClient()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            return new Producer(clientConfig, new ConcurrentDictionary<string, bool>(),
                1, null);
        }

        private Message CreateMessage()
        {
            return new Message.Builder()
                .SetTopic(FakeTopic)
                .SetBody(_bytes)
                .SetTag(FakeTag)
                .SetKeys(FakeMsgKey)
                .Build();
        }

        private Proto.MessageQueue CreateMessageQueue()
        {
            return new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = BrokerName,
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses = { new Proto.Address { Host = Host, Port = Port } }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource { ResourceNamespace = "foo-bar-namespace", Name = "TestTopic" }
            };
        }

        private IEnumerable<SendReceipt> CreateSendReceipt(Proto.MessageQueue mq0)
        {
            var metadata = _producer.Sign();
            var sendResultEntry = new Proto.SendResultEntry
            {
                MessageId = "fakeMsgId",
                TransactionId = "fakeTxId",
                Status = new Proto.Status { Code = Proto.Code.Ok },
                Offset = 1
            };
            var sendMessageResponse = new Proto.SendMessageResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok },
                Entries = { sendResultEntry }
            };
            var invocation = new RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>(null, sendMessageResponse, metadata);
            return SendReceipt.ProcessSendMessageResponse(new MessageQueue(mq0), invocation);
        }

        private void SetupCommitOrRollback(Mock<IClientManager> mockClientManager, bool commit)
        {
            var endTransactionMetadata = _producer.Sign();
            var endTransactionResponse = new Proto.EndTransactionResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok }
            };
            var endTransactionInvocation = new RpcInvocation<Proto.EndTransactionRequest, Proto.EndTransactionResponse>(null,
                endTransactionResponse, endTransactionMetadata);
            mockClientManager.Setup(cm => cm.EndTransaction(It.IsAny<Endpoints>(),
                It.IsAny<Proto.EndTransactionRequest>(), It.IsAny<TimeSpan>())).Returns(Task.FromResult(endTransactionInvocation));

            _producer.State = State.Running;
        }
    }
}