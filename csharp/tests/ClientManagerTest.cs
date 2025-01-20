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
using Apache.Rocketmq.V2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;

namespace tests
{
    [TestClass]
    public class ClientManagerTest
    {
        private static readonly Endpoints FakeEndpoints = new Endpoints("127.0.0.1:8080");
        private static IClientManager _clientManager;

        private readonly ClientConfig _clientConfig = new ClientConfig.Builder()
            .SetEndpoints("127.0.0.1:8080")
            .Build();

        [TestInitialize]
        public void Initialize()
        {
            _clientManager = new ClientManager(CreateTestClient());
        }

        [TestMethod]
        public void TestHeartbeat()
        {
            var request = new HeartbeatRequest();
            _clientManager.Heartbeat(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.Heartbeat(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestSendMessage()
        {
            var request = new SendMessageRequest();
            _clientManager.SendMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.SendMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestQueryAssignment()
        {
            var request = new QueryAssignmentRequest();
            _clientManager.QueryAssignment(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.QueryAssignment(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestReceiveMessage()
        {
            var request = new ReceiveMessageRequest();
            _clientManager.ReceiveMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.ReceiveMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestAckMessage()
        {
            var request = new AckMessageRequest();
            _clientManager.AckMessage(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.AckMessage(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestChangeInvisibleDuration()
        {
            var request = new ChangeInvisibleDurationRequest();
            _clientManager.ChangeInvisibleDuration(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.ChangeInvisibleDuration(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestForwardMessageToDeadLetterQueue()
        {
            var request = new ForwardMessageToDeadLetterQueueRequest();
            _clientManager.ForwardMessageToDeadLetterQueue(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.ForwardMessageToDeadLetterQueue(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestEndTransaction()
        {
            var request = new EndTransactionRequest();
            _clientManager.EndTransaction(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.EndTransaction(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        [TestMethod]
        public void TestNotifyClientTermination()
        {
            var request = new NotifyClientTerminationRequest();
            _clientManager.NotifyClientTermination(FakeEndpoints, request, TimeSpan.FromSeconds(1));
            _clientManager.NotifyClientTermination(null, request, TimeSpan.FromSeconds(1));
            // Expect no exception thrown.
        }

        private Client CreateTestClient()
        {
            return new Producer(_clientConfig, new ConcurrentDictionary<string, bool>(), 1, null);
        }
    }
}
