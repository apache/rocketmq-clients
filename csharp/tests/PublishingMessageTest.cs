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
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class PublishingMessageTest
    {
        private const string ClientId = "fakeClientId";
        private static readonly Endpoints Endpoints = new Endpoints("127.0.0.1:8081");
        private const string Namespace = "fakeNamespace";


        [TestMethod]
        public void TestNormalMessage()
        {
            const string topic = "yourNormalTopic";
            var message = new Message.Builder().SetTopic(topic).SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            var topics = new ConcurrentDictionary<string, bool>
            {
                [topic] = true
            };
            var settings = new PublishingSettings(Namespace, ClientId, Endpoints,
                ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(3), TimeSpan.FromSeconds(3), topics);
            var publishingMessage = new PublishingMessage(message, settings, false);
            Assert.AreEqual(publishingMessage.MessageType, MessageType.Normal);
        }

        [TestMethod]
        public void TestFifoMessage()
        {
            const string topic = "yourFifoTopic";
            const string messageGroup = "yourMessageGroup";
            var message = new Message.Builder().SetTopic(topic)
                .SetMessageGroup(messageGroup).SetBody(Encoding.UTF8.GetBytes("foobar"))
                .Build();
            var topics = new ConcurrentDictionary<string, bool>
            {
                [topic] = true
            };
            var settings = new PublishingSettings(Namespace, ClientId, Endpoints,
                ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(3), TimeSpan.FromSeconds(3), topics);
            var publishingMessage = new PublishingMessage(message, settings, false);
            Assert.AreEqual(publishingMessage.MessageType, MessageType.Fifo);
        }

        [TestMethod]
        public void TestDelayMessage()
        {
            const string topic = "yourDelayTopic";
            var message = new Message.Builder()
                .SetTopic(topic)
                .SetDeliveryTimestamp(DateTime.UtcNow + TimeSpan.FromSeconds(30))
                .SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            var topics = new ConcurrentDictionary<string, bool>
            {
                [topic] = true
            };
            var settings = new PublishingSettings(Namespace, ClientId, Endpoints,
                ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(3),
                TimeSpan.FromSeconds(3), topics);
            var publishingMessage = new PublishingMessage(message, settings, false);
            Assert.AreEqual(publishingMessage.MessageType, MessageType.Delay);
        }

        [TestMethod]
        public void TestTransactionMessage()
        {
            const string topic = "yourTransactionMessage";
            var message = new Message.Builder()
                .SetTopic(topic)
                .SetBody(Encoding.UTF8.GetBytes("foobar")).Build();
            var topics = new ConcurrentDictionary<string, bool>
            {
                [topic] = true
            };
            var settings = new PublishingSettings(Namespace, ClientId, Endpoints,
                ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(3),
                TimeSpan.FromSeconds(3), topics);
            var publishingMessage = new PublishingMessage(message, settings, true);
            Assert.AreEqual(publishingMessage.MessageType, MessageType.Transaction);
        }
    }
}