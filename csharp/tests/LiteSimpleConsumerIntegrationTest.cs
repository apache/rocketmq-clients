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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    /// <summary>
    /// Integration tests of LiteSimpleConsumer against a real RocketMQ cluster.
    ///
    /// They are skipped unless ROCKETMQ_CSHARP_LITE_ENDPOINTS is set, so that the
    /// default test run stays offline.
    ///
    /// Server prerequisites:
    /// - broker.conf: enableLmq=true, enableMultiDispatch=true
    /// - parent topic created with message.type=LITE
    /// - consumer group created with the attribute +lite.bind.topic=&lt;parentTopic&gt;
    /// </summary>
    [TestClass]
    public class LiteSimpleConsumerIntegrationTest
    {
        private static readonly string Endpoint = Environment.GetEnvironmentVariable("ROCKETMQ_CSHARP_LITE_ENDPOINTS");
        private static readonly string BindTopic =
            Environment.GetEnvironmentVariable("ROCKETMQ_CSHARP_LITE_PARENT_TOPIC") ?? "lite-parent-topic";
        private static readonly string ConsumerGroup =
            Environment.GetEnvironmentVariable("ROCKETMQ_CSHARP_LITE_GROUP") ?? "csharp-lite-unittest-group";

        private static readonly TimeSpan AwaitDuration = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan InvisibleDuration = TimeSpan.FromSeconds(15);

        private ClientConfig _clientConfig;
        private LiteSimpleConsumer _consumer;
        private Producer _producer;

        [TestInitialize]
        public void SetUp()
        {
            if (string.IsNullOrEmpty(Endpoint))
            {
                Assert.Inconclusive("ROCKETMQ_CSHARP_LITE_ENDPOINTS is not set, skip the lite integration test");
            }

            _clientConfig = new ClientConfig.Builder()
                .SetEndpoints(Endpoint)
                .SetRequestTimeout(TimeSpan.FromSeconds(10))
                .Build();
        }

        [TestCleanup]
        public async Task TearDown()
        {
            if (null != _consumer)
            {
                await _consumer.DisposeAsync();
                _consumer = null;
            }
            if (null != _producer)
            {
                await _producer.DisposeAsync();
                _producer = null;
            }
        }

        [TestMethod]
        public async Task TestReceiveAndAckLiteMessages()
        {
            var liteTopic = $"lite-topic-it-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            await StartClientsAsync();

            var bodies = await SendLiteMessagesAsync(liteTopic, 5);

            await _consumer.SubscribeLite(liteTopic, OffsetOption.MinOffset);
            Assert.IsTrue(_consumer.GetLiteTopicSet().Contains(liteTopic));

            var received = await ReceiveAndAckAsync(5, TimeSpan.FromSeconds(30));

            CollectionAssert.AreEquivalent(bodies.OrderBy(b => b).ToList(), received.OrderBy(b => b).ToList());

            await _consumer.UnsubscribeLite(liteTopic);
            Assert.AreEqual(0, _consumer.GetLiteTopicSet().Count);
        }

        [TestMethod]
        public async Task TestDeliverMessagesSentAfterSubscribeLite()
        {
            var liteTopic = $"lite-topic-live-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            await StartClientsAsync();

            await _consumer.SubscribeLite(liteTopic);
            var bodies = await SendLiteMessagesAsync(liteTopic, 3);

            var received = await ReceiveAndAckAsync(3, TimeSpan.FromSeconds(30));

            CollectionAssert.AreEquivalent(bodies.OrderBy(b => b).ToList(), received.OrderBy(b => b).ToList());
        }

        [TestMethod]
        public async Task TestStopDeliveringAfterUnsubscribeLite()
        {
            var liteTopic = $"lite-topic-off-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
            await StartClientsAsync();

            await _consumer.SubscribeLite(liteTopic, OffsetOption.MinOffset);
            await _consumer.UnsubscribeLite(liteTopic);
            Assert.AreEqual(0, _consumer.GetLiteTopicSet().Count);

            await SendLiteMessagesAsync(liteTopic, 3);

            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                var messages = await _consumer.Receive(16, InvisibleDuration);
                Assert.AreEqual(0, messages.Count, "No message is expected after unsubscribeLite");
            }
        }

        private async Task StartClientsAsync()
        {
            _producer = await new Producer.Builder()
                .SetTopics(BindTopic)
                .SetClientConfig(_clientConfig)
                .Build();

            _consumer = await new LiteSimpleConsumer.Builder()
                .SetClientConfig(_clientConfig)
                .SetConsumerGroup(ConsumerGroup)
                .SetBindTopic(BindTopic)
                .SetAwaitDuration(AwaitDuration)
                .Build();

            Assert.AreEqual(BindTopic, _consumer.GetBindTopic());
            Assert.AreEqual(0, _consumer.GetLiteTopicSet().Count);
        }

        private async Task<List<string>> SendLiteMessagesAsync(string liteTopic, int count)
        {
            var bodies = new List<string>();
            for (var i = 0; i < count; i++)
            {
                var body = $"lite-body-{liteTopic}-{i}";
                var message = new Message.Builder()
                    .SetTopic(BindTopic)
                    .SetBody(Encoding.UTF8.GetBytes(body))
                    .SetTag("LiteTest")
                    .SetKeys($"lite-{i}")
                    .SetLiteTopic(liteTopic)
                    .Build();

                var receipt = await _producer.Send(message);
                Assert.IsNotNull(receipt.MessageId);
                bodies.Add(body);
            }
            return bodies;
        }

        private async Task<List<string>> ReceiveAndAckAsync(int expected, TimeSpan timeout)
        {
            var received = new List<string>();
            var deadline = DateTime.UtcNow.Add(timeout);
            while (received.Count < expected && DateTime.UtcNow < deadline)
            {
                var messages = await _consumer.Receive(16, InvisibleDuration);
                foreach (var message in messages)
                {
                    Assert.IsFalse(string.IsNullOrEmpty(message.LiteTopic),
                        "Received lite message must carry its lite topic");
                    await _consumer.Ack(message);
                    received.Add(Encoding.UTF8.GetString(message.Body));
                }
            }
            return received;
        }
    }
}
