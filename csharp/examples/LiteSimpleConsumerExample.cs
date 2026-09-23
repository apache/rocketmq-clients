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
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq;

namespace examples
{
    /// <summary>
    /// Demonstrates how to consume lite topics with LiteSimpleConsumer, which pulls
    /// messages explicitly and acks them one by one.
    ///
    /// Key Points:
    /// - The consumer binds to one parent topic, all lite topics belong to it
    /// - SubscribeLite() / UnsubscribeLite() change the lite topic set at runtime
    /// - Receive() returns a batch, Ack() confirms the consumption of each message
    /// - The lite topic is reported in ack requests automatically
    ///
    /// Prerequisites:
    /// - Broker: enableLmq=true, enableMultiDispatch=true
    /// - Parent topic created with message.type=LITE
    /// - Consumer group created with the attribute +lite.bind.topic=&lt;parentTopic&gt;
    /// </summary>
    internal static class LiteSimpleConsumerExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(LiteSimpleConsumerExample).FullName);

        private static readonly string Endpoint = Environment.GetEnvironmentVariable("ROCKETMQ_ENDPOINT") ?? "127.0.0.1:8081";
        private const string BindTopic = "topic-lite";
        private const string ConsumerGroup = "GID-lite-simple-consumer";
        private static readonly TimeSpan AwaitDuration = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan InvisibleDuration = TimeSpan.FromSeconds(15);

        internal static async Task QuickStart()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(Endpoint)
                .Build();

            var liteTopic = $"lite-topic-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

            // Send some lite messages first, the producer publishes them to the parent topic
            // and the broker indexes them under the lite topic.
            await SendLiteMessages(clientConfig, BindTopic, liteTopic);

            // Build the lite simple consumer bound to the parent topic.
            Logger.LogInformation($"Creating LiteSimpleConsumer, bindTopic={BindTopic}, consumerGroup={ConsumerGroup}");
            var consumer = await new LiteSimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(ConsumerGroup)
                .SetBindTopic(BindTopic)
                .SetAwaitDuration(AwaitDuration)
                .Build();

            try
            {
                // Consume from the minimum offset so that messages sent before the
                // subscription are delivered as well.
                await consumer.SubscribeLite(liteTopic, OffsetOption.MinOffset);
                Logger.LogInformation($"Subscribed to {liteTopic}, lite topic set=[{string.Join(", ", consumer.GetLiteTopicSet())}]");

                var received = new List<string>();
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (received.Count < 5 && DateTime.UtcNow < deadline)
                {
                    var messages = await consumer.Receive(16, InvisibleDuration);
                    foreach (var message in messages)
                    {
                        Logger.LogInformation($"Received message, messageId={message.MessageId}, topic={message.Topic}, " +
                                              $"liteTopic={message.LiteTopic}, body={Encoding.UTF8.GetString(message.Body)}");
                        await consumer.Ack(message);
                        received.Add(message.MessageId);
                    }
                }

                Logger.LogInformation($"Received and acked {received.Count} lite message(s)");

                await consumer.UnsubscribeLite(liteTopic);
                Logger.LogInformation($"Unsubscribed from {liteTopic}, lite topic set=[{string.Join(", ", consumer.GetLiteTopicSet())}]");
            }
            finally
            {
                await consumer.DisposeAsync();
                Logger.LogInformation("LiteSimpleConsumer closed");
            }
        }

        private static async Task SendLiteMessages(ClientConfig clientConfig, string parentTopic, string liteTopic)
        {
            var producer = await new Producer.Builder()
                .SetTopics(parentTopic)
                .SetClientConfig(clientConfig)
                .Build();

            try
            {
                for (var i = 0; i < 5; i++)
                {
                    var message = new Message.Builder()
                        .SetTopic(parentTopic)
                        .SetBody(Encoding.UTF8.GetBytes($"lite-simple-consumer-body-{i}"))
                        .SetTag("LiteTest")
                        .SetKeys($"lite-{i}")
                        .SetLiteTopic(liteTopic)
                        .Build();

                    var receipt = await producer.Send(message);
                    Logger.LogInformation($"Sent lite message, messageId={receipt.MessageId}, liteTopic={liteTopic}");
                }
            }
            finally
            {
                await producer.DisposeAsync();
            }
        }
    }
}
