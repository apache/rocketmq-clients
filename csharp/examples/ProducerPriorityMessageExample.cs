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
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq;

namespace examples
{
    /// <summary>
    /// Demonstrates how to send priority messages using Apache RocketMQ C# client.
    /// Priority messages allow you to assign different priority levels to messages,
    /// where higher priority messages are consumed before lower priority ones.
    /// </summary>
    internal static class ProducerPriorityMessageExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(ProducerPriorityMessageExample).FullName);

        private static readonly string AccessKey = Environment.GetEnvironmentVariable("ROCKETMQ_ACCESS_KEY");
        private static readonly string SecretKey = Environment.GetEnvironmentVariable("ROCKETMQ_SECRET_KEY");
        private static readonly string Endpoint = Environment.GetEnvironmentVariable("ROCKETMQ_ENDPOINT");

        internal static async Task QuickStart()
        {
            // Enable the switch if you use .NET Core 3.1 and want to disable TLS/SSL.
            // AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // No credentials needed for local testing
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8081")
                .Build();

            const string topic = "topic-priority";
            // In most case, you don't need to create too many producers, singleton pattern is recommended.
            // Producer here will be closed automatically.
            var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .Build();

            // Define your message body.
            var bytes = Encoding.UTF8.GetBytes("This is a priority message for testing");
            const string tag = "PriorityTest";

            // Send messages with different priority levels
            // Priority must be >= 0, higher value means higher priority
            // Consumer Group: GID-priority-consumer
            Logger.LogInformation($"Consumer Group: GID-priority-consumer");
            var priorities = new[] { 1, 3, 5, 8, 10 };

            foreach (var priority in priorities)
            {
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys($"yourMessageKey-priority-{priority}")
                    // Set priority of message. Higher priority messages are consumed first.
                    .SetPriority(priority)
                    .Build();

                try
                {
                    var sendReceipt = await producer.Send(message);
                    Logger.LogInformation($"Priority message sent successfully, messageId={sendReceipt.MessageId}, priority={priority}");
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Failed to send priority message, priority={priority}");
                }
            }

            Logger.LogInformation("\nAll priority messages sent. Higher priority messages will be consumed first by GID-priority-consumer.");

            // Close the producer if you don't need it anymore.
            await producer.DisposeAsync();
        }
    }
}
