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
    /// 
    /// Key Points:
    /// - Priority must be >= 0 (higher value = higher priority)
    /// - Cannot be used with: messageGroup, deliveryTimestamp, or liteTopic
    /// - Consumer Group for testing: GID-priority-consumer
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

            // Configure client for local testing (no authentication required)
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8081")
                .Build();

            const string topic = "topic-priority";
            
            // Create producer with singleton pattern (recommended)
            var producer = await new Producer.Builder()
                .SetTopics(topic)  // Prefetch topic route for better performance
                .SetClientConfig(clientConfig)
                .Build();

            // Define message content
            var body = Encoding.UTF8.GetBytes("This is a priority message for testing");
            const string tag = "PriorityTest";
            
            Logger.LogInformation($"Consumer Group: GID-priority-consumer");
            Logger.LogInformation("Sending messages with different priority levels (higher value = higher priority)...");
            
            // Send messages with priority levels from low to high
            var priorities = new[] { 1, 3, 5, 8, 10 };

            foreach (var priority in priorities)
            {
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(body)
                    .SetTag(tag)
                    .SetKeys($"priority-{priority}")
                    .SetPriority(priority)  // Higher priority messages are consumed first
                    .Build();

                try
                {
                    var sendReceipt = await producer.Send(message);
                    Logger.LogInformation($"Sent priority message successfully, messageId={sendReceipt.MessageId}, priority={priority}");
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Failed to send priority message, priority={priority}");
                }
            }

            Logger.LogInformation("\nAll priority messages sent. Messages will be consumed in priority order (highest first).");

            // Close the producer if you don't need it anymore.
            await producer.DisposeAsync();
        }
    }
}
