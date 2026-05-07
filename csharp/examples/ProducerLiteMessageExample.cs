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
    /// Demonstrates how to send lite messages using Apache RocketMQ C# client.
    /// Lite messages use dynamic topic routing without pre-defining all topics.
    /// 
    /// Key Points:
    /// - LiteTopic enables flexible message routing
    /// - Cannot be used with: messageGroup, deliveryTimestamp, or priority
    /// - Parent topic must be configured for lite messaging
    /// </summary>
    internal static class ProducerLiteMessageExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(ProducerLiteMessageExample).FullName);

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

            const string parentTopic = "topic-lite";
            
            // Create producer with singleton pattern (recommended)
            var producer = await new Producer.Builder()
                .SetTopics(parentTopic)  // Prefetch topic route for better performance
                .SetClientConfig(clientConfig)
                .Build();

            // Define message content
            var body = Encoding.UTF8.GetBytes("This is a lite message for testing");
            const string tag = "LiteTest";
            
            Logger.LogInformation("Sending lite messages with different lite topics...");
            
            // Send messages with different lite topics for dynamic routing
            var liteTopics = new[] { "order-created", "order-updated", "order-completed" };

            foreach (var liteTopic in liteTopics)
            {
                var message = new Message.Builder()
                    .SetTopic(parentTopic)
                    .SetBody(body)
                    .SetTag(tag)
                    .SetKeys($"lite-{liteTopic}")
                    .SetLiteTopic(liteTopic)  // Dynamic topic routing
                    .Build();

                try
                {
                    var sendReceipt = await producer.Send(message);
                    Logger.LogInformation($"Sent lite message successfully, messageId={sendReceipt.MessageId}, liteTopic={liteTopic}");
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Failed to send lite message, liteTopic={liteTopic}");
                }
            }

            Logger.LogInformation("\nAll lite messages sent. Messages will be routed based on their lite topics.");

            // Close the producer if you don't need it anymore.
            await producer.DisposeAsync();
        }
    }
}
