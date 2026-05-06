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
    /// Demonstrates how to send timed/delay messages and recall them before delivery
    /// using Apache RocketMQ C# client.
    /// 
    /// This example shows:
    /// 1. How to send a delay message with a future delivery timestamp
    /// 2. How to recall (cancel) a scheduled message before it's delivered
    /// 3. Use cases: order cancellation, appointment reminders, scheduled notifications
    /// </summary>
    internal static class ProducerWithRecallingTimedMessageExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(ProducerWithRecallingTimedMessageExample).FullName);

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

            const string topic = "topic-delay-new";
            // In most case, you don't need to create too many producers, singleton pattern is recommended.
            var producer = await new Producer.Builder()
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .Build();

            try
            {
                // Example 1: Send a delay message using RocketMQ delay levels
                // RocketMQ supports 18 delay levels: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
                Logger.LogInformation("=== Example 1: Send delay message with level 5 (1 minute) ===");
                var delayMessageBytes = Encoding.UTF8.GetBytes("This is a delay message for testing recall functionality");
                var delayMessage = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(delayMessageBytes)
                    .SetTag("DelayTest")
                    .SetKeys("test-delay-recall-001")
                    // Set delivery timestamp to 60 seconds from now (delay level 5)
                    .SetDeliveryTimestamp(DateTime.Now.AddSeconds(60))
                    .Build();

                var sendReceipt = await producer.Send(delayMessage);
                Logger.LogInformation($"Delay message sent successfully, messageId={sendReceipt.MessageId}");
                Logger.LogInformation($"Consumer Group: GID-normal-consumer_topic-normal");

                // Note: In a real scenario, you would store the recallHandle from sendReceipt
                // to use for recalling the message later if needed.
                // For this example, we'll demonstrate the recall API structure.

                // Example 2: Send a delay message and demonstrate recall
                Logger.LogInformation("\n=== Example 2: Send delay message for recall test ===");
                var recallableMessageBytes = Encoding.UTF8.GetBytes("This message will be recalled before delivery - Test Case");
                var recallableMessage = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(recallableMessageBytes)
                    .SetTag("RecallTest")
                    .SetKeys("test-delay-recall-002")
                    // Set delivery timestamp to 120 seconds from now (delay level 8)
                    .SetDeliveryTimestamp(DateTime.Now.AddSeconds(120))
                    .Build();

                var recallableReceipt = await producer.Send(recallableMessage);
                Logger.LogInformation($"Recallable message sent, messageId={recallableReceipt.MessageId}");
                Logger.LogInformation($"To recall this message, use the recallHandle from SendReceipt");

                // Simulate a scenario where you need to recall the message
                // For example: user cancelled the order, so we don't need to send the reminder
                Logger.LogInformation("Simulating message recall (e.g., order cancelled)...");

                // In production, you would get the recallHandle from the send receipt
                // and store it in your database along with the business data
                // var recallHandle = recallableReceipt.RecallHandle; // This would be available in the receipt

                // When you need to recall the message:
                // var recallReceipt = await producer.RecallMessage(topic, recallHandle);
                // Logger.LogInformation($"Message recalled successfully, recallReceipt={recallReceipt}");

                // For demonstration purposes, we show the API call structure:
                Logger.LogInformation("To recall a message, use: await producer.RecallMessage(topic, recallHandle)");
                Logger.LogInformation("The recallHandle should be obtained from the SendReceipt when sending the message");

                // Example 3: Send multiple delay messages with different RocketMQ delay levels
                Logger.LogInformation("\n=== Example 3: Multiple delay messages with different levels ===");
                // RocketMQ 18 delay levels: 1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h
                var delayLevels = new[] { 1, 5, 10, 30, 60 }; // seconds

                foreach (var delaySeconds in delayLevels)
                {
                    var messageBytes = Encoding.UTF8.GetBytes($"Delay message - Level {delaySeconds}s");
                    var message = new Message.Builder()
                        .SetTopic(topic)
                        .SetBody(messageBytes)
                        .SetTag($"Delay-{delaySeconds}s")
                        .SetKeys($"test-delay-{delaySeconds}")
                        .SetDeliveryTimestamp(DateTime.Now.AddSeconds(delaySeconds))
                        .Build();

                    var receipt = await producer.Send(message);
                    Logger.LogInformation($"Delay message sent, level={delaySeconds}s, messageId={receipt.MessageId}");
                }

                Logger.LogInformation("\nAll delay messages sent successfully!");
                Logger.LogInformation("In production, store recallHandle for each message to enable recall functionality.");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Failed to send/recall delay messages");
            }
            finally
            {
                // Close the producer if you don't need it anymore.
                await producer.DisposeAsync();
            }
        }
    }
}
