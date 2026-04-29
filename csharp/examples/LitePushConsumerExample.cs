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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Org.Apache.Rocketmq.Examples
{
    /// <summary>
    /// Example demonstrating how to use LitePushConsumer for consuming messages from lite topics.
    /// Lite topics enable dynamic topic routing without pre-defining all topics.
    /// 
    /// Key Points:
    /// - SubscribeLite() adds lite topics dynamically
    /// - UnsubscribeLite() removes lite topics
    /// - GetLiteTopicSet() returns current subscriptions
    /// - BindTopic is the parent topic for all lite topics
    /// </summary>
    internal static class LitePushConsumerExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<LitePushConsumerExample>();

        internal static async Task QuickStart()
        {
            // Configure client for local testing (no authentication required)
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8081")
                .Build();

            // Define bind topic (parent topic) and consumer group
            const string bindTopic = "topic-lite";
            const string consumerGroup = "GID-lite-consumer";

            // Create message listener
            var messageListener = new MessageListener(async messageView =>
            {
                var body = Encoding.UTF8.GetString(messageView.Body.Span);
                Logger.LogInformation($"Received lite message: messageId={messageView.MessageId}, " +
                                    $"topic={messageView.Topic}, liteTopic={messageView.LiteTopic}, " +
                                    $"body={body}");
                return ConsumeResult.SUCCESS;
            });

            // Build lite push consumer
            Logger.LogInformation($"Creating LitePushConsumer, bindTopic={bindTopic}, consumerGroup={consumerGroup}");
            var litePushConsumer = await new LitePushConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetBindTopic(bindTopic)
                .SetMessageListener(messageListener)
                .SetMaxCacheMessageCount(1024)
                .SetMaxCacheMessageSizeInBytes(64 * 1024 * 1024)
                .SetConsumptionThreadCount(20)
                .Build();

            Logger.LogInformation($"LitePushConsumer started successfully, bindTopic={bindTopic}, consumerGroup={consumerGroup}");

            try
            {
                // Subscribe to lite topics dynamically
                var liteTopics = new[] { "order-created", "order-updated", "order-completed" };
                
                Logger.LogInformation($"Subscribing to {liteTopics.Length} lite topics...");
                foreach (var liteTopic in liteTopics)
                {
                    await litePushConsumer.SubscribeLite(liteTopic);
                    Logger.LogInformation($"Subscribed to lite topic: {liteTopic}");
                }

                // Optionally subscribe with offset option
                // await litePushConsumer.SubscribeLite("new-topic", OffsetOption.LastOffset);

                // Get current lite topic set
                var subscribedTopics = litePushConsumer.GetLiteTopicSet();
                Logger.LogInformation($"Currently subscribed to {subscribedTopics.Count} lite topics");

                // Keep the consumer running
                Logger.LogInformation("LitePushConsumer is running. Press any key to exit...");
                Console.ReadKey();

                // Unsubscribe from a lite topic
                await litePushConsumer.UnsubscribeLite("order-completed");
                Logger.LogInformation("Unsubscribed from lite topic: order-completed");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error occurred while running LitePushConsumer");
            }
            finally
            {
                // Shutdown the consumer
                await litePushConsumer.DisposeAsync();
                Logger.LogInformation("LitePushConsumer shutdown completed");
            }
        }
    }
}
