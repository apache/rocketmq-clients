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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq;

namespace examples
{
    public class PushConsumerExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(PushConsumerExample).FullName);

        private static readonly string AccessKey = Environment.GetEnvironmentVariable("ROCKETMQ_ACCESS_KEY");
        private static readonly string SecretKey = Environment.GetEnvironmentVariable("ROCKETMQ_SECRET_KEY");
        private static readonly string Endpoint = Environment.GetEnvironmentVariable("ROCKETMQ_ENDPOINT");

        internal static async Task QuickStart()
        {
            // Enable the switch if you use .NET Core 3.1 and want to disable TLS/SSL.
            // AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticSessionCredentialsProvider(AccessKey, SecretKey);
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(Endpoint)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            // Add your subscriptions.
            const string consumerGroup = "yourConsumerGroup";
            const string topic = "yourTopic";
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression("*") } };

            var pushConsumer = await new PushConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetSubscriptionExpression(subscription)
                .SetMessageListener(new CustomMessageListener())
                .Build();

            Thread.Sleep(Timeout.Infinite);

            // Close the push consumer if you don't need it anymore.
            // await pushConsumer.DisposeAsync();
        }

        private class CustomMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                // Handle the received message and return consume result.
                Logger.LogInformation($"Consume message={messageView}");
                return ConsumeResult.SUCCESS;
            }
        }
    }
}