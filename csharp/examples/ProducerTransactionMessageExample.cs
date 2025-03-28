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
    internal static class ProducerTransactionMessageExample
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger(typeof(ProducerTransactionMessageExample).FullName);

        private static readonly string AccessKey = Environment.GetEnvironmentVariable("ROCKETMQ_ACCESS_KEY");
        private static readonly string SecretKey = Environment.GetEnvironmentVariable("ROCKETMQ_SECRET_KEY");
        private static readonly string Endpoint = Environment.GetEnvironmentVariable("ROCKETMQ_ENDPOINT");

        private class TransactionChecker : ITransactionChecker
        {
            public TransactionResolution Check(MessageView messageView)
            {
                Logger.LogInformation("Receive transaction check, messageId={}", messageView.MessageId);
                return TransactionResolution.Commit;
            }
        }

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

            const string topic = "yourTransactionTopic";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker())
                .Build();

            var transaction = producer.BeginTransaction();
            // Define your message body.
            var bytes = Encoding.UTF8.GetBytes("foobar");
            const string tag = "yourMessageTagA";
            var message = new Message.Builder()
                .SetTopic(topic)
                .SetBody(bytes)
                .SetTag(tag)
                // You could set multiple keys for the single message actually.
                .SetKeys("yourMessageKey-7044358f98fc")
                .Build();

            var sendReceipt = await producer.Send(message, transaction);
            Logger.LogInformation("Send transaction message successfully, messageId={}", sendReceipt.MessageId);
            // Commit the transaction.
            await transaction.Commit();
            // Or rollback the transaction.
            // await transaction.Rollback();

            // Close the producer if you don't need it anymore.
            await producer.DisposeAsync();
        }
    }
}