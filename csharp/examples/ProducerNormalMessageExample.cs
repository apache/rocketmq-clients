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

using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    internal static class ProducerNormalMessageExample
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        internal static async Task QuickStart()
        {
            const string accessKey = "yourAccessKey";
            const string secretKey = "yourSecretKey";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticCredentialsProvider(accessKey, secretKey);
            const string endpoints = "foobar.com:8080";
            var clientConfig = new ClientConfig(endpoints)
            {
                CredentialsProvider = credentialsProvider
            };
            // In most case, you don't need to create too many producers, single pattern is recommended.
            var producer = new Producer(clientConfig);

            const string topic = "yourNormalTopic";
            producer.SetTopics(topic);
            // Set the topic name(s), which is optional but recommended. It makes producer could prefetch
            // the topic route before message publishing.
            await producer.Start();
            // Define your message body.
            var bytes = Encoding.UTF8.GetBytes("foobar");
            const string tag = "yourMessageTagA";
            // You could set multiple keys for the single message.
            var keys = new List<string>
            {
                "yourMessageKey-7044358f98fc",
                "yourMessageKey-f72539fbc246"
            };
            // Set topic for current message.
            var message = new Message(topic, bytes)
            {
                Tag = tag,
                Keys = keys
            };
            var sendReceipt = await producer.Send(message);
            Logger.Info($"Send message successfully, sendReceipt={sendReceipt}");
            Thread.Sleep(9999999);
            // Close the producer if you don't need it anymore.
            await producer.Shutdown();
        }
    }
}