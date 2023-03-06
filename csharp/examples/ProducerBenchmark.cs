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
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    public static class ProducerBenchmark
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private static readonly SemaphoreSlim Semaphore = new(0);
        private const int TpsLimit = 1;
        private static long _counter = 0;

        private static void DoStats()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    Semaphore.Release(TpsLimit);
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            });

            Task.Run(async () =>
            {
                while (true)
                {
                    Logger.Info($"Send {Interlocked.Exchange(ref _counter, 0)} messages successfully.");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            });
        }

        internal static async Task QuickStart()
        {
            const string accessKey = "yourAccessKey";
            const string secretKey = "yourSecretKey";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            const string endpoints = "foobar.com:8080";
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            const string topic = "yourNormalTopic";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .Build();

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

            DoStats();
            var tasks = new List<Task>();
            while (true)
            {
                await Semaphore.WaitAsync();
                Interlocked.Increment(ref _counter);
                var task = producer.Send(message);
                tasks.Add(task);
            }

            Task.WhenAll(tasks).Wait();
        }
    }
}