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
using System.Collections.Concurrent;
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

        private static readonly SemaphoreSlim Semaphore = new SemaphoreSlim(0);
        private const int TpsLimit = 300;
        private static long _successCounter;
        private static long _failureCounter;

        private static readonly BlockingCollection<Task<ISendReceipt>> Tasks =
            new BlockingCollection<Task<ISendReceipt>>();

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
                    Logger.Info($"{Interlocked.Exchange(ref _successCounter, 0)} success, " +
                                $"{Interlocked.Exchange(ref _failureCounter, 0)} failure.");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            });

            Task.Run(async () =>
            {
                foreach (var task in Tasks.GetConsumingEnumerable())
                {
                    try
                    {
                        await task;
                        Interlocked.Increment(ref _successCounter);
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref _failureCounter);
                    }
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
            var producer = await new Producer.Builder()
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
            while (true)
            {
                await Semaphore.WaitAsync();
                var task = producer.Send(message);
                Tasks.Add(task);
            }
        }
    }
}