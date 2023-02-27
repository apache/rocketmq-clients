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
    public class ProducerBenchmark
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private static readonly SemaphoreSlim Semaphore = new(0);
        private static long _counter = 0;

        internal static void QuickStart()
        {
            const string accessKey = "amKhwEM40L61znSz";
            const string secretKey = "bT6c3gpF3EFB10F3";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticCredentialsProvider(accessKey, secretKey);
            const string endpoints = "rmq-cn-nwy337bf81g.cn-hangzhou.rmq.aliyuncs.com:8080";
            var clientConfig = new ClientConfig(endpoints)
            {
                CredentialsProvider = credentialsProvider
            };
            // In most case, you don't need to create too many producers, single pattern is recommended.
            var producer = new Producer(clientConfig);

            const string topic = "lingchu_normal_topic";
            producer.SetTopics(topic);
            // Set the topic name(s), which is optional but recommended. It makes producer could prefetch
            // the topic route before message publishing.
            producer.Start().Wait();
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

            const int tpsLimit = 1;

            Task.Run(async () =>
            {
                while (true)
                {
                    Semaphore.Release(tpsLimit);
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

            var tasks = new List<Task>();
            while (true)
            {
                Semaphore.Wait();
                Interlocked.Increment(ref _counter);
                var task = producer.Send(message);
                tasks.Add(task);
            }

            Task.WhenAll(tasks).Wait();
        }
    }
}