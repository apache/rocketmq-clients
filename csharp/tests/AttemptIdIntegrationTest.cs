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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using FilterExpression = Org.Apache.Rocketmq.FilterExpression;

namespace tests
{
    [TestClass]
    public class AttemptIdIntegrationTest : GrpcServerIntegrationTest
    {
        private const string Topic = "topic";
        private const string Broker = "broker";

        private Server _server;
        private readonly List<string> _attemptIdList = new ConcurrentBag<string>().ToList();

        [TestInitialize]
        public void SetUp()
        {
            var mockServer = new MockServer(Topic, Broker, _attemptIdList);
            _server = SetUpServer(mockServer);
            mockServer.Port = Port;
        }

        [TestCleanup]
        public void TearDown()
        {
            _server.ShutdownAsync();
        }

        [TestMethod]
        public async Task Test()
        {
            var endpoint = "127.0.0.1" + ":" + Port;
            var credentialsProvider = new StaticSessionCredentialsProvider("yourAccessKey", "yourSecretKey");
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoint)
                .SetCredentialsProvider(credentialsProvider)
                .EnableSsl(false)
                .SetRequestTimeout(TimeSpan.FromMilliseconds(1000))
                .Build();

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

            await Task.Run(async () =>
            {
                await WaitForConditionAsync(() =>
                {
                    Assert.IsTrue(_attemptIdList.Count >= 3);
                    Assert.AreEqual(_attemptIdList[0], _attemptIdList[1]);
                    Assert.AreNotEqual(_attemptIdList[0], _attemptIdList[2]);
                }, TimeSpan.FromSeconds(5));
            });
        }

        private async Task WaitForConditionAsync(Action assertCondition, TimeSpan timeout)
        {
            var startTime = DateTime.UtcNow;
            while (DateTime.UtcNow - startTime < timeout)
            {
                try
                {
                    assertCondition();
                    return; // Condition met, exit the method
                }
                catch
                {
                    // Condition not met, ignore exception and try again after a delay
                }

                await Task.Delay(100); // Small delay to avoid tight loop
            }

            // Perform last check to throw the exception
            assertCondition();
        }

        private class CustomMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}