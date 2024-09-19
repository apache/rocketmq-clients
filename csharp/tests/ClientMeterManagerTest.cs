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

using System.Collections.Concurrent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Metric = Org.Apache.Rocketmq.Metric;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ClientMeterManagerTest
    {
        [TestMethod]
        public void TestResetWithMetricOn()
        {
            var meterManager = CreateClientMeterManager();
            var metric = CreateMetric(true);
            meterManager.Reset(metric);
            Assert.IsTrue(meterManager.IsEnabled());
        }

        [TestMethod]
        public void TestResetWithMetricOff()
        {
            var meterManager = CreateClientMeterManager();
            var metric = CreateMetric(false);
            meterManager.Reset(metric);
            Assert.IsFalse(meterManager.IsEnabled());
        }

        private ClientMeterManager CreateClientMeterManager()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:8080")
                .Build();

            return new ClientMeterManager(CreateTestClient(clientConfig));
        }

        private Client CreateTestClient(ClientConfig clientConfig)
        {
            return new PushConsumer(clientConfig, "testGroup",
                new ConcurrentDictionary<string, FilterExpression>(), new TestMessageListener(),
                0, 0, 1);
        }

        private Metric CreateMetric(bool isOn)
        {
            var endpoints = new Proto.Endpoints
            {
                Scheme = Proto.AddressScheme.Ipv4,
                Addresses =
                {
                    new Proto.Address { Host = "127.0.0.1", Port = 8080 }
                }
            };

            return new Metric(new Proto.Metric { On = isOn, Endpoints = endpoints });
        }

        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                return ConsumeResult.SUCCESS;
            }
        }
    }
}