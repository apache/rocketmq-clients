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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Org.Apache.Rocketmq;
using Metric = Org.Apache.Rocketmq.Metric;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class ClientMeterTest
    {
        private MeterProvider CreateMeterProvider()
        {
            return Sdk.CreateMeterProviderBuilder()
                      .SetResourceBuilder(ResourceBuilder.CreateEmpty())
                      .Build();
        }

        [TestMethod]
        public void TestShutdownWithEnabledMeter()
        {
            var endpoints = new Endpoints(new Proto.Endpoints
            {
                Scheme = Proto.AddressScheme.Ipv4,
                Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
            });
            var provider = CreateMeterProvider();
            var clientId = "testClientId";
            var clientMeter = new ClientMeter(endpoints, provider, clientId);
            Assert.IsTrue(clientMeter.Enabled);
            clientMeter.Shutdown();
        }

        [TestMethod]
        public void TestShutdownWithDisabledMeter()
        {
            var clientId = "testClientId";
            var clientMeter = ClientMeter.DisabledInstance(clientId);
            Assert.IsFalse(clientMeter.Enabled);
            clientMeter.Shutdown();
        }

        [TestMethod]
        public void TestSatisfy()
        {
            var clientId = "testClientId";
            var clientMeter = ClientMeter.DisabledInstance(clientId);

            var metric = new Metric(new Proto.Metric { On = false });
            Assert.IsTrue(clientMeter.Satisfy(metric));

            metric = new Metric(new Proto.Metric { On = true });
            Assert.IsTrue(clientMeter.Satisfy(metric));

            var endpoints0 = new Proto.Endpoints
            {
                Scheme = Proto.AddressScheme.Ipv4,
                Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8080 } }
            };

            metric = new Metric(new Proto.Metric { On = false, Endpoints = endpoints0 });
            Assert.IsTrue(clientMeter.Satisfy(metric));

            metric = new Metric(new Proto.Metric { On = true, Endpoints = endpoints0 });
            Assert.IsFalse(clientMeter.Satisfy(metric));

            var endpoints = new Endpoints(endpoints0);
            var provider = CreateMeterProvider();
            clientMeter = new ClientMeter(endpoints, provider, clientId);

            metric = new Metric(new Proto.Metric { On = false });
            Assert.IsFalse(clientMeter.Satisfy(metric));

            metric = new Metric(new Proto.Metric { On = true });
            Assert.IsFalse(clientMeter.Satisfy(metric));

            metric = new Metric(new Proto.Metric { On = false, Endpoints = endpoints0 });
            Assert.IsFalse(clientMeter.Satisfy(metric));

            metric = new Metric(new Proto.Metric { On = true, Endpoints = endpoints0 });
            Assert.IsTrue(clientMeter.Satisfy(metric));

            var endpoints1 = new Proto.Endpoints
            {
                Scheme = Proto.AddressScheme.Ipv4,
                Addresses = { new Proto.Address { Host = "127.0.0.2", Port = 8081 } }
            };
            metric = new Metric(new Proto.Metric { On = true, Endpoints = endpoints1 });
            Assert.IsFalse(clientMeter.Satisfy(metric));
        }
    }
}