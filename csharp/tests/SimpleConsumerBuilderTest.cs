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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class SimpleConsumerBuilderTest
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetClientConfigurationWithNull()
        {
            var builder = new SimpleConsumer.Builder();
            builder.SetClientConfig(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetConsumerGroupWithNull()
        {
            var builder = new SimpleConsumer.Builder();
            builder.SetConsumerGroup(null);
        }

        [TestMethod]
        public void TestSetAwaitDuration()
        {
            var builder = new SimpleConsumer.Builder();
            builder.SetAwaitDuration(TimeSpan.FromSeconds(5));
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBuildWithEmptyExpressions()
        {
            var builder = new SimpleConsumer.Builder();
            builder.SetSubscriptionExpression(new Dictionary<string, FilterExpression>());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBuildWithoutExpressions()
        {
            var builder = new SimpleConsumer.Builder();
            builder.SetSubscriptionExpression(null);
        }

        [TestMethod]
        public void TestBuild()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:9876").Build();
            var subscription = new Dictionary<string, FilterExpression>
                {{ "testTopic", new FilterExpression("*") }};
            var builder = new SimpleConsumer.Builder();
            builder.SetClientConfig(clientConfig).SetConsumerGroup("testGroup").
                SetSubscriptionExpression(subscription).Build();
        }
    }
}