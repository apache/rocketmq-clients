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
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class PushConsumerBuilderTest
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetClientConfigWithNull()
        {
            var builder = new PushConsumer.Builder();
            builder.SetClientConfig(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetConsumerGroupWithNull()
        {
            var builder = new PushConsumer.Builder();
            builder.SetConsumerGroup(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetConsumerGroupWithSpecialChar()
        {
            var builder = new PushConsumer.Builder();
            builder.SetConsumerGroup("#.testGroup#");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBuildWithoutExpressions()
        {
            var builder = new PushConsumer.Builder();
            builder.SetSubscriptionExpression(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBuildWithEmptyExpressions()
        {
            var builder = new PushConsumer.Builder();
            builder.SetSubscriptionExpression(new Dictionary<string, FilterExpression>());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBuildWithNullMessageListener()
        {
            var builder = new PushConsumer.Builder();
            builder.SetMessageListener(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestNegativeMaxCacheMessageCount()
        {
            var builder = new PushConsumer.Builder();
            builder.SetMaxCacheMessageCount(-1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestNegativeMaxCacheMessageSizeInBytes()
        {
            var builder = new PushConsumer.Builder();
            builder.SetMaxCacheMessageSizeInBytes(-1);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestNegativeConsumptionThreadCount()
        {
            var builder = new PushConsumer.Builder();
            builder.SetMaxCacheMessageCount(-1);
        }

        [TestMethod]
        public void TestBuild()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:9876").Build();
            var subscription = new Dictionary<string, FilterExpression>
                {{ "fakeTopic", new FilterExpression("*") }};
            var builder = new PushConsumer.Builder();
            builder.SetClientConfig(clientConfig).SetSubscriptionExpression(subscription).SetConsumerGroup("testGroup")
                .SetMessageListener(new TestMessageListener()).SetMaxCacheMessageCount(10)
                .SetMaxCacheMessageSizeInBytes(10).SetConsumptionThreadCount(10).Build();
        }

        private class TestMessageListener : IMessageListener
        {
            public ConsumeResult Consume(MessageView messageView)
            {
                // Handle the received message and return consume result.
                return ConsumeResult.SUCCESS;
            }
        }
    }
}