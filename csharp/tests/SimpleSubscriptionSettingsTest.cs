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
using Apache.Rocketmq.V2;
using Castle.Core.Internal;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using FilterExpression = Org.Apache.Rocketmq.FilterExpression;

namespace tests
{
    [TestClass]
    public class SimpleSubscriptionSettingsTest
    {
        private const string TestNamespace = "testNamespace";
        private const string GroupResource = "testConsumerGroup";
        private const string ClientId = "testClientId";
        private const string TestTopic = "testTopic";
        private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan LongPollingTimeout = TimeSpan.FromSeconds(15);
        private SimpleSubscriptionSettings _simpleSubscriptionSettings;

        [TestInitialize]
        public void Setup()
        {
            var subscriptionExpression = new ConcurrentDictionary<string, FilterExpression>(
                new Dictionary<string, FilterExpression> { { TestTopic, new FilterExpression("*") } });
            _simpleSubscriptionSettings = new SimpleSubscriptionSettings(
                TestNamespace,
                ClientId,
                new Endpoints("127.0.0.1:9876"),
                GroupResource,
                RequestTimeout,
                LongPollingTimeout,
                subscriptionExpression
            );
        }

        [TestMethod]
        public void TestToProtobuf()
        {
            var settings = _simpleSubscriptionSettings.ToProtobuf();

            Assert.AreEqual(Proto.ClientType.SimpleConsumer, settings.ClientType);
            Assert.AreEqual(Duration.FromTimeSpan(RequestTimeout), settings.RequestTimeout);
            Assert.IsFalse(settings.Subscription.Subscriptions.Count == 0);

            var subscription = settings.Subscription;

            Assert.AreEqual(subscription.Group, new Proto.Resource
            {
                ResourceNamespace = TestNamespace,
                Name = GroupResource
            });
            Assert.IsFalse(subscription.Fifo);
            Assert.AreEqual(Duration.FromTimeSpan(LongPollingTimeout), subscription.LongPollingTimeout);

            var subscriptionsList = subscription.Subscriptions;
            Assert.AreEqual(1, subscriptionsList.Count);

            var subscriptionEntry = subscriptionsList[0];
            Assert.AreEqual(FilterType.Tag, subscriptionEntry.Expression.Type);
            Assert.AreEqual(subscriptionEntry.Topic, new Proto.Resource
            {
                ResourceNamespace = TestNamespace,
                Name = TestTopic
            });
        }

        [TestMethod]
        public void TestSync()
        {
            var subscription = new Proto.Subscription
            {
                Fifo = true
            };

            var settings = new Proto.Settings
            {
                Subscription = subscription
            };

            _simpleSubscriptionSettings.Sync(settings);
        }
    }
}