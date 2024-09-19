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
using Castle.Core.Internal;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class PushSubscriptionSettingsTest
    {
        private const string Namespace = "testNamespace";
        private const string GroupResource = "testConsumerGroup";
        private const string ClientId = "testClientId";
        private const string Endpoint = "127.0.0.1:8080";
        private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(3);
        private static readonly ConcurrentDictionary<string, FilterExpression> SubscriptionExpression =
            new ConcurrentDictionary<string, FilterExpression>(new Dictionary<string, FilterExpression> { { "testTopic", new FilterExpression("*") } });

        private PushSubscriptionSettings CreateSettings()
        {
            return new PushSubscriptionSettings(Namespace, ClientId, new Endpoints(Endpoint), GroupResource, RequestTimeout, SubscriptionExpression);
        }

        [TestMethod]
        public void TestToProtobuf()
        {
            var pushSubscriptionSettings = CreateSettings();
            var settings = pushSubscriptionSettings.ToProtobuf();

            Assert.AreEqual(Proto.ClientType.PushConsumer, settings.ClientType);
            Assert.AreEqual(Duration.FromTimeSpan(RequestTimeout), settings.RequestTimeout);
            Assert.IsFalse(settings.Subscription.Subscriptions.Count == 0);

            var subscription = settings.Subscription;
            Assert.AreEqual(subscription.Group, new Proto.Resource
            {
                ResourceNamespace = Namespace,
                Name = GroupResource
            });

            Assert.IsFalse(subscription.Fifo);

            var subscriptionsList = subscription.Subscriptions;
            Assert.AreEqual(1, subscriptionsList.Count);

            var subscriptionEntry = subscriptionsList[0];
            Assert.AreEqual(Proto.FilterType.Tag, subscriptionEntry.Expression.Type);
            Assert.AreEqual(subscriptionEntry.Topic, new Proto.Resource
            {
                ResourceNamespace = Namespace,
                Name = "testTopic"
            });
        }

        [TestMethod]
        public void TestSync()
        {
            var durations = new List<Duration>
        {
            Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
            Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
            Duration.FromTimeSpan(TimeSpan.FromSeconds(3))
        };

            var customizedBackoff = new Proto.CustomizedBackoff
            {
                Next = { durations }
            };

            var retryPolicy = new Proto.RetryPolicy
            {
                CustomizedBackoff = customizedBackoff,
                MaxAttempts = 3
            };

            var subscription = new Proto.Subscription
            {
                Fifo = true,
                ReceiveBatchSize = 96,
                LongPollingTimeout = Duration.FromTimeSpan(TimeSpan.FromSeconds(60))
            };

            var settings = new Proto.Settings
            {
                Subscription = subscription,
                BackoffPolicy = retryPolicy
            };

            var pushSubscriptionSettings = new PushSubscriptionSettings(
                "fakeNamespace", ClientId, new Endpoints(Endpoint), GroupResource, RequestTimeout,
                new ConcurrentDictionary<string, FilterExpression>(SubscriptionExpression));

            pushSubscriptionSettings.Sync(settings);
        }
    }

}