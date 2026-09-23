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
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Settings for LiteSimpleConsumer, reports LITE_SIMPLE_CONSUMER as the client type
    /// so that the server resolves lite topic subscriptions for this client.
    /// </summary>
    public class LiteSimpleSubscriptionSettings : SimpleSubscriptionSettings
    {
        public LiteSimpleSubscriptionSettings(string namespaceName, string clientId, Endpoints endpoints,
            string consumerGroup, TimeSpan requestTimeout, TimeSpan longPollingTimeout,
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions)
            : base(namespaceName, clientId, endpoints, consumerGroup, requestTimeout, longPollingTimeout,
                subscriptionExpressions)
        {
            // LiteSimpleConsumer uses LITE_SIMPLE_CONSUMER client type instead of SIMPLE_CONSUMER
        }

        public override Proto.Settings ToProtobuf()
        {
            var settings = base.ToProtobuf();
            settings.ClientType = ClientTypeHelper.ToProtobuf(ClientType.LiteSimpleConsumer);
            return settings;
        }
    }
}
