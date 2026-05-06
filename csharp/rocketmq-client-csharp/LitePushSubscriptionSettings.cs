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
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Settings for LitePushConsumer, extends PushSubscriptionSettings with LITE_PUSH_CONSUMER client type.
    /// </summary>
    public class LitePushSubscriptionSettings : PushSubscriptionSettings
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<LitePushSubscriptionSettings>();

        public LitePushSubscriptionSettings(string namespaceName, string clientId, Endpoints endpoints, 
            string consumerGroup, TimeSpan requestTimeout, 
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions)
            : base(namespaceName, clientId, ClientType.LitePushConsumer, endpoints, consumerGroup, requestTimeout, subscriptionExpressions)
        {
            // LitePushConsumer uses LITE_PUSH_CONSUMER client type instead of PUSH_CONSUMER
        }

        public override Proto.Settings ToProtobuf()
        {
            var settings = base.ToProtobuf();
            // Ensure the ClientType is set to LITE_PUSH_CONSUMER
            settings.ClientType = ClientTypeHelper.ToProtobuf(ClientType.LitePushConsumer);
            return settings;
        }
    }
}
