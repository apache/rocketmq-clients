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
using Google.Protobuf.WellKnownTypes;
using NLog;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class SimpleSubscriptionSettings : Settings
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private readonly Resource _group;
        private readonly TimeSpan _longPollingTimeout;
        private readonly ConcurrentDictionary<string /* topic */, FilterExpression> _subscriptionExpressions;

        public SimpleSubscriptionSettings(string clientId, Endpoints endpoints, string consumerGroup,
            TimeSpan requestTimeout, TimeSpan longPollingTimeout,
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions) : base(
            clientId, ClientType.SimpleConsumer, endpoints, requestTimeout)
        {
            _group = new Resource(consumerGroup);
            _longPollingTimeout = longPollingTimeout;
            _subscriptionExpressions = subscriptionExpressions;
        }

        public override void Sync(Proto::Settings settings)
        {
            if (Proto.Settings.PubSubOneofCase.Subscription != settings.PubSubCase)
            {
                Logger.Error($"[Bug] Issued settings doesn't match with the client type, clientId={ClientId}, " +
                             $"pubSubCase={settings.PubSubCase}, clientType={ClientType}");
            }
        }

        public override Proto.Settings ToProtobuf()
        {
            var subscriptionEntries = new List<Proto.SubscriptionEntry>();
            foreach (var (key, value) in _subscriptionExpressions)
            {
                var topic = new Proto.Resource()
                {
                    Name = key,
                };
                var subscriptionEntry = new Proto.SubscriptionEntry();
                var filterExpression = new Proto.FilterExpression();
                switch (value.Type)
                {
                    case ExpressionType.Tag:
                        filterExpression.Type = Proto.FilterType.Tag;
                        break;
                    case ExpressionType.Sql92:
                        filterExpression.Type = Proto.FilterType.Sql;
                        break;
                    default:
                        Logger.Warn($"[Bug] Unrecognized filter type={value.Type} for simple consumer");
                        break;
                }

                filterExpression.Expression = value.Expression;
                subscriptionEntry.Topic = topic;
                subscriptionEntries.Add(subscriptionEntry);
            }

            var subscription = new Proto.Subscription
            {
                Group = _group.ToProtobuf(),
                Subscriptions = { subscriptionEntries },
                LongPollingTimeout = Duration.FromTimeSpan(_longPollingTimeout)
            };
            return new Proto.Settings
            {
                AccessPoint = Endpoints.ToProtobuf(),
                ClientType = ClientTypeHelper.ToProtobuf(ClientType),
                RequestTimeout = Duration.FromTimeSpan(RequestTimeout),
                Subscription = subscription,
                UserAgent = UserAgent.Instance.ToProtobuf()
            };
        }
    }
}