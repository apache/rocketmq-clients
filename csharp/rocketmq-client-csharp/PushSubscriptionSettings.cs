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
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class PushSubscriptionSettings : Settings
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<PushSubscriptionSettings>();

        private readonly Resource _group;
        private readonly ConcurrentDictionary<string, FilterExpression> _subscriptionExpressions;
        private volatile bool _fifo = false;
        private volatile int _receiveBatchSize = 32;
        private TimeSpan _longPollingTimeout = TimeSpan.FromSeconds(30);

        public PushSubscriptionSettings(string namespaceName, string clientId, Endpoints endpoints, string consumerGroup,
            TimeSpan requestTimeout, ConcurrentDictionary<string, FilterExpression> subscriptionExpressions)
            : base(namespaceName, clientId, ClientType.PushConsumer, endpoints, requestTimeout)
        {
            _group = new Resource(namespaceName, consumerGroup);
            _subscriptionExpressions = subscriptionExpressions;
        }

        public bool IsFifo()
        {
            return _fifo;
        }

        public int GetReceiveBatchSize()
        {
            return _receiveBatchSize;
        }

        public TimeSpan GetLongPollingTimeout()
        {
            return _longPollingTimeout;
        }

        public override Proto.Settings ToProtobuf()
        {
            var subscriptionEntries = new List<Proto.SubscriptionEntry>();
            foreach (var (key, value) in _subscriptionExpressions)
            {
                var topic = new Proto.Resource()
                {
                    ResourceNamespace = Namespace,
                    Name = key
                };
                var filterExpression = new Proto.FilterExpression()
                {
                    Expression = value.Expression
                };
                switch (value.Type)
                {
                    case ExpressionType.Tag:
                        filterExpression.Type = Proto.FilterType.Tag;
                        break;
                    case ExpressionType.Sql92:
                        filterExpression.Type = Proto.FilterType.Sql;
                        break;
                    default:
                        Logger.LogWarning($"[Bug] Unrecognized filter type={value.Type} for push consumer");
                        break;
                }

                var subscriptionEntry = new Proto.SubscriptionEntry
                {
                    Topic = topic,
                    Expression = filterExpression
                };

                subscriptionEntries.Add(subscriptionEntry);
            }

            var subscription = new Proto.Subscription
            {
                Group = _group.ToProtobuf(),
                Subscriptions = { subscriptionEntries }
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

        public override void Sync(Proto.Settings settings)
        {
            if (Proto.Settings.PubSubOneofCase.Subscription != settings.PubSubCase)
            {
                Logger.LogError($"[Bug] Issued settings doesn't match with the client type, clientId={ClientId}, " +
                                $"pubSubCase={settings.PubSubCase}, clientType={ClientType}");
            }

            var subscription = settings.Subscription ?? new Proto.Subscription();
            _fifo = subscription.Fifo;
            _receiveBatchSize = subscription.ReceiveBatchSize;
            _longPollingTimeout = subscription.LongPollingTimeout?.ToTimeSpan() ?? TimeSpan.Zero;
            var backoffPolicy = settings.BackoffPolicy ?? new Proto.RetryPolicy();
            switch (backoffPolicy.StrategyCase)
            {
                case Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff:
                    RetryPolicy = ExponentialBackoffRetryPolicy.FromProtobuf(backoffPolicy);
                    break;
                case Proto.RetryPolicy.StrategyOneofCase.CustomizedBackoff:
                    RetryPolicy = CustomizedBackoffRetryPolicy.FromProtobuf(backoffPolicy);
                    break;
                default:
                    throw new ArgumentException("Unrecognized backoff policy strategy.");
            }
        }
    }
}