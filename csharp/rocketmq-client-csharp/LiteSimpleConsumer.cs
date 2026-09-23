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
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Lite simple consumer extends the standard simple consumer with lite topic support.
    /// It binds to one parent topic and (un)subscribes lite topics at runtime, then pulls
    /// and acks messages from them explicitly.
    /// </summary>
    public class LiteSimpleConsumer : SimpleConsumer, ILiteSimpleConsumer
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<LiteSimpleConsumer>();

        private readonly LiteSubscriptionManager _liteSubscriptionManager;
        private readonly string _bindTopic;
        private readonly LiteSimpleSubscriptionSettings _liteSimpleSubscriptionSettings;

        /// <summary>
        /// Creates a new instance of LiteSimpleConsumer.
        /// </summary>
        /// <param name="clientConfig">Client configuration</param>
        /// <param name="consumerGroup">Consumer group name</param>
        /// <param name="awaitDuration">Long-polling timeout of receive requests</param>
        /// <param name="bindTopic">The parent topic all lite topics belong to</param>
        public LiteSimpleConsumer(ClientConfig clientConfig, string consumerGroup, TimeSpan awaitDuration,
            string bindTopic)
            : this(clientConfig, consumerGroup, awaitDuration, bindTopic,
                new ConcurrentDictionary<string, FilterExpression> { [bindTopic] = FilterExpression.SubAll })
        {
        }

        private LiteSimpleConsumer(ClientConfig clientConfig, string consumerGroup, TimeSpan awaitDuration,
            string bindTopic, ConcurrentDictionary<string, FilterExpression> subscriptionExpressions)
            : base(clientConfig, consumerGroup, awaitDuration, subscriptionExpressions.ToDictionary(kv => kv.Key, kv => kv.Value))
        {
            if (string.IsNullOrWhiteSpace(bindTopic))
            {
                throw new ArgumentException("bindTopic cannot be null or empty", nameof(bindTopic));
            }

            _bindTopic = bindTopic;
            _liteSubscriptionManager = new LiteSubscriptionManager(this, bindTopic, consumerGroup);
            _liteSimpleSubscriptionSettings = new LiteSimpleSubscriptionSettings(clientConfig.Namespace, ClientId,
                Endpoints, consumerGroup, clientConfig.RequestTimeout, awaitDuration, subscriptionExpressions);
        }

        protected override async Task Start()
        {
            await base.Start();

            // Fetch the route of the bind topic up-front, the lite subscription sync and
            // the receive requests both need it.
            try
            {
                var routeData = await GetRouteData(_bindTopic);
                Logger.LogInformation($"Fetched route for bind topic: {_bindTopic}, " +
                                      $"messageQueueCount={routeData.MessageQueues.Count}, clientId={ClientId}");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Failed to fetch route for bind topic: {_bindTopic}, clientId={ClientId}. " +
                                    "Lite subscription sync may be affected.");
            }

            _liteSubscriptionManager.Start();
            Logger.LogInformation($"LiteSimpleConsumer started, clientId={ClientId}, bindTopic={_bindTopic}, " +
                                  $"consumerGroup={ConsumerGroup}");
        }

        protected override async Task Shutdown()
        {
            _liteSubscriptionManager.Shutdown();
            await base.Shutdown();
        }

        /// <summary>
        /// Subscribe to a lite topic.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        public async Task SubscribeLite(string liteTopic)
        {
            await _liteSubscriptionManager.SubscribeLite(liteTopic, null);
        }

        /// <summary>
        /// Subscribe to a lite topic with an offsetOption to specify the consume from offset.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        /// <param name="offsetOption">The consume from offset option. If null, uses the default offset policy.</param>
        public async Task SubscribeLite(string liteTopic, OffsetOption offsetOption)
        {
            await _liteSubscriptionManager.SubscribeLite(liteTopic, offsetOption);
        }

        /// <summary>
        /// Unsubscribe from a lite topic.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to unsubscribe from</param>
        public async Task UnsubscribeLite(string liteTopic)
        {
            await _liteSubscriptionManager.UnsubscribeLite(liteTopic);
        }

        /// <summary>
        /// Get the lite topic immutable set.
        /// </summary>
        /// <returns>Lite topic immutable set</returns>
        public ISet<string> GetLiteTopicSet()
        {
            return _liteSubscriptionManager.GetLiteTopicSet();
        }

        /// <summary>
        /// Get the parent topic this consumer binds to.
        /// </summary>
        /// <returns>The bound parent topic</returns>
        public string GetBindTopic()
        {
            return _bindTopic;
        }

        internal override void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
        {
            base.OnSettingsCommand(endpoints, settings);
            _liteSubscriptionManager.Sync(settings);
        }

        internal override void OnNotifyUnsubscribeLiteCommand(Endpoints endpoints, Proto.NotifyUnsubscribeLiteCommand command)
        {
            _liteSubscriptionManager.OnNotifyUnsubscribeLiteCommand(command);
        }

        internal override Proto.HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto.HeartbeatRequest
            {
                ClientType = Proto.ClientType.LiteSimpleConsumer,
                Group = new Proto.Resource
                {
                    ResourceNamespace = ClientConfig.Namespace,
                    Name = ConsumerGroup
                }
            };
        }

        internal override Settings GetSettings()
        {
            return _liteSimpleSubscriptionSettings;
        }

        protected override ClientType GetClientType()
        {
            return ClientType.LiteSimpleConsumer;
        }

        /// <summary>
        /// Lite consumers only need routes to brokers, so keep the first readable master queue.
        /// </summary>
        internal override SubscriptionLoadBalancer UpdateSubscriptionLoadBalancer(string topic, TopicRouteData topicRouteData)
        {
            var firstReadableMasterQueue = topicRouteData.MessageQueues.FirstOrDefault(mq =>
                Utilities.MasterBrokerId == mq.Broker.Id && Permission.None != mq.Permission &&
                (Permission.Read == mq.Permission || Permission.ReadWrite == mq.Permission));

            var liteTopicRouteData = new TopicRouteData(null == firstReadableMasterQueue
                ? new List<Proto.MessageQueue>()
                : new List<Proto.MessageQueue> { firstReadableMasterQueue.ToProtobuf() });

            return base.UpdateSubscriptionLoadBalancer(topic, liteTopicRouteData);
        }

        /// <summary>
        /// Lite consumers must report the lite topic in ack requests, otherwise the server
        /// cannot resolve the receipt handle back to the parent topic.
        /// </summary>
        internal override Proto.AckMessageRequest WrapAckMessageRequest(MessageView messageView)
        {
            var request = base.WrapAckMessageRequest(messageView);
            if (!string.IsNullOrEmpty(messageView.LiteTopic) && request.Entries.Count > 0)
            {
                request.Entries[0].LiteTopic = messageView.LiteTopic;
            }
            return request;
        }

        /// <summary>
        /// Lite consumers must report the lite topic when changing the invisible duration.
        /// </summary>
        internal override Proto.ChangeInvisibleDurationRequest WrapChangeInvisibleDuration(MessageView messageView,
            TimeSpan invisibleDuration)
        {
            var request = base.WrapChangeInvisibleDuration(messageView, invisibleDuration);
            if (!string.IsNullOrEmpty(messageView.LiteTopic))
            {
                request.LiteTopic = messageView.LiteTopic;
            }
            return request;
        }

        /// <summary>
        /// Builder for creating LiteSimpleConsumer instances.
        /// </summary>
        public new class Builder
        {
            private ClientConfig _clientConfig;
            private string _consumerGroup;
            private string _bindTopic;
            private TimeSpan _awaitDuration = TimeSpan.FromSeconds(30);

            public Builder SetClientConfig(ClientConfig clientConfig)
            {
                Preconditions.CheckArgument(clientConfig != null, "clientConfig should not be null");
                _clientConfig = clientConfig;
                return this;
            }

            public Builder SetConsumerGroup(string consumerGroup)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(consumerGroup), "consumerGroup should not be null or empty");
                Preconditions.CheckArgument(ConsumerGroupRegex.Match(consumerGroup).Success,
                    $"consumerGroup does not match the regex {ConsumerGroupRegex}");
                _consumerGroup = consumerGroup;
                return this;
            }

            public Builder SetBindTopic(string bindTopic)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(bindTopic), "bindTopic should not be null or empty");
                _bindTopic = bindTopic;
                return this;
            }

            public Builder SetAwaitDuration(TimeSpan awaitDuration)
            {
                Preconditions.CheckArgument(awaitDuration > TimeSpan.Zero, "awaitDuration should be positive");
                _awaitDuration = awaitDuration;
                return this;
            }

            public async Task<LiteSimpleConsumer> Build()
            {
                Preconditions.CheckArgument(_clientConfig != null, "clientConfig has not been set yet");
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(_consumerGroup), "consumerGroup has not been set yet");
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(_bindTopic), "bindTopic has not been set yet");

                var liteSimpleConsumer = new LiteSimpleConsumer(_clientConfig, _consumerGroup, _awaitDuration, _bindTopic);
                await liteSimpleConsumer.Start();
                return liteSimpleConsumer;
            }
        }
    }
}
