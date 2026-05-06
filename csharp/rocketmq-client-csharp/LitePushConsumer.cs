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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Lite push consumer extends standard push consumer with lite topic support.
    /// Lite topics allow dynamic topic routing without pre-defining all topics.
    /// </summary>
    public class LitePushConsumer : PushConsumer, ILitePushConsumer
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<LitePushConsumer>();

        private readonly LiteSubscriptionManager _liteSubscriptionManager;
        private readonly string _bindTopic;

        /// <summary>
        /// Creates a new instance of LitePushConsumer.
        /// </summary>
        /// <param name="clientConfig">Client configuration</param>
        /// <param name="consumerGroup">Consumer group name</param>
        /// <param name="bindTopic">The bind topic for lite subscriptions</param>
        /// <param name="subscriptionExpressions">Initial subscription expressions (can be empty for lite consumer)</param>
        /// <param name="messageListener">Message listener callback</param>
        /// <param name="maxCacheMessageCount">Maximum cache message count</param>
        /// <param name="maxCacheMessageSizeInBytes">Maximum cache message size in bytes</param>
        /// <param name="consumptionThreadCount">Number of consumption threads</param>
        /// <param name="enableFifoConsumeAccelerator">Enable FIFO consume accelerator</param>
        public LitePushConsumer(
            ClientConfig clientConfig,
            string consumerGroup,
            string bindTopic,
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions,
            IMessageListener messageListener,
            int maxCacheMessageCount,
            int maxCacheMessageSizeInBytes,
            int consumptionThreadCount,
            bool enableFifoConsumeAccelerator = false)
            : base(clientConfig, consumerGroup, subscriptionExpressions, messageListener,
                   maxCacheMessageCount, maxCacheMessageSizeInBytes, consumptionThreadCount,
                   enableFifoConsumeAccelerator)
        {
            if (string.IsNullOrWhiteSpace(bindTopic))
            {
                throw new ArgumentException("bindTopic cannot be null or empty", nameof(bindTopic));
            }

            _bindTopic = bindTopic;
            _liteSubscriptionManager = new LiteSubscriptionManager(this, bindTopic, consumerGroup);
        }

        protected override async Task Start()
        {
            await base.Start();
            
            // Manually fetch route for bind topic to ensure it's available before lite subscription sync
            // This triggers infrastructure initialization (Telemetry Session, routing, etc.)
            try
            {
                Logger.LogInformation($"Fetching route for bind topic: {_bindTopic}, clientId={ClientId}");
                var routeData = await GetRouteData(_bindTopic);
                Logger.LogInformation($"Successfully fetched route for bind topic: {_bindTopic}, " +
                                    $"messageQueueCount={routeData.MessageQueues.Count}, clientId={ClientId}");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Failed to fetch route for bind topic: {_bindTopic}, clientId={ClientId}. " +
                                  $"Lite subscription sync may be affected.");
                // Don't throw here, allow the consumer to start anyway
            }
            
            // Start LiteSubscriptionManager which will:
            // 1. Perform initial sync of all lite subscriptions (even if empty)
            // 2. Schedule periodic sync every 30 seconds
            _liteSubscriptionManager.Start();
            
            Logger.LogInformation($"LitePushConsumer started successfully, clientId={ClientId}, bindTopic={_bindTopic}, consumerGroup={ConsumerGroup}");
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
        /// Subscribe to a lite topic with offsetOption to specify the consume from offset.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        /// <param name="offsetOption">The consume from offset option. If null, uses default offset policy.</param>
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
        /// Get the load balancing group for the consumer.
        /// </summary>
        /// <returns>Consumer load balancing group</returns>
        public new string GetConsumerGroup()
        {
            return ConsumerGroup;
        }

        /// <summary>
        /// Handle settings command from server, sync lite subscription quota.
        /// </summary>
        internal override void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
        {
            base.OnSettingsCommand(endpoints, settings);
            _liteSubscriptionManager.Sync(settings);
        }

        /// <summary>
        /// Handle notify unsubscribe lite command from server.
        /// </summary>
        internal override void OnNotifyUnsubscribeLiteCommand(Endpoints endpoints, Proto.NotifyUnsubscribeLiteCommand command)
        {
            _liteSubscriptionManager.OnNotifyUnsubscribeLiteCommand(command);
        }

        /// <summary>
        /// Wrap heartbeat request with LITE_PUSH_CONSUMER client type.
        /// </summary>
        internal override Proto.HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto.HeartbeatRequest
            {
                ClientType = Proto.ClientType.LitePushConsumer,
                Group = GetProtobufGroup()
            };
        }

        /// <summary>
        /// Get settings with LITE_PUSH_CONSUMER client type.
        /// </summary>
        internal override Settings GetSettings()
        {
            // Create LitePushSubscriptionSettings with correct ClientType
            var clientConfig = GetClientConfig();
            var liteSettings = new LitePushSubscriptionSettings(
                clientConfig.Namespace,
                ClientId,
                Endpoints,
                ConsumerGroup,
                clientConfig.RequestTimeout,
                GetSubscriptionExpressions());
            return liteSettings;
        }

        /// <summary>
        /// Get client type for this lite push consumer.
        /// </summary>
        /// <returns>The client type (LITE_PUSH_CONSUMER).</returns>
        protected override ClientType GetClientType()
        {
            return ClientType.LitePushConsumer;
        }

        /// <summary>
        /// Get the bind topic for lite subscriptions.
        /// </summary>
        public string GetBindTopic()
        {
            return _bindTopic;
        }

        /// <summary>
        /// Builder for creating LitePushConsumer instances.
        /// </summary>
        public new class Builder
        {
            private ClientConfig _clientConfig;
            private string _consumerGroup;
            private string _bindTopic;
            private ConcurrentDictionary<string, FilterExpression> _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>();
            private IMessageListener _messageListener;
            private int _maxCacheMessageCount = 1024;
            private int _maxCacheMessageSizeInBytes = 64 * 1024 * 1024;
            private int _consumptionThreadCount = 20;
            private bool _enableFifoConsumeAccelerator;

            public Builder SetClientConfig(ClientConfig clientConfig)
            {
                Preconditions.CheckArgument(clientConfig != null, "clientConfig should not be null");
                _clientConfig = clientConfig;
                return this;
            }

            public Builder SetConsumerGroup(string consumerGroup)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(consumerGroup), "consumerGroup should not be null or empty");
                Preconditions.CheckArgument(PushConsumer.ConsumerGroupRegex.Match(consumerGroup).Success,
                    $"consumerGroup does not match the regex {PushConsumer.ConsumerGroupRegex}");
                _consumerGroup = consumerGroup;
                return this;
            }

            public Builder SetBindTopic(string bindTopic)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(bindTopic), "bindTopic should not be null or empty");
                _bindTopic = bindTopic;
                // Default subscription: (bindTopic, *) for code reuse.
                _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>
                {
                    [bindTopic] = FilterExpression.SubAll
                };
                return this;
            }

            public Builder SetSubscriptionExpressions(Dictionary<string, FilterExpression> subscriptionExpressions)
            {
                if (subscriptionExpressions != null)
                {
                    _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>(subscriptionExpressions);
                }
                else
                {
                    _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>();
                }
                return this;
            }

            public Builder SetMessageListener(IMessageListener messageListener)
            {
                Preconditions.CheckArgument(messageListener != null, "messageListener should not be null");
                _messageListener = messageListener;
                return this;
            }

            public Builder SetMaxCacheMessageCount(int maxCacheMessageCount)
            {
                Preconditions.CheckArgument(maxCacheMessageCount > 0, "maxCacheMessageCount should be positive");
                _maxCacheMessageCount = maxCacheMessageCount;
                return this;
            }

            public Builder SetMaxCacheMessageSizeInBytes(int maxCacheMessageSizeInBytes)
            {
                Preconditions.CheckArgument(maxCacheMessageSizeInBytes > 0, "maxCacheMessageSizeInBytes should be positive");
                _maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
                return this;
            }

            public Builder SetConsumptionThreadCount(int consumptionThreadCount)
            {
                Preconditions.CheckArgument(consumptionThreadCount > 0, "consumptionThreadCount should be positive");
                _consumptionThreadCount = consumptionThreadCount;
                return this;
            }

            public Builder SetEnableFifoConsumeAccelerator(bool enableFifoConsumeAccelerator)
            {
                _enableFifoConsumeAccelerator = enableFifoConsumeAccelerator;
                return this;
            }

            public async Task<LitePushConsumer> Build()
            {
                Preconditions.CheckArgument(_clientConfig != null, "clientConfig has not been set yet");
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(_consumerGroup), "consumerGroup has not been set yet");
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(_bindTopic), "bindTopic has not been set yet");
                Preconditions.CheckArgument(_messageListener != null, "messageListener has not been set yet");

                var litePushConsumer = new LitePushConsumer(
                    _clientConfig,
                    _consumerGroup,
                    _bindTopic,
                    _subscriptionExpressions,
                    _messageListener,
                    _maxCacheMessageCount,
                    _maxCacheMessageSizeInBytes,
                    _consumptionThreadCount,
                    _enableFifoConsumeAccelerator);

                await litePushConsumer.Start();
                return litePushConsumer;
            }
        }
    }
}
