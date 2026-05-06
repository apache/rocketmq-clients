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
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Manages lite topic subscriptions for lite push consumer.
    /// Handles subscription synchronization, quota management, and server notifications.
    /// </summary>
    internal class LiteSubscriptionManager
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<LiteSubscriptionManager>();

        private readonly PushConsumer _consumer;
        private readonly string _bindTopic;
        private readonly string _consumerGroup;
        private readonly ConcurrentDictionary<string, byte> _liteTopicSet;
        
        // Client-side lite subscription quota limit
        private volatile int _liteSubscriptionQuota;
        private volatile int _maxLiteTopicSize = 64;

        public LiteSubscriptionManager(PushConsumer consumer, string bindTopic, string consumerGroup)
        {
            _consumer = consumer;
            _bindTopic = bindTopic;
            _consumerGroup = consumerGroup;
            _liteTopicSet = new ConcurrentDictionary<string, byte>();
        }

        public void Start()
        {
            // Sync all after startup to initialize lite subscription infrastructure
            // This triggers the initial sync even if liteTopicSet is empty
            Logger.LogInformation($"Starting LiteSubscriptionManager, bindTopic={GetBindTopicName()}, consumerGroup={GetConsumerGroupName()}");
            
            try
            {
                SyncAllLiteSubscription();
                Logger.LogInformation($"Initial lite subscription sync completed successfully");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Failed to perform initial lite subscription sync");
            }
            
            // Schedule periodic sync every 30 seconds using Timer
            var timer = new System.Threading.Timer(
                callback: state =>
                {
                    try
                    {
                        SyncAllLiteSubscription();
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex, $"Error in periodic syncAllLiteSubscription, clientId={_consumer.GetClientId()}");
                    }
                },
                state: null,
                dueTime: TimeSpan.FromSeconds(30),
                period: TimeSpan.FromSeconds(30)
            );
            
            Logger.LogInformation($"LiteSubscriptionManager started successfully, scheduled periodic sync every 30 seconds");
        }

        public string GetBindTopicName()
        {
            return _bindTopic;
        }

        public string GetConsumerGroupName()
        {
            return _consumerGroup;
        }

        public ISet<string> GetLiteTopicSet()
        {
            // Return a snapshot copy to avoid concurrent modification
            lock (_liteTopicSet)
            {
                return new HashSet<string>(_liteTopicSet.Keys);
            }
        }

        public void Sync(Proto.Settings settings)
        {
            // If subscription doesn't exist, return early
            if (settings.Subscription.Equals(default(Proto.Subscription)))
            {
                return;
            }

            var subscription = settings.Subscription;
            if (subscription.HasLiteSubscriptionQuota)
            {
                _liteSubscriptionQuota = subscription.LiteSubscriptionQuota;
            }
            if (subscription.HasMaxLiteTopicSize)
            {
                _maxLiteTopicSize = subscription.MaxLiteTopicSize;
            }
        }

        public async Task SubscribeLite(string liteTopic, OffsetOption offsetOption = null)
        {
            _consumer.CheckRunning();
            
            if (_liteTopicSet.ContainsKey(liteTopic))
            {
                return;
            }

            ValidateLiteTopic(liteTopic, _maxLiteTopicSize);
            CheckLiteSubscriptionQuota(1);

            try
            {
                await SyncLiteSubscription(
                    Proto.LiteSubscriptionAction.PartialAdd,
                    new[] { liteTopic },
                    offsetOption);
            }
            catch (Exception)
            {
                Logger.LogError($"Failed to subscribeLite {liteTopic}, topic={GetBindTopicName()}, group={GetConsumerGroupName()}, clientId={_consumer.GetClientId()}");
                throw;
            }

            _liteTopicSet.TryAdd(liteTopic, 0);
            Logger.LogInformation($"SubscribeLite success, liteTopic={liteTopic}, topic={GetBindTopicName()}, group={GetConsumerGroupName()}, clientId={_consumer.GetClientId()}");
        }

        public async Task UnsubscribeLite(string liteTopic)
        {
            _consumer.CheckRunning();
            
            if (!_liteTopicSet.ContainsKey(liteTopic))
            {
                return;
            }

            try
            {
                await SyncLiteSubscription(
                    Proto.LiteSubscriptionAction.PartialRemove,
                    new[] { liteTopic },
                    null);
            }
            catch (Exception)
            {
                Logger.LogError($"Failed to unsubscribeLite {liteTopic}, topic={GetBindTopicName()}, group={GetConsumerGroupName()}, clientId={_consumer.GetClientId()}");
                throw;
            }

            _liteTopicSet.TryRemove(liteTopic, out _);
            Logger.LogInformation($"UnsubscribeLite success, liteTopic={liteTopic}, topic={GetBindTopicName()}, group={GetConsumerGroupName()}, clientId={_consumer.GetClientId()}");
        }

        public void SyncAllLiteSubscription()
        {
            try
            {
                CheckLiteSubscriptionQuota(0);
                var liteTopics = _liteTopicSet.Keys.ToList();
                Logger.LogDebug($"Syncing all lite subscriptions, count={liteTopics.Count}, topics=[{string.Join(", ", liteTopics)}]");
                
                SyncLiteSubscription(
                    Proto.LiteSubscriptionAction.CompleteAdd,
                    liteTopics,
                    null).Wait();
                
                Logger.LogDebug($"SyncAllLiteSubscription completed successfully, clientId={_consumer.GetClientId()}");
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Schedule syncAllLiteSubscription error, clientId={_consumer.GetClientId()}, bindTopic={GetBindTopicName()}, consumerGroup={GetConsumerGroupName()}");
            }
        }

        private async Task SyncLiteSubscription(
            Proto.LiteSubscriptionAction action,
            IEnumerable<string> diff,
            OffsetOption offsetOption)
        {
            var builder = new Proto.SyncLiteSubscriptionRequest
            {
                Action = action,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = _consumer.Namespace,
                    Name = _bindTopic
                },
                Group = new Proto.Resource
                {
                    ResourceNamespace = _consumer.Namespace,
                    Name = _consumerGroup
                }
            };

            builder.LiteTopicSet.AddRange(diff);

            if (offsetOption != null)
            {
                builder.OffsetOption = OffsetOptionHelper.ToProtobuf(offsetOption);
            }

            var requestTimeout = _consumer.GetRequestTimeout();
            var response = await _consumer.SyncLiteSubscription(builder, requestTimeout);
            
            StatusChecker.Check(response.Status, builder, "");
        }

        public void OnNotifyUnsubscribeLiteCommand(Proto.NotifyUnsubscribeLiteCommand command)
        {
            var liteTopic = command.LiteTopic;
            Logger.LogInformation($"Notify unsubscribe lite command received, liteTopic={liteTopic}, group={GetConsumerGroupName()}, bindTopic={GetBindTopicName()}");
            
            if (!string.IsNullOrWhiteSpace(liteTopic))
            {
                _liteTopicSet.TryRemove(liteTopic, out _);
            }
        }

        private void ValidateLiteTopic(string liteTopic, int maxLength)
        {
            if (string.IsNullOrWhiteSpace(liteTopic))
            {
                throw new ArgumentException("liteTopic is blank", nameof(liteTopic));
            }

            if (liteTopic.Length > maxLength)
            {
                var errorMessage = $"liteTopic length exceeded max length {maxLength}, liteTopic: {liteTopic}";
                throw new ArgumentException(errorMessage, nameof(liteTopic));
            }
        }

        private void CheckLiteSubscriptionQuota(int delta)
        {
            if (_liteTopicSet.Count + delta > _liteSubscriptionQuota)
            {
                throw new LiteSubscriptionQuotaExceededException(
                    $"Lite subscription quota exceeded {_liteSubscriptionQuota}, current size: {_liteTopicSet.Count}, delta: {delta}");
            }
        }
    }
}
