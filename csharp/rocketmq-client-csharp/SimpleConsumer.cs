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
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;
using NLog;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    public class SimpleConsumer : Consumer
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly ConcurrentDictionary<string /* topic */, SubscriptionLoadBalancer> _subscriptionRouteDataCache;
        private readonly ConcurrentDictionary<string /* topic */, FilterExpression> _subscriptionExpressions;
        private readonly TimeSpan _awaitDuration;
        private readonly SimpleSubscriptionSettings _simpleSubscriptionSettings;
        private int _topicRoundRobinIndex;

        public SimpleConsumer(ClientConfig clientConfig, string consumerGroup, TimeSpan awaitDuration,
            Dictionary<string, FilterExpression> subscriptionExpressions) : this(clientConfig, consumerGroup,
            awaitDuration, new ConcurrentDictionary<string, FilterExpression>(subscriptionExpressions))
        {
        }

        private SimpleConsumer(ClientConfig clientConfig, string consumerGroup, TimeSpan awaitDuration,
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions) : base(clientConfig, consumerGroup)
        {
            _awaitDuration = awaitDuration;
            _subscriptionRouteDataCache = new ConcurrentDictionary<string, SubscriptionLoadBalancer>();
            _subscriptionExpressions = subscriptionExpressions;
            _simpleSubscriptionSettings = new SimpleSubscriptionSettings(ClientId, Endpoints,
                ConsumerGroup, clientConfig.RequestTimeout, awaitDuration, subscriptionExpressions);
            _topicRoundRobinIndex = 0;
        }

        public async Task Subscribe(string topic, FilterExpression filterExpression)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

            await GetSubscriptionLoadBalancer(topic);
            _subscriptionExpressions.TryAdd(topic, filterExpression);
        }

        public void Unsubscribe(string topic)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

            _subscriptionExpressions.TryRemove(topic, out _);
        }

        public override async Task Start()
        {
            try
            {
                State = State.Starting;
                Logger.Info($"Begin to start the rocketmq simple consumer, clientId={ClientId}");
                await base.Start();
                Logger.Info($"The rocketmq simple consumer starts successfully, clientId={ClientId}");
                State = State.Running;
            }
            catch (Exception)
            {
                State = State.Failed;
                throw;
            }
        }

        public override async Task Shutdown()
        {
            try
            {
                State = State.Stopping;
                Logger.Info($"Begin to shutdown the rocketmq simple consumer, clientId={ClientId}");
                await base.Shutdown();
                Logger.Info($"The rocketmq simple consumer starts successfully, clientId={ClientId}");
                State = State.Terminated;
            }
            catch (Exception)
            {
                State = State.Failed;
                throw;
            }
        }

        protected override IEnumerable<string> GetTopics()
        {
            return _subscriptionExpressions.Keys;
        }

        protected override Proto.NotifyClientTerminationRequest WrapNotifyClientTerminationRequest()
        {
            return new Proto.NotifyClientTerminationRequest()
            {
                Group = GetProtobufGroup()
            };
        }

        protected override Proto.HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto::HeartbeatRequest
            {
                ClientType = Proto.ClientType.SimpleConsumer
            };
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
            var subscriptionLoadBalancer = new SubscriptionLoadBalancer(topicRouteData);
            _subscriptionRouteDataCache.TryAdd(topic, subscriptionLoadBalancer);
        }

        public override Settings GetSettings()
        {
            return _simpleSubscriptionSettings;
        }

        private async Task<SubscriptionLoadBalancer> GetSubscriptionLoadBalancer(string topic)
        {
            if (_subscriptionRouteDataCache.TryGetValue(topic, out var subscriptionLoadBalancer))
            {
                return subscriptionLoadBalancer;
            }

            var topicRouteData = await GetRouteData(topic);
            subscriptionLoadBalancer = new SubscriptionLoadBalancer(topicRouteData);
            _subscriptionRouteDataCache.TryAdd(topic, subscriptionLoadBalancer);

            return subscriptionLoadBalancer;
        }


        public async Task<List<MessageView>> Receive(int maxMessageNum, TimeSpan invisibleDuration)
        {
            if (maxMessageNum <= 0)
            {
                throw new InternalErrorException("maxMessageNum must be greater than 0");
            }

            var copy = new ConcurrentDictionary<string, FilterExpression>(_subscriptionExpressions);
            var topics = new List<string>(copy.Keys);
            if (topics.Count <= 0)
            {
                throw new ArgumentException("There is no topic to receive message");
            }

            var index = Utilities.GetPositiveMod(Interlocked.Increment(ref _topicRoundRobinIndex), topics.Count);
            var topic = topics[index];
            var filterExpression = _subscriptionExpressions[topic];
            var subscriptionLoadBalancer = await GetSubscriptionLoadBalancer(topic);

            var mq = subscriptionLoadBalancer.TakeMessageQueue();
            var request = WrapReceiveMessageRequest(maxMessageNum, mq, filterExpression, invisibleDuration);
            var receiveMessageResult = await ReceiveMessage(request, mq, _awaitDuration);
            return receiveMessageResult.Messages;
        }

        public async void ChangeInvisibleDuration(MessageView messageView, TimeSpan invisibleDuration)
        {
            var request = WrapChangeInvisibleDuration(messageView, invisibleDuration);
            var response = await ClientManager.ChangeInvisibleDuration(messageView.MessageQueue.Broker.Endpoints,
                request, ClientConfig.RequestTimeout);
            StatusChecker.Check(response.Status, request);
        }


        public async Task Ack(MessageView messageView)
        {
            var request = WrapAckMessageRequest(messageView);
            var response = await ClientManager.AckMessage(messageView.MessageQueue.Broker.Endpoints, request,
                ClientConfig.RequestTimeout);
            StatusChecker.Check(response.Status, request);
        }

        private Proto.AckMessageRequest WrapAckMessageRequest(MessageView messageView)
        {
            var topicResource = new Proto.Resource
            {
                Name = messageView.Topic
            };
            var entry = new Proto.AckMessageEntry
            {
                MessageId = messageView.MessageId,
                ReceiptHandle = messageView.ReceiptHandle,
            };
            return new Proto.AckMessageRequest
            {
                Group = GetProtobufGroup(),
                Topic = topicResource,
                Entries = { entry }
            };
        }

        private Proto.ChangeInvisibleDurationRequest WrapChangeInvisibleDuration(MessageView messageView,
            TimeSpan invisibleDuration)
        {
            var topicResource = new Proto.Resource
            {
                Name = messageView.Topic
            };
            return new Proto.ChangeInvisibleDurationRequest
            {
                Topic = topicResource,
                Group = GetProtobufGroup(),
                ReceiptHandle = messageView.ReceiptHandle,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration),
                MessageId = messageView.MessageId
            };
        }

        private Proto.Resource GetProtobufGroup()
        {
            return new Proto.Resource()
            {
                Name = ConsumerGroup
            };
        }
    }
}