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
    public class SimpleConsumer : Consumer, IAsyncDisposable, IDisposable
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
            _subscriptionExpressions[topic] = filterExpression;
        }

        public void Unsubscribe(string topic)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

            _subscriptionExpressions.TryRemove(topic, out _);
        }

        protected override async Task Start()
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

        public async ValueTask DisposeAsync()
        {
            await Shutdown().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            Shutdown().Wait();
            GC.SuppressFinalize(this);
        }

        protected override async Task Shutdown()
        {
            try
            {
                State = State.Stopping;
                Logger.Info($"Begin to shutdown the rocketmq simple consumer, clientId={ClientId}");
                await base.Shutdown();
                Logger.Info($"Shutdown the rocketmq simple consumer successfully, clientId={ClientId}");
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
                ClientType = Proto.ClientType.SimpleConsumer,
                Group = GetProtobufGroup()
            };
        }

        private SubscriptionLoadBalancer UpdateSubscriptionLoadBalancer(string topic, TopicRouteData topicRouteData)
        {
            if (_subscriptionRouteDataCache.TryGetValue(topic, out var subscriptionLoadBalancer))
            {
                subscriptionLoadBalancer = subscriptionLoadBalancer.Update(topicRouteData);
            }
            else
            {
                subscriptionLoadBalancer = new SubscriptionLoadBalancer(topicRouteData);
            }

            _subscriptionRouteDataCache[topic] = subscriptionLoadBalancer;
            return subscriptionLoadBalancer;
        }

        private async Task<SubscriptionLoadBalancer> GetSubscriptionLoadBalancer(string topic)
        {
            if (_subscriptionRouteDataCache.TryGetValue(topic, out var subscriptionLoadBalancer))
            {
                return subscriptionLoadBalancer;
            }

            var topicRouteData = await GetRouteData(topic);
            return UpdateSubscriptionLoadBalancer(topic, topicRouteData);
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
            UpdateSubscriptionLoadBalancer(topic, topicRouteData);
        }

        internal override Settings GetSettings()
        {
            return _simpleSubscriptionSettings;
        }

        public async Task<List<MessageView>> Receive(int maxMessageNum, TimeSpan invisibleDuration)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

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
            var request =
                WrapReceiveMessageRequest(maxMessageNum, mq, filterExpression, _awaitDuration, invisibleDuration);
            var receiveMessageResult = await ReceiveMessage(request, mq, _awaitDuration);
            return receiveMessageResult.Messages;
        }

        public async void ChangeInvisibleDuration(MessageView messageView, TimeSpan invisibleDuration)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

            var request = WrapChangeInvisibleDuration(messageView, invisibleDuration);
            var invocation = await ClientManager.ChangeInvisibleDuration(messageView.MessageQueue.Broker.Endpoints,
                request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }


        public async Task Ack(MessageView messageView)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Simple consumer is not running");
            }

            var request = WrapAckMessageRequest(messageView);
            var invocation = await ClientManager.AckMessage(messageView.MessageQueue.Broker.Endpoints, request,
                ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
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

        public class Builder
        {
            private ClientConfig _clientConfig;
            private string _consumerGroup;
            private TimeSpan _awaitDuration;
            private ConcurrentDictionary<string, FilterExpression> _subscriptionExpressions;

            public Builder SetClientConfig(ClientConfig clientConfig)
            {
                Preconditions.CheckArgument(null != clientConfig, "clientConfig should not be null");
                _clientConfig = clientConfig;
                return this;
            }

            public Builder SetConsumerGroup(string consumerGroup)
            {
                Preconditions.CheckArgument(null != consumerGroup, "consumerGroup should not be null");
                Preconditions.CheckArgument(consumerGroup != null && ConsumerGroupRegex.Match(consumerGroup).Success,
                    $"topic does not match the regex {ConsumerGroupRegex}");
                _consumerGroup = consumerGroup;
                return this;
            }

            public Builder SetAwaitDuration(TimeSpan awaitDuration)
            {
                _awaitDuration = awaitDuration;
                return this;
            }

            public Builder SetSubscriptionExpression(Dictionary<string, FilterExpression> subscriptionExpressions)
            {
                Preconditions.CheckArgument(null != subscriptionExpressions,
                    "subscriptionExpressions should not be null");
                Preconditions.CheckArgument(subscriptionExpressions!.Count != 0,
                    "subscriptionExpressions should not be empty");
                _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>(subscriptionExpressions!);
                return this;
            }

            public async Task<SimpleConsumer> Build()
            {
                Preconditions.CheckArgument(null != _clientConfig, "clientConfig has not been set yet");
                Preconditions.CheckArgument(null != _consumerGroup, "consumerGroup has not been set yet");
                Preconditions.CheckArgument(!_subscriptionExpressions!.IsEmpty,
                    "subscriptionExpressions has not been set yet");
                var simpleConsumer = new SimpleConsumer(_clientConfig, _consumerGroup, _awaitDuration,
                    _subscriptionExpressions);
                await simpleConsumer.Start();
                return simpleConsumer;
            }
        }
    }
}