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
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading.Tasks;
using Proto = Apache.Rocketmq.V2;
using NLog;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    public class Producer : Client, IAsyncDisposable, IDisposable
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly ConcurrentDictionary<string /* topic */, PublishingLoadBalancer> _publishingRouteDataCache;
        internal readonly PublishingSettings PublishingSettings;
        private readonly ConcurrentDictionary<string, bool> _publishingTopics;
        private readonly ITransactionChecker _checker;

        private readonly Histogram<double> _sendCostTimeHistogram;

        private Producer(ClientConfig clientConfig, ConcurrentDictionary<string, bool> publishingTopics,
            int maxAttempts, ITransactionChecker checker) : base(clientConfig)
        {
            var retryPolicy = ExponentialBackoffRetryPolicy.ImmediatelyRetryPolicy(maxAttempts);
            PublishingSettings = new PublishingSettings(ClientId, Endpoints, retryPolicy,
                clientConfig.RequestTimeout, publishingTopics);
            _publishingRouteDataCache = new ConcurrentDictionary<string, PublishingLoadBalancer>();
            _publishingTopics = publishingTopics;
            _sendCostTimeHistogram =
                ClientMeterManager.Meter.CreateHistogram<double>(MetricConstant.SendCostTimeMetricName, "milliseconds");
            _checker = checker;
        }

        protected override IEnumerable<string> GetTopics()
        {
            return _publishingTopics.Keys;
        }

        protected override async Task Start()
        {
            try
            {
                State = State.Starting;
                Logger.Info($"Begin to start the rocketmq producer, clientId={ClientId}");
                await base.Start();
                Logger.Info($"The rocketmq producer starts successfully, clientId={ClientId}");
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
                Logger.Info($"Begin to shutdown the rocketmq producer, clientId={ClientId}");
                await base.Shutdown();
                Logger.Info($"Shutdown the rocketmq producer successfully, clientId={ClientId}");
                State = State.Terminated;
            }
            catch (Exception)
            {
                State = State.Failed;
                throw;
            }
        }

        protected override Proto::HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto::HeartbeatRequest
            {
                ClientType = Proto.ClientType.Producer
            };
        }

        protected override Proto::NotifyClientTerminationRequest WrapNotifyClientTerminationRequest()
        {
            return new Proto::NotifyClientTerminationRequest();
        }

        private PublishingLoadBalancer UpdatePublishingLoadBalancer(string topic, TopicRouteData topicRouteData)
        {
            if (_publishingRouteDataCache.TryGetValue(topic, out var publishingLoadBalancer))
            {
                publishingLoadBalancer = publishingLoadBalancer.Update(topicRouteData);
            }
            else
            {
                publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
            }

            _publishingRouteDataCache[topic] = publishingLoadBalancer;
            return publishingLoadBalancer;
        }

        private async Task<PublishingLoadBalancer> GetPublishingLoadBalancer(string topic)
        {
            if (_publishingRouteDataCache.TryGetValue(topic, out var publishingLoadBalancer))
            {
                return publishingLoadBalancer;
            }

            var topicRouteData = await GetRouteData(topic);
            return UpdatePublishingLoadBalancer(topic, topicRouteData);
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
            UpdatePublishingLoadBalancer(topic, topicRouteData);
        }

        private IRetryPolicy GetRetryPolicy()
        {
            return PublishingSettings.GetRetryPolicy();
        }

        private async Task<SendReceipt> Send(Message message, bool txEnabled)
        {
            var publishingLoadBalancer = await GetPublishingLoadBalancer(message.Topic);
            var publishingMessage = new PublishingMessage(message, PublishingSettings, txEnabled);
            var retryPolicy = GetRetryPolicy();
            var maxAttempts = retryPolicy.GetMaxAttempts();

            // Prepare the candidate message queue(s) for retry-sending in advance.
            var candidates = null == publishingMessage.MessageGroup
                ? publishingLoadBalancer.TakeMessageQueues(new HashSet<Endpoints>(Isolated.Keys), maxAttempts)
                : new List<MessageQueue>
                    { publishingLoadBalancer.TakeMessageQueueByMessageGroup(publishingMessage.MessageGroup) };
            var sendReceipt = await Send0(publishingMessage, candidates);
            return sendReceipt;
        }

        public async Task<ISendReceipt> Send(Message message)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Producer is not running");
            }

            var sendReceipt = await Send(message, false);
            return sendReceipt;
        }

        public async Task<SendReceipt> Send(Message message, ITransaction transaction)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Producer is not running");
            }

            var tx = (Transaction)transaction;
            var publishingMessage = tx.TryAddMessage(message);
            var sendReceipt = await Send(message, true);
            tx.TryAddReceipt(publishingMessage, sendReceipt);
            return sendReceipt;
        }

        private static Proto.SendMessageRequest WrapSendMessageRequest(PublishingMessage message, MessageQueue mq)
        {
            return new Proto.SendMessageRequest
            {
                Messages = { message.ToProtobuf(mq.QueueId) }
            };
        }

        private async Task<SendReceipt> Send0(PublishingMessage message, List<MessageQueue> candidates)
        {
            var retryPolicy = GetRetryPolicy();
            var maxAttempts = retryPolicy.GetMaxAttempts();
            Exception exception = null;
            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                var stopwatch = Stopwatch.StartNew();

                var candidateIndex = (attempt - 1) % candidates.Count;
                var mq = candidates[candidateIndex];
                if (PublishingSettings.IsValidateMessageType() && !mq.AcceptMessageTypes.Contains(message.MessageType))
                {
                    throw new ArgumentException(
                        "Current message type does not match with the accept message types," +
                        $" topic={message.Topic}, actualMessageType={message.MessageType}" +
                        $" acceptMessageType={string.Join(",", mq.AcceptMessageTypes)}");
                }

                var sendMessageRequest = WrapSendMessageRequest(message, mq);
                var endpoints = mq.Broker.Endpoints;
                try
                {
                    var invocation =
                        await ClientManager.SendMessage(endpoints, sendMessageRequest, ClientConfig.RequestTimeout);
                    var sendReceipts = SendReceipt.ProcessSendMessageResponse(mq, invocation);

                    var sendReceipt = sendReceipts.First();
                    if (attempt > 1)
                    {
                        Logger.Info(
                            $"Re-send message successfully, topic={message.Topic}, messageId={sendReceipt.MessageId}," +
                            $" maxAttempts={maxAttempts}, endpoints={endpoints}, clientId={ClientId}");
                    }

                    return sendReceipt;
                }
                catch (Exception e)
                {
                    exception = e;

                    // Isolate current endpoints.
                    Isolated[endpoints] = true;
                    if (attempt >= maxAttempts)
                    {
                        Logger.Error(e, "Failed to send message finally, run out of attempt times, " +
                                        $"topic={message.Topic}, maxAttempt={maxAttempts}, attempt={attempt}, " +
                                        $"endpoints={endpoints}, messageId={message.MessageId}, clientId={ClientId}");
                        throw;
                    }

                    if (MessageType.Transaction == message.MessageType)
                    {
                        Logger.Error(e, "Failed to send transaction message, run out of attempt times, " +
                                        $"topic={message.Topic}, maxAttempt=1, attempt={attempt}, " +
                                        $"endpoints={endpoints}, messageId={message.MessageId}, clientId={ClientId}");
                        throw;
                    }

                    if (!(exception is TooManyRequestsException))
                    {
                        // Retry immediately if the request is not throttled.
                        Logger.Warn(e, $"Failed to send message, topic={message.Topic}, maxAttempts={maxAttempts}, " +
                                       $"attempt={attempt}, endpoints={endpoints}, messageId={message.MessageId}," +
                                       $" clientId={ClientId}");
                        continue;
                    }

                    var nextAttempt = 1 + attempt;
                    var delay = retryPolicy.GetNextAttemptDelay(nextAttempt);
                    await Task.Delay(delay);
                    Logger.Warn(e, "Failed to send message due to too many request, would attempt to resend " +
                                   $"after {delay}, topic={message.Topic}, maxAttempts={maxAttempts}, attempt={attempt}, " +
                                   $"endpoints={endpoints}, messageId={message.MessageId}, clientId={ClientId}");
                }
                finally
                {
                    var elapsed = stopwatch.Elapsed.Milliseconds;
                    _sendCostTimeHistogram.Record(elapsed,
                        new KeyValuePair<string, object>(MetricConstant.Topic, message.Topic),
                        new KeyValuePair<string, object>(MetricConstant.ClientId, ClientId),
                        new KeyValuePair<string, object>(MetricConstant.InvocationStatus,
                            null == exception ? MetricConstant.Success : MetricConstant.Failure));
                }
            }

            throw new Exception($"Failed to send message finally, topic={message.Topic}, clientId={ClientId}",
                exception);
        }

        internal override Settings GetSettings()
        {
            return PublishingSettings;
        }

        internal override async void OnRecoverOrphanedTransactionCommand(Endpoints endpoints,
            Proto.RecoverOrphanedTransactionCommand command)
        {
            var messageId = command.Message.SystemProperties.MessageId;
            if (null == _checker)
            {
                Logger.Error($"No transaction checker registered, ignore it, messageId={messageId}, " +
                             $"transactionId={command.TransactionId}, endpoints={endpoints}, clientId={ClientId}");
                return;
            }

            var message = MessageView.FromProtobuf(command.Message);
            var transactionResolution = _checker.Check(message);
            switch (transactionResolution)
            {
                case TransactionResolution.Commit:
                case TransactionResolution.Rollback:
                    await EndTransaction(endpoints, message.Topic, message.MessageId, command.TransactionId,
                        transactionResolution);
                    break;
                case TransactionResolution.Unknown:
                default:
                    break;
            }
        }

        public ITransaction BeginTransaction()
        {
            return new Transaction(this);
        }

        internal async Task EndTransaction(Endpoints endpoints, string topic, string messageId, string transactionId,
            TransactionResolution resolution)
        {
            var topicResource = new Proto.Resource
            {
                Name = topic
            };
            var request = new Proto.EndTransactionRequest
            {
                TransactionId = transactionId,
                MessageId = messageId,
                Topic = topicResource,
                Resolution = TransactionResolution.Commit == resolution
                    ? Proto.TransactionResolution.Commit
                    : Proto.TransactionResolution.Rollback
            };
            var invocation = await ClientManager.EndTransaction(endpoints, request, ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }

        public class Builder
        {
            private ClientConfig _clientConfig;

            private readonly ConcurrentDictionary<string, bool> _publishingTopics =
                new ConcurrentDictionary<string, bool>();

            private int _maxAttempts = 3;
            private ITransactionChecker _checker;

            public Builder SetClientConfig(ClientConfig clientConfig)
            {
                Preconditions.CheckArgument(null != clientConfig, "clientConfig should not be null");
                _clientConfig = clientConfig;
                return this;
            }

            public Builder SetTopics(params string[] topics)
            {
                foreach (var topic in topics)
                {
                    Preconditions.CheckArgument(null != topic, "topic should not be null");
                    Preconditions.CheckArgument(topic != null && Message.TopicRegex.Match(topic).Success,
                        $"topic does not match the regex {Message.TopicRegex}");
                    _publishingTopics[topic!] = true;
                }

                return this;
            }

            public Builder SetMaxAttempts(int maxAttempts)
            {
                Preconditions.CheckArgument(maxAttempts > 0, "maxAttempts must be positive");
                _maxAttempts = maxAttempts;
                return this;
            }

            public Builder SetTransactionChecker(ITransactionChecker checker)
            {
                Preconditions.CheckArgument(null != checker, "checker should not be null");
                _checker = checker;
                return this;
            }

            public async Task<Producer> Build()
            {
                Preconditions.CheckArgument(null != _clientConfig, "clientConfig has not been set yet");
                var producer = new Producer(_clientConfig, _publishingTopics, _maxAttempts, _checker);
                await producer.Start();
                return producer;
            }
        }
    }
}