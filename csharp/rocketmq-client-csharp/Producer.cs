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

        public override async Task Start()
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
        
        public override async Task Shutdown()
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

        private async Task<PublishingLoadBalancer> GetPublishingLoadBalancer(string topic)
        {
            if (_publishingRouteDataCache.TryGetValue(topic, out var publishingLoadBalancer))
            {
                return publishingLoadBalancer;
            }

            var topicRouteData = await GetRouteData(topic);
            publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
            _publishingRouteDataCache.TryAdd(topic, publishingLoadBalancer);

            return publishingLoadBalancer;
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
            var publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
            _publishingRouteDataCache.TryAdd(topic, publishingLoadBalancer);
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
            if (MessageType.Transaction == publishingMessage.MessageType)
            {
                // No more retries for transactional message.
                maxAttempts = 1;
            }

            // Prepare the candidate message queue(s) for retry-sending in advance.
            var candidates = null == publishingMessage.MessageGroup
                ? publishingLoadBalancer.TakeMessageQueues(new HashSet<Endpoints>(Isolated.Keys), maxAttempts)
                : new List<MessageQueue>
                    { publishingLoadBalancer.TakeMessageQueueByMessageGroup(publishingMessage.MessageGroup) };
            Exception exception = null;
            for (var attempt = 1; attempt <= maxAttempts; attempt++)
            {
                var stopwatch = Stopwatch.StartNew();
                try
                {
                    var sendReceipt = await Send0(publishingMessage, candidates, attempt, maxAttempts);
                    return sendReceipt;
                }
                catch (Exception e)
                {
                    exception = e;
                }
                finally
                {
                    var elapsed = stopwatch.Elapsed.Milliseconds;
                    _sendCostTimeHistogram.Record(elapsed,
                        new KeyValuePair<string, object>(MetricConstant.Topic, message.Topic),
                        new KeyValuePair<string, object>(MetricConstant.ClientId, ClientId),
                        new KeyValuePair<string, object>(MetricConstant.InvocationStatus,
                            null == exception ? MetricConstant.True : MetricConstant.False));
                }
            }

            throw exception!;
        }

        public async Task<SendReceipt> Send(Message message)
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

        private async Task<SendReceipt> Send0(PublishingMessage message, List<MessageQueue> candidates, int attempt,
            int maxAttempts)
        {
            var candidateIndex = (attempt - 1) % candidates.Count;
            var mq = candidates[candidateIndex];
            if (PublishingSettings.IsValidateMessageType() &&
                !mq.AcceptMessageTypes.Contains(message.MessageType))
            {
                throw new ArgumentException("Current message type does not match with the accept message types," +
                                            $" topic={message.Topic}, actualMessageType={message.MessageType}" +
                                            $" acceptMessageType={string.Join(",", mq.AcceptMessageTypes)}");
            }

            var sendMessageRequest = WrapSendMessageRequest(message, mq);
            var endpoints = mq.Broker.Endpoints;
            var response = await ClientManager.SendMessage(endpoints, sendMessageRequest, ClientConfig.RequestTimeout);
            try
            {
                var sendReceipts = SendReceipt.ProcessSendMessageResponse(mq, response);

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
                // Isolate current endpoints.
                Isolated[endpoints] = true;
                if (attempt >= maxAttempts)
                {
                    Logger.Error("Failed to send message finally, run out of attempt times, " +
                                 $"topic={message.Topic}, maxAttempt={maxAttempts}, attempt={attempt}, " +
                                 $"endpoints={endpoints}, messageId={message.MessageId}, clientId={ClientId}");
                    throw;
                }

                Logger.Warn(e, $"Failed to send message, topic={message.Topic}, maxAttempts={maxAttempts}, " +
                               $"attempt={attempt}, endpoints={endpoints}, messageId={message.MessageId}," +
                               $" clientId={ClientId}");
                throw;
            }
        }

        public override Settings GetSettings()
        {
            return PublishingSettings;
        }

        public override async void OnRecoverOrphanedTransactionCommand(Endpoints endpoints,
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
                case TransactionResolution.COMMIT:
                case TransactionResolution.ROLLBACK:
                    await EndTransaction(endpoints, message.Topic, message.MessageId, command.TransactionId,
                        transactionResolution);
                    break;
                case TransactionResolution.UNKNOWN:
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
                Resolution = TransactionResolution.COMMIT == resolution
                    ? Proto.TransactionResolution.Commit
                    : Proto.TransactionResolution.Rollback
            };
            var response = await ClientManager.EndTransaction(endpoints, request, ClientConfig.RequestTimeout);
            StatusChecker.Check(response.Status, request);
        }

        public class Builder
        {
            private ClientConfig _clientConfig;
            private readonly ConcurrentDictionary<string, bool> _publishingTopics = new();
            private int _maxAttempts = 3;
            private ITransactionChecker _checker;

            public Builder SetClientConfig(ClientConfig clientConfig)
            {
                _clientConfig = clientConfig;
                return this;
            }

            public Builder SetTopics(params string[] topics)
            {
                foreach (var topic in topics)
                {
                    _publishingTopics[topic] = true;
                }

                return this;
            }

            public Builder SetMaxAttempts(int maxAttempts)
            {
                _maxAttempts = maxAttempts;
                return this;
            }

            public Builder SetTransactionChecker(ITransactionChecker checker)
            {
                _checker = checker;
                return this;
            }

            public async Task<Producer> Build()
            {
                var producer = new Producer(_clientConfig, _publishingTopics, _maxAttempts, _checker);
                await producer.Start();
                return producer;
            }
        }
    }
}