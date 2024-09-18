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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;
using Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;
using Microsoft.Extensions.Logging;

[assembly: InternalsVisibleTo("tests")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace Org.Apache.Rocketmq
{
    public class PushConsumer : Consumer, IAsyncDisposable, IDisposable
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<PushConsumer>();

        private static readonly TimeSpan AssignmentScanScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan AssignmentScanSchedulePeriod = TimeSpan.FromSeconds(5);

        private readonly ClientConfig _clientConfig;
        private readonly PushSubscriptionSettings _pushSubscriptionSettings;
        private readonly string _consumerGroup;
        private readonly ConcurrentDictionary<string, FilterExpression> _subscriptionExpressions;
        private readonly ConcurrentDictionary<string, Assignments> _cacheAssignments;
        private readonly IMessageListener _messageListener;
        private readonly int _maxCacheMessageCount;
        private readonly int _maxCacheMessageSizeInBytes;

        private readonly ConcurrentDictionary<MessageQueue, ProcessQueue> _processQueueTable;
        private ConsumeService _consumeService;
        private readonly TaskScheduler _consumptionTaskScheduler;
        private readonly CancellationTokenSource _consumptionCts;

        private readonly CancellationTokenSource _scanAssignmentCts;

        private readonly CancellationTokenSource _receiveMsgCts;
        private readonly CancellationTokenSource _ackMsgCts;
        private readonly CancellationTokenSource _changeInvisibleDurationCts;
        private readonly CancellationTokenSource _forwardMsgToDeadLetterQueueCts;

        /// <summary>
        /// The caller is supposed to have validated the arguments and handled throwing exception or
        /// logging warnings already, so we avoid repeating args check here.
        /// </summary>
        public PushConsumer(ClientConfig clientConfig, string consumerGroup,
            ConcurrentDictionary<string, FilterExpression> subscriptionExpressions, IMessageListener messageListener,
            int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount)
            : base(clientConfig, consumerGroup)
        {
            _clientConfig = clientConfig;
            _consumerGroup = consumerGroup;
            _subscriptionExpressions = subscriptionExpressions;
            _pushSubscriptionSettings = new PushSubscriptionSettings(_clientConfig.Namespace, ClientId, Endpoints, consumerGroup,
                clientConfig.RequestTimeout, subscriptionExpressions);
            _cacheAssignments = new ConcurrentDictionary<string, Assignments>();
            _messageListener = messageListener;
            _maxCacheMessageCount = maxCacheMessageCount;
            _maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;

            _scanAssignmentCts = new CancellationTokenSource();

            _processQueueTable = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
            _consumptionTaskScheduler = new LimitedConcurrencyLevelTaskScheduler(consumptionThreadCount);
            _consumptionCts = new CancellationTokenSource();

            _receiveMsgCts = new CancellationTokenSource();
            _ackMsgCts = new CancellationTokenSource();
            _changeInvisibleDurationCts = new CancellationTokenSource();
            _forwardMsgToDeadLetterQueueCts = new CancellationTokenSource();
        }

        protected override async Task Start()
        {
            try
            {
                State = State.Starting;
                Logger.LogInformation($"Begin to start the rocketmq push consumer, clientId={ClientId}");
                await base.Start();
                _consumeService = CreateConsumerService();
                ScheduleWithFixedDelay(ScanAssignments, AssignmentScanScheduleDelay, AssignmentScanSchedulePeriod,
                    _scanAssignmentCts.Token);
                Logger.LogInformation($"The rocketmq push consumer starts successfully, clientId={ClientId}");
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
                Logger.LogInformation($"Begin to shutdown the rocketmq push consumer, clientId={ClientId}");
                _receiveMsgCts.Cancel();
                _ackMsgCts.Cancel();
                _changeInvisibleDurationCts.Cancel();
                _forwardMsgToDeadLetterQueueCts.Cancel();
                _scanAssignmentCts.Cancel();
                await base.Shutdown();
                _consumptionCts.Cancel();
                Logger.LogInformation($"Shutdown the rocketmq push consumer successfully, clientId={ClientId}");
                State = State.Terminated;
            }
            catch (Exception)
            {
                State = State.Failed;
                throw;
            }
        }

        private ConsumeService CreateConsumerService()
        {
            if (_pushSubscriptionSettings.IsFifo())
            {
                Logger.LogInformation(
                    $"Create FIFO consume service, consumerGroup={_consumerGroup}, clientId={ClientId}");
                return new FifoConsumeService(ClientId, _messageListener, _consumptionTaskScheduler, _consumptionCts.Token);
            }
            Logger.LogInformation(
                $"Create standard consume service, consumerGroup={_consumerGroup}, clientId={ClientId}");
            return new StandardConsumeService(ClientId, _messageListener, _consumptionTaskScheduler, _consumptionCts.Token);
        }

        /// <summary>
        /// Adds a subscription expression dynamically.
        /// </summary>
        /// <param name="filterExpression">The new filter expression to add.</param>
        /// <returns>The push consumer instance.</returns>
        public async Task Subscribe(string topic, FilterExpression filterExpression)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Push consumer is not running");
            }

            await GetRouteData(topic);
            _subscriptionExpressions[topic] = filterExpression;
        }

        /// <summary>
        /// Removes a subscription expression dynamically by topic.
        /// </summary>
        /// <remarks>
        /// It stops the backend task to fetch messages from the server.
        /// The locally cached messages whose topic was removed before would not be delivered 
        /// to the <see cref="IMessageListener"/> anymore.
        /// 
        /// Nothing occurs if the specified topic does not exist in subscription expressions 
        /// of the push consumer.
        /// </remarks>
        /// <param name="topic">The topic to remove the subscription.</param>
        /// <returns>The push consumer instance.</returns>
        public void Unsubscribe(string topic)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Push consumer is not running");
            }

            _subscriptionExpressions.TryRemove(topic, out _);
        }

        internal void ScanAssignments()
        {
            try
            {
                Logger.LogDebug($"Start to scan assignments periodically, clientId={ClientId}");
                foreach (var (topic, filterExpression) in _subscriptionExpressions)
                {
                    var existed = _cacheAssignments.GetValueOrDefault(topic);

                    var queryAssignmentTask = QueryAssignment(topic);
                    queryAssignmentTask.ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        {
                            Logger.LogError(task.Exception, "Exception raised while scanning the assignments," +
                                                            $" topic={topic}, clientId={ClientId}");
                            return;
                        }

                        var latest = task.Result;
                        if (latest.GetAssignmentList().Count == 0)
                        {
                            if (existed == null || existed.GetAssignmentList().Count == 0)
                            {
                                Logger.LogInformation("Acquired empty assignments from remote, would scan later," +
                                                      $" topic={topic}, clientId={ClientId}");
                                return;
                            }

                            Logger.LogInformation("Attention!!! acquired empty assignments from remote, but" +
                                                  $" existed assignments are not empty, topic={topic}," +
                                                  $" clientId={ClientId}");
                        }

                        if (!latest.Equals(existed))
                        {
                            Logger.LogInformation($"Assignments of topic={topic} has changed, {existed} =>" +
                                                  $" {latest}, clientId={ClientId}");
                            SyncProcessQueue(topic, latest, filterExpression);
                            _cacheAssignments[topic] = latest;
                            return;
                        }

                        Logger.LogDebug($"Assignments of topic={topic} remain the same," +
                                        $" assignments={existed}, clientId={ClientId}");
                        // Process queue may be dropped, need to be synchronized anyway.
                        SyncProcessQueue(topic, latest, filterExpression);
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Exception raised while scanning the assignments for all topics, clientId={ClientId}");
            }
        }

        private void SyncProcessQueue(string topic, Assignments assignments, FilterExpression filterExpression)
        {
            var latest = new HashSet<MessageQueue>();
            var assignmentList = assignments.GetAssignmentList();
            foreach (var assignment in assignmentList)
            {
                latest.Add(assignment.MessageQueue);
            }

            var activeMqs = new HashSet<MessageQueue>();
            foreach (var (mq, pq) in _processQueueTable)
            {
                if (!topic.Equals(mq.Topic))
                {
                    continue;
                }

                if (!latest.Contains(mq))
                {
                    Logger.LogInformation($"Drop message queue according to the latest assignmentList," +
                                          $" mq={mq}, clientId={ClientId}");
                    DropProcessQueue(mq);
                    continue;
                }

                if (pq.Expired())
                {
                    Logger.LogWarning($"Drop message queue because it is expired," +
                                      $" mq={mq}, clientId={ClientId}");
                    DropProcessQueue(mq);
                    continue;
                }
                activeMqs.Add(mq);
            }

            foreach (var mq in latest)
            {
                if (activeMqs.Contains(mq))
                {
                    continue;
                }
                var processQueue = CreateProcessQueue(mq, filterExpression);
                if (processQueue != null)
                {
                    Logger.LogInformation($"Start to fetch message from remote, mq={mq}, clientId={ClientId}");
                    processQueue.FetchMessageImmediately();
                }
            }
        }

        internal Task<Assignments> QueryAssignment(string topic)
        {
            var pickEndpointsTask = PickEndpointsToQueryAssignments(topic);
            return pickEndpointsTask.ContinueWith(task0 =>
            {
                if (task0 is { IsFaulted: true, Exception: { } })
                {
                    throw task0.Exception;
                }

                var endpoints = task0.Result;
                var request = WrapQueryAssignmentRequest(topic);
                var requestTimeout = _clientConfig.RequestTimeout;
                var queryAssignmentTask = ClientManager.QueryAssignment(endpoints, request, requestTimeout);

                return queryAssignmentTask.ContinueWith(task1 =>
                {
                    if (task1 is { IsFaulted: true, Exception: { } })
                    {
                        throw task1.Exception;
                    }

                    var response = task1.Result.Response;
                    var status = response.Status;
                    StatusChecker.Check(status, request, task1.Result.RequestId);
                    var assignmentList = response.Assignments
                        .Select(assignment => new Assignment(new MessageQueue(assignment.MessageQueue)))
                        .ToList();
                    return Task.FromResult(new Assignments(assignmentList));
                }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
            }, TaskContinuationOptions.ExecuteSynchronously).Unwrap();
        }

        private Task<Endpoints> PickEndpointsToQueryAssignments(string topic)
        {
            var getRouteDataTask = GetRouteData(topic);
            return getRouteDataTask.ContinueWith(task =>
            {
                if (task is { IsFaulted: true, Exception: { } })
                {
                    throw task.Exception;
                }

                var topicRouteData = task.Result;
                return topicRouteData.PickEndpointsToQueryAssignments();
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private QueryAssignmentRequest WrapQueryAssignmentRequest(string topic)
        {
            var topicResource = new Proto.Resource
            {
                ResourceNamespace = _clientConfig.Namespace,
                Name = topic
            };
            return new QueryAssignmentRequest
            {
                Topic = topicResource,
                Group = GetProtobufGroup(),
                Endpoints = Endpoints.ToProtobuf()
            };
        }

        /// <summary>
        /// Drops the <see cref="ProcessQueue"/> by <see cref="MessageQueue"/>.
        /// <see cref="ProcessQueue"/> must be removed before it is dropped.
        /// </summary>
        /// <param name="mq">The message queue.</param>
        internal void DropProcessQueue(MessageQueue mq)
        {
            if (_processQueueTable.TryRemove(mq, out var pq))
            {
                pq.Drop();
            }
        }

        /// <summary>
        /// Creates a process queue and adds it into the <see cref="_processQueueTable"/>.
        /// Returns <see cref="ProcessQueue"/> if the mapped process queue already exists.
        /// </summary>
        /// <remarks>
        /// This function and <see cref="DropProcessQueue"/> ensure that a process queue is not dropped if
        /// it is contained in <see cref="_processQueueTable"/>. Once a process queue is dropped, it must have been
        /// removed from <see cref="_processQueueTable"/>.
        /// </remarks>
        /// <param name="mq">The message queue.</param>
        /// <param name="filterExpression">The filter expression of the topic.</param>
        /// <returns>A process queue.</returns>
        protected ProcessQueue CreateProcessQueue(MessageQueue mq, FilterExpression filterExpression)
        {
            var processQueue = new ProcessQueue(this, mq, filterExpression, _receiveMsgCts, _ackMsgCts,
                _changeInvisibleDurationCts, _forwardMsgToDeadLetterQueueCts);
            if (_processQueueTable.TryGetValue(mq, out var previous))
            {
                return null;
            }
            _processQueueTable.TryAdd(mq, processQueue);
            return processQueue;
        }

        public async Task AckMessage(MessageView messageView)
        {
            if (State.Running != State)
            {
                throw new InvalidOperationException("Push consumer is not running");
            }

            var request = WrapAckMessageRequest(messageView);
            var invocation = await ClientManager.AckMessage(messageView.MessageQueue.Broker.Endpoints, request,
                ClientConfig.RequestTimeout);
            StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
        }

        protected override IEnumerable<string> GetTopics()
        {
            return _subscriptionExpressions.Keys;
        }

        internal override Proto.HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto::HeartbeatRequest
            {
                ClientType = Proto.ClientType.PushConsumer,
                Group = GetProtobufGroup()
            };
        }

        protected internal ChangeInvisibleDurationRequest WrapChangeInvisibleDuration(MessageView messageView,
            TimeSpan invisibleDuration)
        {
            var topicResource = new Proto.Resource
            {
                ResourceNamespace = _clientConfig.Namespace,
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

        protected internal AckMessageRequest WrapAckMessageRequest(MessageView messageView)
        {
            var topicResource = new Proto.Resource
            {
                ResourceNamespace = _clientConfig.Namespace,
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

        protected internal ForwardMessageToDeadLetterQueueRequest WrapForwardMessageToDeadLetterQueueRequest(MessageView messageView)
        {
            var topicResource = new Proto.Resource
            {
                ResourceNamespace = _clientConfig.Namespace,
                Name = messageView.Topic
            };

            return new ForwardMessageToDeadLetterQueueRequest
            {
                Group = GetProtobufGroup(),
                Topic = topicResource,
                ReceiptHandle = messageView.ReceiptHandle,
                MessageId = messageView.MessageId,
                DeliveryAttempt = messageView.DeliveryAttempt,
                MaxDeliveryAttempts = GetRetryPolicy().GetMaxAttempts()
            };
        }

        protected override void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData)
        {
        }

        internal override async void OnVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command)
        {
            var nonce = command.Nonce;
            var messageView = MessageView.FromProtobuf(command.Message);
            var messageId = messageView.MessageId;
            Proto.TelemetryCommand telemetryCommand = null;

            try
            {
                var consumeResult = await _consumeService.Consume(messageView);
                var code = consumeResult == ConsumeResult.SUCCESS ? Code.Ok : Code.FailedToConsumeMessage;
                var status = new Status
                {
                    Code = code
                };
                var verifyMessageResult = new VerifyMessageResult
                {
                    Nonce = nonce
                };
                telemetryCommand = new TelemetryCommand
                {
                    VerifyMessageResult = verifyMessageResult,
                    Status = status
                };
                var (_, session) = GetSession(endpoints);
                await session.WriteAsync(telemetryCommand);
            }
            catch (Exception e)
            {
                Logger.LogError(e,
                    $"Failed to send message verification result command, endpoints={Endpoints}, command={telemetryCommand}, messageId={messageId}, clientId={ClientId}");
            }
        }

        internal override NotifyClientTerminationRequest WrapNotifyClientTerminationRequest()
        {
            return new NotifyClientTerminationRequest()
            {
                Group = GetProtobufGroup()
            };
        }

        internal int GetQueueSize()
        {
            return _processQueueTable.Count;
        }

        internal int CacheMessageBytesThresholdPerQueue()
        {
            var size = this.GetQueueSize();
            // All process queues are removed, no need to cache messages.
            return size <= 0 ? 0 : Math.Max(1, _maxCacheMessageSizeInBytes / size);
        }

        internal int CacheMessageCountThresholdPerQueue()
        {
            var size = this.GetQueueSize();
            // All process queues are removed, no need to cache messages.
            if (size <= 0)
            {
                return 0;
            }

            return Math.Max(1, _maxCacheMessageCount / size);
        }

        internal override Settings GetSettings()
        {
            return _pushSubscriptionSettings;
        }

        /// <summary>
        /// Gets the load balancing group for the consumer.
        /// </summary>
        /// <returns>The consumer load balancing group.</returns>
        public string GetConsumerGroup()
        {
            return _consumerGroup;
        }

        public PushSubscriptionSettings GetPushConsumerSettings()
        {
            return _pushSubscriptionSettings;
        }

        /// <summary>
        /// Lists the existing subscription expressions in the push consumer.
        /// </summary>
        /// <returns>Collections of the subscription expressions.</returns>
        public ConcurrentDictionary<string, FilterExpression> GetSubscriptionExpressions()
        {
            return _subscriptionExpressions;
        }

        public IRetryPolicy GetRetryPolicy()
        {
            return _pushSubscriptionSettings.GetRetryPolicy();
        }

        public ConsumeService GetConsumeService()
        {
            return _consumeService;
        }

        private Proto.Resource GetProtobufGroup()
        {
            return new Proto.Resource()
            {
                ResourceNamespace = _clientConfig.Namespace,
                Name = ConsumerGroup
            };
        }

        public class Builder
        {
            private ClientConfig _clientConfig;
            private string _consumerGroup;
            private ConcurrentDictionary<string, FilterExpression> _subscriptionExpressions;
            private IMessageListener _messageListener;
            private int _maxCacheMessageCount = 1024;
            private int _maxCacheMessageSizeInBytes = 64 * 1024 * 1024;
            private int _consumptionThreadCount = 20;

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

            public Builder SetSubscriptionExpression(Dictionary<string, FilterExpression> subscriptionExpressions)
            {
                Preconditions.CheckArgument(null != subscriptionExpressions,
                    "subscriptionExpressions should not be null");
                Preconditions.CheckArgument(subscriptionExpressions!.Count != 0,
                    "subscriptionExpressions should not be empty");
                _subscriptionExpressions = new ConcurrentDictionary<string, FilterExpression>(subscriptionExpressions!);
                return this;
            }

            public Builder SetMessageListener(IMessageListener messageListener)
            {
                Preconditions.CheckArgument(null != messageListener,
                    "messageListener should not be null");
                _messageListener = messageListener;
                return this;
            }

            public Builder SetMaxCacheMessageCount(int maxCacheMessageCount)
            {
                Preconditions.CheckArgument(maxCacheMessageCount > 0,
                    "maxCacheMessageCount should be positive");
                _maxCacheMessageCount = maxCacheMessageCount;
                return this;
            }

            public Builder SetMaxCacheMessageSizeInBytes(int maxCacheMessageSizeInBytes)
            {
                Preconditions.CheckArgument(maxCacheMessageSizeInBytes > 0,
                    "maxCacheMessageSizeInBytes should be positive");
                _maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
                return this;
            }

            public Builder SetConsumptionThreadCount(int consumptionThreadCount)
            {
                Preconditions.CheckArgument(consumptionThreadCount > 0,
                    "consumptionThreadCount should be positive");
                _consumptionThreadCount = consumptionThreadCount;
                return this;
            }

            public async Task<PushConsumer> Build()
            {
                Preconditions.CheckArgument(null != _clientConfig, "clientConfig has not been set yet");
                Preconditions.CheckArgument(null != _consumerGroup, "consumerGroup has not been set yet");
                Preconditions.CheckArgument(!_subscriptionExpressions!.IsEmpty,
                    "subscriptionExpressions has not been set yet");
                Preconditions.CheckArgument(null != _messageListener, "messageListener has not been set yet");
                var pushConsumer = new PushConsumer(_clientConfig, _consumerGroup, _subscriptionExpressions,
                    _messageListener, _maxCacheMessageCount,
                    _maxCacheMessageSizeInBytes, _consumptionThreadCount);
                await pushConsumer.Start();
                return pushConsumer;
            }
        }
    }
}