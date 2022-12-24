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
using rmq = Apache.Rocketmq.V2;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using Grpc.Core;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf.WellKnownTypes;

namespace Org.Apache.Rocketmq
{
    public class SimpleConsumer : Client
    {

        public SimpleConsumer(string accessUrl, string group)
        : base(accessUrl)
        {
            _subscriptions = new();
            _topicAssignments = new();
            _group = group;
        }

        public override void BuildClientSetting(rmq::Settings settings)
        {
            base.BuildClientSetting(settings);

            settings.ClientType = rmq::ClientType.SimpleConsumer;
            settings.Subscription = new rmq::Subscription
            {
                Group = new rmq::Resource
                {
                    Name = _group,
                    ResourceNamespace = ResourceNamespace
                }
            };

            foreach (var kv in _subscriptions)
            {
                settings.Subscription.Subscriptions.Add(kv.Value);
            }
            Logger.Info($"ClientSettings built OK. {settings}");
        }

        public override async Task Start()
        {
            await base.Start();
            
            // Scan load assignment periodically
            Schedule(async () =>
            {
                Logger.Debug("Scan load assignments by schedule");
                await ScanLoadAssignments();
            }, 30, _scanAssignmentCts.Token);

            await ScanLoadAssignments();
            Logger.Debug("Step of #Start: ScanLoadAssignments completed");
        }

        public override async Task Shutdown()
        {
            _scanAssignmentCts.Cancel();
            await base.Shutdown();
            var group = new rmq.Resource()
            {
                Name = _group,
                ResourceNamespace = "",
            };
            if (!await NotifyClientTermination(group))
            {
                Logger.Warn("Failed to NotifyClientTermination");
            }
        }
        
        /**
         * For 5.x, we can assume there is a load-balancer before gateway nodes.
         */
        private async Task ScanLoadAssignments()
        {
            var tasks = new List<Task<List<rmq.Assignment>>>();
            var topics = new List<string>();
            foreach (var sub in _subscriptions)
            {
                var request = new rmq::QueryAssignmentRequest
                {
                    Topic = new rmq::Resource
                    {
                        ResourceNamespace = ResourceNamespace,
                        Name = sub.Key
                    }
                };
                topics.Add(sub.Key);
                request.Group = new rmq::Resource
                {
                    Name = _group,
                    ResourceNamespace = ResourceNamespace
                };

                request.Endpoints = new rmq::Endpoints
                {
                    Scheme = AccessPoint.HostScheme()
                };
                var address = new rmq::Address
                {
                    Host = AccessPoint.Host,
                    Port = AccessPoint.Port
                };
                request.Endpoints.Addresses.Add(address);

                var metadata = new Metadata();
                Signature.Sign(this, metadata);
                tasks.Add(Manager.QueryLoadAssignment(AccessPoint.TargetUrl(), metadata, request, TimeSpan.FromSeconds(3)));
            }

            var list = await Task.WhenAll(tasks);
            var i = 0;
            foreach (var assignments in list)
            {
                string topic = topics[i++];
                if (null == assignments || 0 == assignments.Count)
                {
                    Logger.Warn($"Faild to acquire assignments. Topic={topic}, Group={_group}");
                    continue;
                }

                Logger.Debug($"Assignments received. Topic={topic}, Group={_group}");
                var newSubscriptionLB = new SubscriptionLoadBalancer(assignments);
                _topicAssignments.AddOrUpdate(topic, newSubscriptionLB, (t, prev) => prev.Update(assignments));
            }
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {
            request.ClientType = rmq::ClientType.SimpleConsumer;
            request.Group = new rmq::Resource
            {
                Name = _group,
                ResourceNamespace = ResourceNamespace
            };
        }

        public void Subscribe(string topic, FilterExpression filterExpression)
        {
            var entry = new rmq::SubscriptionEntry
            {
                Topic = new rmq::Resource
                {
                    Name = topic,
                    ResourceNamespace = ResourceNamespace
                },
                Expression = new rmq::FilterExpression
                {
                    Type = filterExpression.Type switch {
                        ExpressionType.TAG => rmq::FilterType.Tag,
                        ExpressionType.SQL92 => rmq::FilterType.Sql,
                        _ => rmq.FilterType.Tag
                    },
                    Expression = filterExpression.Expression
                }
            };

            _subscriptions.AddOrUpdate(topic, entry, (k, prev) => entry);
            AddTopicOfInterest(topic);
        }

        public void Unsubscribe(string topic)
        {
            _subscriptions.TryRemove(topic, out var _);
            RemoveTopicOfInterest(topic);
        }

        internal override void OnSettingsReceived(rmq.Settings settings)
        {
            base.OnSettingsReceived(settings);

            if (settings.Subscription.Fifo)
            {
                Logger.Info($"#OnSettingsReceived: Group {_group} is FIFO");
            }
        }

        public async Task<List<Message>> Receive(int batchSize, TimeSpan invisibleDuration, TimeSpan? awaitDuration = null)
        {
            var messageQueue = NextQueue();
            if (null == messageQueue)
            {
                throw new TopicRouteException("No topic to receive message from");
            }

            var request = new rmq.ReceiveMessageRequest
            {
                Group = new rmq.Resource
                {
                    ResourceNamespace = ResourceNamespace,
                    Name = _group
                },
                MessageQueue = new rmq.MessageQueue()
            };

            request.MessageQueue.MergeFrom(messageQueue);
            request.BatchSize = batchSize;
            request.InvisibleDuration = Duration.FromTimeSpan(invisibleDuration);
            
            // Client is responsible of extending message invisibility duration
            request.AutoRenew = false;
            
            var targetUrl = Utilities.TargetUrl(messageQueue);
            var metadata = new Metadata();
            Signature.Sign(this, metadata);

            var timeout = (awaitDuration ?? ClientSettings.Subscription.LongPollingTimeout.ToTimeSpan())
                .Add(this.RequestTimeout);

            return await Manager.ReceiveMessage(targetUrl, metadata, request, timeout);
        }


        public async Task Ack(Message message)
        {
            var request = new rmq.AckMessageRequest
            {
                Group = new rmq.Resource
                {
                    ResourceNamespace = ResourceNamespace,
                    Name = _group
                },
                Topic = new rmq.Resource
                {
                    ResourceNamespace = ResourceNamespace,
                    Name = message.Topic
                }
            };

            var entry = new rmq.AckMessageEntry();
            request.Entries.Add(entry);
            entry.MessageId = message.MessageId;
            entry.ReceiptHandle = message._receiptHandle;

            var targetUrl = message._sourceHost;
            var metadata = new Metadata();
            Signature.Sign(this, metadata);
            await Manager.Ack(targetUrl, metadata, request, RequestTimeout);
        }

        public async Task ChangeInvisibleDuration(Message message, TimeSpan invisibleDuration)
        {
            var request = new rmq.ChangeInvisibleDurationRequest
            {
                Group = new rmq.Resource
                {
                    ResourceNamespace = ResourceNamespace,
                    Name = _group
                },
                Topic = new rmq.Resource
                {
                    ResourceNamespace = ResourceNamespace,
                    Name = message.Topic
                },
                ReceiptHandle = message._receiptHandle,
                MessageId = message.MessageId,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration)
            };

            var targetUrl = message._sourceHost;
            var metadata = new Metadata();
            Signature.Sign(this, metadata);
            await Manager.ChangeInvisibleDuration(targetUrl, metadata, request, RequestTimeout);
        }
        
        private rmq.MessageQueue NextQueue()
        {
            if (_topicAssignments.IsEmpty)
            {
                return null;
            }
            
            var topicSeq = _currentTopicSequence.Value;
            _currentTopicSequence.Value = topicSeq + 1;

            var total = _topicAssignments.Count;
            var topicIndex = topicSeq % total;
            var topic = _topicAssignments.Keys.Skip((int)topicIndex).First();

            if (!_topicAssignments.TryGetValue(topic, out var subscriptionLB))
            {
                return null;
            }

            return subscriptionLB.TakeMessageQueue();
        }

        private readonly ThreadLocal<UInt32> _currentTopicSequence = new ThreadLocal<UInt32>(true)
        {
            Value = 0
        };

        private readonly string _group;
        private readonly ConcurrentDictionary<string, rmq::SubscriptionEntry> _subscriptions;
        private readonly ConcurrentDictionary<string, SubscriptionLoadBalancer> _topicAssignments;
        private readonly CancellationTokenSource _scanAssignmentCts = new CancellationTokenSource();
    }
}