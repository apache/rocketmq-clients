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

        public SimpleConsumer(AccessPoint accessPoint,
        string resourceNamespace, string group)
        : base(accessPoint, resourceNamespace)
        {
            _fifo = false;
            _subscriptions = new ConcurrentDictionary<string, rmq.SubscriptionEntry>();
            _topicAssignments = new ConcurrentDictionary<string, List<rmq.Assignment>>();
            _group = group;
        }

        public override void BuildClientSetting(rmq::Settings settings)
        {
            base.BuildClientSetting(settings);

            settings.ClientType = rmq::ClientType.SimpleConsumer;
            settings.Subscription = new rmq::Subscription();
            settings.Subscription.Group = new rmq::Resource();
            settings.Subscription.Group.Name = _group;
            settings.Subscription.Group.ResourceNamespace = ResourceNamespace;

            foreach (var kv in _subscriptions)
            {
                settings.Subscription.Subscriptions.Add(kv.Value);
            }
        }

        public override async Task Start()
        {
            await base.Start();
            
            // Scan load assignment periodically
            schedule(async () =>
            {
                while (!_scanAssignmentCts.IsCancellationRequested)
                {
                    await ScanLoadAssignments();                    
                }
            }, 30, _scanAssignmentCts.Token);

            await ScanLoadAssignments();
        }

        public override async Task Shutdown()
        {
            await base.Shutdown();
            if (!await NotifyClientTermination())
            {
                Logger.Warn("Failed to NotifyClientTermination");
            }
        }

        private async Task ScanLoadAssignments()
        {

            List<Task<List<rmq::Assignment>>> tasks = new List<Task<List<rmq.Assignment>>>();
            List<string> topics = new List<string>();
            foreach (var sub in _subscriptions)
            {
                var request = new rmq::QueryAssignmentRequest();
                request.Topic = new rmq::Resource();
                request.Topic.ResourceNamespace = ResourceNamespace;
                request.Topic.Name = sub.Key;
                topics.Add(sub.Key);
                request.Group = new rmq::Resource();
                request.Group.Name = _group;
                request.Group.ResourceNamespace = ResourceNamespace;

                request.Endpoints = new rmq::Endpoints();
                request.Endpoints.Scheme = rmq.AddressScheme.Ipv4;
                var address = new rmq::Address();
                address.Host = _accessPoint.Host;
                address.Port = _accessPoint.Port;
                request.Endpoints.Addresses.Add(address);

                var metadata = new Metadata();
                Signature.sign(this, metadata);
                tasks.Add(Manager.QueryLoadAssignment(_accessPoint.TargetUrl(), metadata, request, TimeSpan.FromSeconds(3)));
            }

            List<rmq.Assignment>[] list = await Task.WhenAll(tasks);

            var i = 0;
            foreach (var assignments in list)
            {
                string topic = topics[i];
                if (null == assignments || 0 == assignments.Count)
                {
                    Logger.Warn($"Faild to acquire assignments. Topic={topic}, Group={_group}");
                    ++i;
                    continue;
                }
                Logger.Debug($"Assignments received. Topic={topic}, Group={_group}");
                _topicAssignments.AddOrUpdate(topic, assignments, (t, prev) => assignments);
                ++i;
            }
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {
            request.ClientType = rmq::ClientType.SimpleConsumer;
            request.Group = new rmq::Resource();
            request.Group.Name = _group;
            request.Group.ResourceNamespace = ResourceNamespace;
        }

        public void Subscribe(string topic, rmq::FilterType filterType, string expression)
        {
            var entry = new rmq::SubscriptionEntry();
            entry.Topic = new rmq::Resource();
            entry.Topic.Name = topic;
            entry.Topic.ResourceNamespace = ResourceNamespace;
            entry.Expression = new rmq::FilterExpression();
            entry.Expression.Type = filterType;
            entry.Expression.Expression = expression;
            _subscriptions.AddOrUpdate(topic, entry, (k, prev) => entry);
            AddTopicOfInterest(topic);
        }

        public override void OnSettingsReceived(rmq.Settings settings)
        {
            base.OnSettingsReceived(settings);

            if (settings.Subscription.Fifo)
            {
                _fifo = true;
                Logger.Info($"#OnSettingsReceived: Group {_group} is FIFO");
            }
        }

        public async Task<List<Message>> Receive(int batchSize, TimeSpan timeout)
        {
            var messageQueue = NextQueue();
            if (null == messageQueue)
            {
                Logger.Debug("NextQueue returned null");
                return new List<Message>();
            }

            var request = new rmq.ReceiveMessageRequest();
            request.Group = new rmq.Resource();
            request.Group.ResourceNamespace = ResourceNamespace;
            request.Group.Name = _group;

            request.MessageQueue = new rmq.MessageQueue();
            request.MessageQueue.MergeFrom(messageQueue);
            request.BatchSize = batchSize;
            
            // Client is responsible of extending message invisibility duration
            request.AutoRenew = false;
            
            var targetUrl = Utilities.TargetUrl(messageQueue);
            var metadata = new Metadata();
            Signature.sign(this, metadata);
            
            return await Manager.ReceiveMessage(targetUrl, metadata, request, timeout);
        }


        public async Task Ack(Message message)
        {
            var request = new rmq.AckMessageRequest();
            request.Group = new rmq.Resource();
            request.Group.ResourceNamespace = ResourceNamespace;
            request.Group.Name = _group;

            request.Topic = new rmq.Resource();
            request.Topic.ResourceNamespace = ResourceNamespace;
            request.Topic.Name = message.Topic;
            
            var entry = new rmq.AckMessageEntry();
            request.Entries.Add(entry);
            entry.MessageId = message.MessageId;
            entry.ReceiptHandle = message._receiptHandle;

            var targetUrl = message._sourceHost;
            var metadata = new Metadata();
            Signature.sign(this, metadata);
            await Manager.Ack(targetUrl, metadata, request, RequestTimeout);
        }

        public async Task ChangeInvisibleDuration(Message message, TimeSpan invisibleDuration)
        {
            var request = new rmq.ChangeInvisibleDurationRequest();
            request.Group = new rmq.Resource();
            request.Group.ResourceNamespace = ResourceNamespace;
            request.Group.Name = _group;

            request.Topic = new rmq.Resource();
            request.Topic.ResourceNamespace = ResourceNamespace;
            request.Topic.Name = message.Topic;

            request.ReceiptHandle = message._receiptHandle;
            request.MessageId = message.MessageId;
            
            request.InvisibleDuration = Duration.FromTimeSpan(invisibleDuration);

            var targetUrl = message._sourceHost;
            var metadata = new Metadata();
            Signature.sign(this, metadata);
            await Manager.ChangeInvisibleDuration(targetUrl, metadata, request, RequestTimeout);
        }
        
        private rmq.MessageQueue NextQueue()
        {
            if (_topicAssignments.IsEmpty)
            {
                return null;
            }
            
            UInt32 topicSeq = CurrentTopicSequence.Value;
            CurrentTopicSequence.Value = topicSeq + 1;

            var total = _topicAssignments.Count;
            var topicIndex = topicSeq % total;
            var topic = _topicAssignments.Keys.Skip((int)topicIndex).First();
            
            UInt32 queueSeq = CurrentQueueSequence.Value;
            CurrentQueueSequence.Value = queueSeq + 1;
            List<rmq.Assignment> assignments;
            if (_topicAssignments.TryGetValue(topic, out assignments))
            {
                if (null == assignments)
                {
                    return null;
                }
                var idx = queueSeq % assignments.Count;
                return assignments[(int)idx].MessageQueue;

            }

            return null;
        }

        private ThreadLocal<UInt32> CurrentTopicSequence = new ThreadLocal<UInt32>(true);
        private ThreadLocal<UInt32> CurrentQueueSequence = new ThreadLocal<UInt32>(true);

        private readonly string _group;
        private bool _fifo;
        private readonly ConcurrentDictionary<string, rmq::SubscriptionEntry> _subscriptions;
        private readonly ConcurrentDictionary<string, List<rmq.Assignment>> _topicAssignments;
        private readonly CancellationTokenSource _scanAssignmentCts = new CancellationTokenSource();
    }
}