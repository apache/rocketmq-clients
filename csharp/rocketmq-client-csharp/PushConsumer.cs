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
using System.Collections.Concurrent;
using System.Collections.Generic;
using rmq = Apache.Rocketmq.V2;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    public class PushConsumer : Client, IConsumer
    {
        public PushConsumer(AccessPoint accessPoint, string resourceNamespace, string group) : base(accessPoint, resourceNamespace)
        {
            _group = group;
            _topicFilterExpressionMap = new ConcurrentDictionary<string, FilterExpression>();
            _topicAssignmentsMap = new ConcurrentDictionary<string, List<rmq::Assignment>>();
            _processQueueMap = new ConcurrentDictionary<rmq::Assignment, ProcessQueue>();
            _scanAssignmentCTS = new CancellationTokenSource();
            _scanExpiredProcessQueueCTS = new CancellationTokenSource();
        }

        public override async Task Start()
        {
            if (null == _messageListener)
            {
                throw new System.Exception("Bad configuration: message listener is required");
            }

            await base.Start();

            // Step-1: Resolve topic routes
            List<Task<TopicRouteData>> queryRouteTasks = new List<Task<TopicRouteData>>();
            foreach (var item in _topicFilterExpressionMap)
            {
                queryRouteTasks.Add(GetRouteFor(item.Key, true));
            }
            Task.WhenAll(queryRouteTasks).GetAwaiter().GetResult();

            // Step-2: Send heartbeats to all involving brokers so that we may get immediate, valid assignments.
            await Heartbeat();

            // Step-3: Scan load assignments that are assigned to current client
            schedule(async () =>
            {
                await scanLoadAssignments();
            }, 10, _scanAssignmentCTS.Token);

            schedule(() =>
            {
                ScanExpiredProcessQueue();
            }, 10, _scanExpiredProcessQueueCTS.Token);
        }

        public override async Task Shutdown()
        {
            _scanAssignmentCTS.Cancel();
            _scanExpiredProcessQueueCTS.Cancel();

            // Shutdown resources of derived class
            await base.Shutdown();
        }

        private async Task scanLoadAssignments()
        {
            Logger.Debug("Start to scan load assignments from server");
            List<Task<List<rmq::Assignment>>> tasks = new List<Task<List<rmq::Assignment>>>();
            foreach (var item in _topicFilterExpressionMap)
            {
                tasks.Add(scanLoadAssignment(item.Key, _group));
            }
            var result = await Task.WhenAll(tasks);

            foreach (var assignments in result)
            {
                if (assignments.Count == 0)
                {
                    continue;
                }

                checkAndUpdateAssignments(assignments);
            }
            Logger.Debug("Completed scanning load assignments");
        }

        private void ScanExpiredProcessQueue()
        {
            foreach (var item in _processQueueMap)
            {
                if (item.Value.Expired())
                {
                    Task.Run(async () =>
                    {
                        await ExecutePop0(item.Key);
                    });
                }
            }
        }

        private void checkAndUpdateAssignments(List<rmq::Assignment> assignments)
        {
            if (assignments.Count == 0)
            {
                return;
            }

            string topic = assignments[0].MessageQueue.Topic.Name;

            // Compare to generate or cancel pop-cycles
            List<rmq::Assignment> existing;
            _topicAssignmentsMap.TryGetValue(topic, out existing);

            foreach (var assignment in assignments)
            {
                if (null == existing || !existing.Contains(assignment))
                {
                    ExecutePop(assignment);
                }
            }

            if (null != existing)
            {
                foreach (var assignment in existing)
                {
                    if (!assignments.Contains(assignment))
                    {
                        Logger.Info($"Stop receiving messages from {assignment.MessageQueue.ToString()}");
                        CancelPop(assignment);
                    }
                }
            }

        }

        private void ExecutePop(rmq::Assignment assignment)
        {
            var processQueue = new ProcessQueue();
            if (_processQueueMap.TryAdd(assignment, processQueue))
            {
                Task.Run(async () =>
                {
                    await ExecutePop0(assignment);
                });
            }
        }

        private async Task ExecutePop0(rmq::Assignment assignment)
        {
            Logger.Info($"Start to pop {assignment.MessageQueue.ToString()}");
            while (true)
            {
                try
                {
                    ProcessQueue processQueue;
                    if (!_processQueueMap.TryGetValue(assignment, out processQueue))
                    {
                        break;
                    }

                    if (processQueue.Dropped)
                    {
                        break;
                    }

                    List<Message> messages = await base.ReceiveMessage(assignment, _group);
                    processQueue.LastReceiveTime = System.DateTime.UtcNow;

                    // TODO: cache message and dispatch them 

                    List<Message> failed = new List<Message>();
                    await _messageListener.Consume(messages, failed);

                    foreach (var message in failed)
                    {
                        await base.ChangeInvisibleDuration(message._sourceHost, _group, message.Topic, message._receiptHandle, message.MessageId);
                    }

                    foreach (var message in messages)
                    {
                        if (!failed.Contains(message))
                        {
                            bool success = await base.Ack(message._sourceHost, _group, message.Topic, message._receiptHandle, message.MessageId);
                            if (!success)
                            {
                                //TODO: log error.
                            }
                        }
                    }
                }
                catch (System.Exception)
                {
                    // TODO: log exception raised.
                }


            }
        }

        private void CancelPop(rmq::Assignment assignment)
        {
            if (!_processQueueMap.ContainsKey(assignment))
            {
                return;
            }

            ProcessQueue processQueue;
            if (_processQueueMap.Remove(assignment, out processQueue))
            {
                processQueue.Dropped = true;
            }
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {
        }

        public void Subscribe(string topic, string expression, ExpressionType type)
        {
            var filterExpression = new FilterExpression(expression, type);
            _topicFilterExpressionMap[topic] = filterExpression;

        }

        public void RegisterListener(IMessageListener listener)
        {
            if (null != listener)
            {
                _messageListener = listener;
            }
        }

        private string _group;

        private ConcurrentDictionary<string, FilterExpression> _topicFilterExpressionMap;
        private IMessageListener _messageListener;

        private CancellationTokenSource _scanAssignmentCTS;

        private ConcurrentDictionary<string, List<rmq::Assignment>> _topicAssignmentsMap;

        private ConcurrentDictionary<rmq::Assignment, ProcessQueue> _processQueueMap;

        private CancellationTokenSource _scanExpiredProcessQueueCTS;

    }

}