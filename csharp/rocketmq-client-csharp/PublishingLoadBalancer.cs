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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Org.Apache.Rocketmq
{
    public class PublishingLoadBalancer
    {
        private static readonly Random Random = new Random();

        private readonly List<MessageQueue> _messageQueues;
        private int _index;

        public PublishingLoadBalancer(TopicRouteData topicRouteData) : this(Random.Next(), topicRouteData)
        {
        }

        private PublishingLoadBalancer(int index, TopicRouteData topicRouteData)
        {
            _index = index;
            _messageQueues = new List<MessageQueue>();
            foreach (var mq in topicRouteData.MessageQueues.Where(messageQueue =>
                         PermissionHelper.IsWritable(messageQueue.Permission) &&
                         Utilities.MasterBrokerId == messageQueue.Broker.Id))
            {
                _messageQueues.Add(mq);
            }
        }

        internal PublishingLoadBalancer Update(TopicRouteData topicRouteData)
        {
            return new PublishingLoadBalancer(_index, topicRouteData);
        }


        public MessageQueue TakeMessageQueueByMessageGroup(string messageGroup)
        {
            // TODO: use SipHash24 algorithm
            var index = Utilities.GetPositiveMod(messageGroup.GetHashCode(), _messageQueues.Count);
            return _messageQueues[index];
        }

        public List<MessageQueue> TakeMessageQueues(HashSet<Endpoints> excluded, int count)
        {
            var next = Interlocked.Increment(ref _index);
            var candidates = new List<MessageQueue>();
            var candidateBrokerNames = new HashSet<string>();

            foreach (var mq in _messageQueues.Select(_ => Utilities.GetPositiveMod(next++, _messageQueues.Count))
                         .Select(index => _messageQueues[index]))
            {
                if (!excluded.Contains(mq.Broker.Endpoints) && !candidateBrokerNames.Contains(mq.Broker.Name))
                {
                    candidateBrokerNames.Add(mq.Broker.Name);
                    candidates.Add(mq);
                }

                if (candidates.Count >= count)
                {
                    return candidates;
                }
            }

            if (candidates.Count != 0)
            {
                return candidates;
            }

            foreach (var mq in _messageQueues.Select(_ => Utilities.GetPositiveMod(next++, _messageQueues.Count))
                         .Select(positiveMod => _messageQueues[positiveMod]))
            {
                if (!candidateBrokerNames.Contains(mq.Broker.Name))
                {
                    candidateBrokerNames.Add(mq.Broker.Name);
                    candidates.Add(mq);
                }

                if (candidates.Count >= count)
                {
                    break;
                }
            }

            return candidates;
        }
    }
}