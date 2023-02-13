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

namespace Org.Apache.Rocketmq
{
    internal sealed class SubscriptionLoadBalancer
    {
        private readonly List<MessageQueue> _messageQueues;
        private int _roundRobinIndex;

        public SubscriptionLoadBalancer(TopicRouteData topicRouteData)
        {
            _messageQueues = new List<MessageQueue>();
            foreach (var mq in topicRouteData.MessageQueues.Where(mq => PermissionHelper.IsReadable(mq.Permission))
                         .Where(mq => Utilities.MasterBrokerId == mq.Broker.Id))
            {
                _messageQueues.Add(mq);
            }

            var random = new Random();
            _roundRobinIndex = random.Next(0, _messageQueues.Count);
        }

        public MessageQueue TakeMessageQueue()
        {
            var next = ++_roundRobinIndex;
            var index = Utilities.GetPositiveMod(next, _messageQueues.Count);
            return _messageQueues[index];
        }
    }
}