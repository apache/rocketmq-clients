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
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class TopicRouteData : IEquatable<TopicRouteData>
    {
        private int _index = 0;

        public TopicRouteData(IEnumerable<Proto.MessageQueue> messageQueues)
        {
            var messageQueuesList = messageQueues.Select(mq => new MessageQueue(mq)).ToList();

            MessageQueues = messageQueuesList;
        }

        public List<MessageQueue> MessageQueues { get; }

        public Endpoints PickEndpointsToQueryAssignments()
        {
            var nextIndex = Interlocked.Increment(ref _index) - 1;
            foreach (var mq in MessageQueues)
            {
                var modIndex = Mod(nextIndex++, MessageQueues.Count);
                var curMessageQueue = MessageQueues[modIndex];

                if (Utilities.MasterBrokerId != curMessageQueue.Broker.Id)
                {
                    continue;
                }
                if (Permission.None.Equals(curMessageQueue.Permission))
                {
                    continue;
                }
                return curMessageQueue.Broker.Endpoints;
            }
            throw new NotFoundException("Failed to pick endpoints to query assignment");
        }

        private int Mod(int x, int m)
        {
            if (m <= 0)
            {
                throw new ArgumentException("Modulus must be positive", nameof(m));
            }
            var result = x % m;
            return result >= 0 ? result : result + m;
        }

        public bool Equals(TopicRouteData other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return ReferenceEquals(this, other) || Equals(MessageQueues, other.MessageQueues);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == GetType() && Equals((TopicRouteData)obj);
        }

        public override int GetHashCode()
        {
            return (MessageQueues != null ? MessageQueues.GetHashCode() : 0);
        }

        public override string ToString()
        {
            var mqs = MessageQueues.Select(mq => mq.ToString()).ToList();
            return $"[{string.Join(",", mqs)}]";
        }
    }
}