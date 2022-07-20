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
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class PublishLoadBalancer
    {
        public PublishLoadBalancer(TopicRouteData route)
        {
            this._messageQueues = new List<rmq::MessageQueue>();
            foreach (var messageQueue in route.MessageQueues)
            {
                if (rmq::Permission.Unspecified == messageQueue.Permission)
                {
                    continue;
                }

                if (rmq::Permission.Read == messageQueue.Permission)
                {
                    continue;
                }

                this._messageQueues.Add(messageQueue);
            }

            this._messageQueues.Sort(Utilities.CompareMessageQueue);
            Random random = new Random();
            this._roundRobinIndex = random.Next(0, this._messageQueues.Count);
        }

        public void Update(TopicRouteData route)
        {
            List<rmq::MessageQueue> partitions = new List<rmq::MessageQueue>();
            foreach (var partition in route.MessageQueues)
            {
                if (rmq::Permission.Unspecified == partition.Permission)
                {
                    continue;
                }

                if (rmq::Permission.Read == partition.Permission)
                {
                    continue;
                }
                partitions.Add(partition);
            }
            partitions.Sort();
            this._messageQueues = partitions;
        }

        /**
         * Accept a partition iff its broker is different.
         */
        private bool Accept(List<rmq::MessageQueue> existing, rmq::MessageQueue messageQueue)
        {
            if (0 == existing.Count)
            {
                return true;
            }

            foreach (var item in existing)
            {
                if (item.Broker.Equals(messageQueue.Broker))
                {
                    return false;
                }
            }
            return true;
        }

        public List<rmq::MessageQueue> Select(int maxAttemptTimes)
        {
            List<rmq::MessageQueue> result = new List<rmq::MessageQueue>();

            List<rmq::MessageQueue> all = this._messageQueues;
            if (0 == all.Count)
            {
                return result;
            }
            int start = ++_roundRobinIndex;
            int found = 0;

            for (int i = 0; i < all.Count; i++)
            {
                int idx = ((start + i) & int.MaxValue) % all.Count;
                if (Accept(result, all[idx]))
                {
                    result.Add(all[idx]);
                    if (++found >= maxAttemptTimes)
                    {
                        break;
                    }
                }
            }

            return result;
        }

        private List<rmq::MessageQueue> _messageQueues;

        private int _roundRobinIndex;
    }
}