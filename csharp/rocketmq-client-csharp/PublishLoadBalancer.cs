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

namespace org.apache.rocketmq
{
    public class PublishLoadBalancer
    {
        public PublishLoadBalancer(TopicRouteData route)
        {
            this.partitions = new List<Partition>();
            foreach (var partition in route.Partitions)
            {
                if (Permission.NONE == partition.Permission)
                {
                    continue;
                }

                if (Permission.READ == partition.Permission)
                {
                    continue;
                }

                this.partitions.Add(partition);
            }

            this.partitions.Sort();
            Random random = new Random();
            this.roundRobinIndex = random.Next(0, this.partitions.Count);
        }

        public void update(TopicRouteData route)
        {
            List<Partition> partitions = new List<Partition>();
            foreach (var partition in route.Partitions)
            {
                if (Permission.NONE == partition.Permission)
                {
                    continue;
                }

                if (Permission.READ == partition.Permission)
                {
                    continue;
                }
                partitions.Add(partition);
            }
            partitions.Sort();
            this.partitions = partitions;
        }

        /**
         * Accept a partition iff its broker is different.
         */
        private bool accept(List<Partition> existing, Partition partition)
        {
            if (0 == existing.Count)
            {
                return true;
            }

            foreach (var item in existing)
            {
                if (item.Broker.Equals(partition.Broker))
                {
                    return false;
                }
            }
            return true;
        }

        public List<Partition> select(int maxAttemptTimes)
        {
            List<Partition> result = new List<Partition>();

            List<Partition> all = this.partitions;
            if (0 == all.Count)
            {
                return result;
            }
            int start = ++roundRobinIndex;
            int found = 0;

            for (int i = 0; i < all.Count; i++)
            {
                int idx = ((start + i) & int.MaxValue) % all.Count;
                if (accept(result, all[idx]))
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

        private List<Partition> partitions;

        private int roundRobinIndex;
    }
}