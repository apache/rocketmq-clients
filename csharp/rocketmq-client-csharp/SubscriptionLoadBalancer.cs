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
using System.Threading;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    internal sealed class SubscriptionLoadBalancer
    {
        public List<rmq.Assignment> Assignments { get; private set; }
        private uint index = 0;

        public SubscriptionLoadBalancer(List<rmq.Assignment> assignments)
        {
            Assignments = assignments;
        }

        private SubscriptionLoadBalancer(uint oldIndex, List<rmq.Assignment> assignments)
        {
            index = oldIndex;
            Assignments = assignments;
        }

        public SubscriptionLoadBalancer Update(List<rmq.Assignment> newAssignments)
        {
            return new SubscriptionLoadBalancer(index, newAssignments);
        }

        public rmq.MessageQueue TakeMessageQueue()
        {
            var i = Interlocked.Increment(ref index);
            return Assignments[(int)(i % Assignments.Count)].MessageQueue;
        }
    }
}
