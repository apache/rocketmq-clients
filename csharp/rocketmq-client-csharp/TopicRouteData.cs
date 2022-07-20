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
    public class TopicRouteData : IEquatable<TopicRouteData>
    {
        public TopicRouteData(List<rmq::MessageQueue> partitions)
        {
            _messageQueues = partitions;

            _messageQueues.Sort(Utilities.CompareMessageQueue);
        }

        private List<rmq::MessageQueue> _messageQueues;
        public List<rmq::MessageQueue> MessageQueues { get { return _messageQueues; } }

        public bool Equals(TopicRouteData other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(_messageQueues, other._messageQueues);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((TopicRouteData)obj);
        }

        public override int GetHashCode()
        {
            return (_messageQueues != null ? _messageQueues.GetHashCode() : 0);
        }
    }
}