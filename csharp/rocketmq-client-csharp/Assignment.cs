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

namespace Org.Apache.Rocketmq
{
    public class Assignment
    {
        public Assignment(MessageQueue messageQueue)
        {
            MessageQueue = messageQueue ?? throw new ArgumentNullException(nameof(messageQueue));
        }

        public MessageQueue MessageQueue { get; }

        public override bool Equals(object obj)
        {
            if (this == obj) return true;
            if (obj == null || GetType() != obj.GetType()) return false;

            var other = (Assignment)obj;
            return EqualityComparer<MessageQueue>.Default.Equals(MessageQueue, other.MessageQueue);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<MessageQueue>.Default.GetHashCode(MessageQueue);
        }

        public override string ToString()
        {
            return $"Assignment{{messageQueue={MessageQueue}}}";
        }
    }
}