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

using System.Collections.Generic;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class MessageQueue
    {
        public MessageQueue(rmq::MessageQueue messageQueue)
        {
            TopicResource = new Resource(messageQueue.Topic);
            QueueId = messageQueue.Id;
            Permission = PermissionHelper.FromProtobuf(messageQueue.Permission);
            var messageTypes = new List<MessageType>();
            foreach (var acceptMessageType in messageQueue.AcceptMessageTypes)
            {
                var messageType = MessageTypeHelper.FromProtobuf(acceptMessageType);
                messageTypes.Add(messageType);
            }

            AcceptMessageTypes = messageTypes;
            Broker = new Broker(messageQueue.Broker);
        }

        public Broker Broker { get; }

        public Resource TopicResource { get; }

        public Permission Permission { get; }

        public int QueueId { get; }

        public List<MessageType> AcceptMessageTypes { get; }

        public string Topic
        {
            get { return TopicResource.Name; }
        }

        public override string ToString()
        {
            return $"{Broker.Name}.{TopicResource}.{QueueId}";
        }
    }
}