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
using System.Linq;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class MessageQueue
    {
        public MessageQueue(Proto::MessageQueue messageQueue)
        {
            TopicResource = new Resource(messageQueue.Topic);
            QueueId = messageQueue.Id;
            Permission = PermissionHelper.FromProtobuf(messageQueue.Permission);
            var messageTypes = messageQueue.AcceptMessageTypes
                .Select(MessageTypeHelper.FromProtobuf).ToList();

            AcceptMessageTypes = messageTypes;
            Broker = new Broker(messageQueue.Broker);
        }

        public Broker Broker { get; }

        private Resource TopicResource { get; }

        public Permission Permission { get; }

        public int QueueId { get; }

        public List<MessageType> AcceptMessageTypes { get; }

        public string Topic => TopicResource.Name;

        public override string ToString()
        {
            return $"{Broker.Name}.{TopicResource}.{QueueId}";
        }

        public Proto.MessageQueue ToProtobuf()
        {
            var messageTypes = AcceptMessageTypes.Select(MessageTypeHelper.ToProtobuf).ToList();

            return new Proto.MessageQueue
            {
                Topic = TopicResource.ToProtobuf(),
                Id = QueueId,
                Permission = PermissionHelper.ToProtobuf(Permission),
                Broker = Broker.ToProtobuf(),
                AcceptMessageTypes = { messageTypes }
            };
        }
    }
}