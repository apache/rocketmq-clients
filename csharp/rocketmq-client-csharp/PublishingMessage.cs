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
using System.IO;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Provides a view for message to publish.
    /// </summary>
    public class PublishingMessage : Message
    {
        public MessageType MessageType { get; }

        internal string MessageId { get; }

        public PublishingMessage(Message message, PublishingSettings publishingSettings, bool txEnabled) : base(
            message)
        {
            var maxBodySizeBytes = publishingSettings.GetMaxBodySizeBytes();
            if (message.Body.Length > maxBodySizeBytes)
            {
                throw new IOException($"Message body size exceed the threshold, max size={maxBodySizeBytes} bytes");
            }

            // Generate message id.
            MessageId = MessageIdGenerator.GetInstance().Next();
            // For NORMAL message.
            if (string.IsNullOrEmpty(message.MessageGroup) && !message.DeliveryTimestamp.HasValue &&
                !txEnabled)
            {
                MessageType = MessageType.Normal;
                return;
            }

            // For FIFO message.
            if (!string.IsNullOrEmpty(message.MessageGroup) && !txEnabled)
            {
                MessageType = MessageType.Fifo;
                return;
            }

            // For DELAY message.
            if (message.DeliveryTimestamp.HasValue && !txEnabled)
            {
                MessageType = MessageType.Delay;
                return;
            }

            // For TRANSACTION message.
            if (!string.IsNullOrEmpty(message.MessageGroup) || message.DeliveryTimestamp.HasValue || !txEnabled)
            {
                throw new InternalErrorException(
                    "Transactional message should not set messageGroup or deliveryTimestamp");
            }

            MessageType = MessageType.Transaction;
        }

        public Proto::Message ToProtobuf(int queueId)
        {
            var systemProperties = new Proto.SystemProperties
            {
                Keys = { Keys },
                MessageId = MessageId,
                BornTimestamp = Timestamp.FromDateTime(DateTime.UtcNow),
                BornHost = Utilities.GetHostName(),
                BodyEncoding = EncodingHelper.ToProtobuf(MqEncoding.Identity),
                QueueId = queueId,
                MessageType = MessageTypeHelper.ToProtobuf(MessageType)
            };
            if (null != Tag)
            {
                systemProperties.Tag = Tag;
            }

            if (DeliveryTimestamp.HasValue)
            {
                systemProperties.DeliveryTimestamp = Timestamp.FromDateTime(DeliveryTimestamp.Value.ToUniversalTime());
            }

            if (null != MessageGroup)
            {
                systemProperties.MessageGroup = MessageGroup;
            }

            var topicResource = new Proto.Resource
            {
                Name = Topic
            };
            return new Proto.Message
            {
                Topic = topicResource,
                Body = ByteString.CopyFrom(Body),
                SystemProperties = systemProperties,
                UserProperties = { Properties }
            };
        }
    }
}