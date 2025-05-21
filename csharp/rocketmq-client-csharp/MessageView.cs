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

using Proto = Apache.Rocketmq.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using System.IO.Hashing;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Provides a read-only view for message.
    /// </summary>
    public class MessageView
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<MessageView>();

        internal readonly MessageQueue MessageQueue;
        internal readonly string ReceiptHandle;
        private readonly long _offset;
        private readonly bool _corrupted;

        private readonly ReadOnlyMemory<byte> _bodyMemory;
        private readonly byte[] _bodyArray;
        private byte[] _bodyMemoryCopy;

        private MessageView(string messageId, string topic, ReadOnlyMemory<byte> bodyMemory, byte[] bodyArray, string tag, string messageGroup,
            DateTime? deliveryTimestamp, List<string> keys, Dictionary<string, string> properties, string bornHost,
            DateTime bornTime, int deliveryAttempt, MessageQueue messageQueue, string receiptHandle, long offset,
            bool corrupted)
        {
            MessageId = messageId;
            Topic = topic;
            Tag = tag;
            MessageGroup = messageGroup;
            DeliveryTimestamp = deliveryTimestamp;
            Keys = keys;
            Properties = properties;
            BornHost = bornHost;
            BornTime = bornTime;
            DeliveryAttempt = deliveryAttempt;
            MessageQueue = messageQueue;
            ReceiptHandle = receiptHandle;
            _bodyMemory = bodyMemory;
            _bodyArray = bodyArray;
            _offset = offset;
            _corrupted = corrupted;
        }

        public string MessageId { get; }

        public string Topic { get; }

        [Obsolete("This property is obsolete and will be removed in the future version. Use BodyMemory instead.")]
        public byte[] Body => _bodyArray ?? (_bodyMemoryCopy ??=_bodyMemory.ToArray());

        public ReadOnlyMemory<byte> BodyMemory => _bodyArray?.AsMemory() ?? _bodyMemory;

        public string Tag { get; }

        public string MessageGroup { get; }

        public DateTime? DeliveryTimestamp { get; }

        public List<string> Keys { get; }

        public Dictionary<string, string> Properties { get; }

        public string BornHost { get; }

        public DateTime BornTime { get; }

        public int DeliveryAttempt { get; set; }

        public int IncrementAndGetDeliveryAttempt()
        {
            return ++DeliveryAttempt;
        }

        public bool IsCorrupted()
        {
            return _corrupted;
        }

        public static MessageView FromProtobuf(Proto.Message message, MessageQueue messageQueue = null)
        {
            var topic = message.Topic.Name;
            var systemProperties = message.SystemProperties;
            var messageId = systemProperties.MessageId;
            var bodyDigest = systemProperties.BodyDigest;
            var checkSum = bodyDigest.Checksum;
            var corrupted = false;
            var type = bodyDigest.Type;
            switch (type)
            {
                case Proto.DigestType.Crc32:
                    {
                        var expectedCheckSum = Crc32.HashToUInt32(message.Body.Span).ToString("X");
                        if (!expectedCheckSum.Equals(checkSum))
                        {
                            corrupted = true;
                        }

                        break;
                    }
                case Proto.DigestType.Md5:
                    {
                        var expectedCheckSum = Utilities.ComputeMd5Hash(message.Body.Span);
                        if (!expectedCheckSum.Equals(checkSum))
                        {
                            corrupted = true;
                        }

                        break;
                    }
                case Proto.DigestType.Sha1:
                    {
                        var expectedCheckSum = Utilities.ComputeSha1Hash(message.Body.Span);
                        if (!expectedCheckSum.Equals(checkSum))
                        {
                            corrupted = true;
                        }

                        break;
                    }
                case Proto.DigestType.Unspecified:
                default:
                    {
                        Logger.LogError(
                            $"Unsupported message body digest algorithm," +
                            $"digestType={type}, topic={topic}, messageId={messageId}");
                        break;
                    }
            }

            var bodyEncoding = systemProperties.BodyEncoding;
            byte[] bodyArray = null;
            switch (bodyEncoding)
            {
                case Proto.Encoding.Gzip:
                    {
                        var body = message.Body;
                        // Gzip
                        if (body[0] == 0x1f && body[1] == 0x8b)
                        {
                            bodyArray = Utilities.DecompressBytesGzip(message.Body.Memory);
                        }
                        // Zlib
                        else if (body[0] == 0x78 || body[0] == 0x79)
                        {
                            bodyArray = Utilities.DecompressBytesZlib(message.Body.Memory);
                        }
                        break;
                    }
                case Proto.Encoding.Identity:
                    {
                        break;
                    }
                case Proto.Encoding.Unspecified:
                default:
                    {
                        Logger.LogError($"Unsupported message encoding algorithm," +
                                        $" topic={topic}, messageId={messageId}, bodyEncoding={bodyEncoding}");
                        break;
                    }
            }

            var tag = systemProperties.HasTag ? systemProperties.Tag : null;
            var messageGroup = systemProperties.HasMessageGroup ? systemProperties.MessageGroup : null;
            DateTime? deliveryTime = null == systemProperties.DeliveryTimestamp
                ? null
                : (DateTime?)TimeZoneInfo.ConvertTimeFromUtc(systemProperties.DeliveryTimestamp.ToDateTime(), TimeZoneInfo.Local);
            var keys = systemProperties.Keys.ToList();

            var bornHost = systemProperties.BornHost;
            var bornTime =
                TimeZoneInfo.ConvertTimeFromUtc(systemProperties.BornTimestamp.ToDateTime(), TimeZoneInfo.Local);
            var deliveryAttempt = systemProperties.DeliveryAttempt;
            var queueOffset = systemProperties.QueueOffset;
            var properties = new Dictionary<string, string>();
            foreach (var (key, value) in message.UserProperties)
            {
                properties.Add(key, value);
            }


            var receiptHandle = systemProperties.ReceiptHandle;
            return new MessageView(messageId, topic, message.Body.Memory, bodyArray, tag, messageGroup, deliveryTime, keys, properties, bornHost,
                bornTime, deliveryAttempt, messageQueue, receiptHandle, queueOffset, corrupted);
        }

        public override string ToString()
        {
            return $"{nameof(MessageId)}: {MessageId}, {nameof(Topic)}: {Topic}, {nameof(Tag)}: {Tag}," +
                   $" {nameof(MessageGroup)}: {MessageGroup}, {nameof(DeliveryTimestamp)}: {DeliveryTimestamp}," +
                   $" {nameof(Keys)}: {string.Join(", ", Keys)}, {nameof(Properties)}: {string.Join(", ", Properties.Select(kvp => kvp.ToString()))}, {nameof(BornHost)}: {BornHost}, " +
                   $"{nameof(BornTime)}: {BornTime}, {nameof(DeliveryAttempt)}: {DeliveryAttempt}";
        }
    }
}