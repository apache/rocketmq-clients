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

using rmq = Apache.Rocketmq.V2;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using NLog;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Provides a read-only view for message.
    /// </summary>
    public class MessageView
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private readonly rmq.MessageQueue _messageQueue;
        private readonly string _receiptHandle;
        private readonly long _offset;
        private readonly bool _corrupted;

        internal MessageView(string messageId, string topic, byte[] body, string tag, string messageGroup,
            DateTime deliveryTime, List<string> keys, Dictionary<string, string> properties, string bornHost,
            DateTime bornTime, int deliveryAttempt, rmq.MessageQueue messageQueue, string receiptHandle, long offset,
            bool corrupted)
        {
            MessageId = messageId;
            Topic = topic;
            Body = body;
            Tag = tag;
            MessageGroup = messageGroup;
            DeliveryTime = deliveryTime;
            Keys = keys;
            Properties = properties;
            BornHost = bornHost;
            BornTime = bornTime;
            DeliveryAttempt = deliveryAttempt;
            _messageQueue = messageQueue;
            _receiptHandle = receiptHandle;
            _offset = offset;
            _corrupted = corrupted;
        }

        public string MessageId { get; }

        public string Topic { get; }

        public byte[] Body { get; }

        public string Tag { get; }

        public string MessageGroup { get; }

        public DateTime DeliveryTime { get; }

        public List<string> Keys { get; }

        public Dictionary<string, string> Properties { get; }

        public string BornHost { get; }

        public DateTime BornTime { get; }

        public int DeliveryAttempt { get; }

        public static MessageView FromProtobuf(rmq.Message message, rmq.MessageQueue messageQueue)
        {
            var topic = message.Topic.Name;
            var systemProperties = message.SystemProperties;
            var messageId = systemProperties.MessageId;
            var bodyDigest = systemProperties.BodyDigest;
            var checkSum = bodyDigest.Checksum;
            var raw = message.Body.ToByteArray();
            bool corrupted = false;
            var type = bodyDigest.Type;
            switch (type)
            {
                case rmq.DigestType.Crc32:
                {
                    var expectedCheckSum = Force.Crc32.Crc32Algorithm.Compute(raw, 0, raw.Length).ToString("X");
                    if (!expectedCheckSum.Equals(checkSum))
                    {
                        corrupted = true;
                    }

                    break;
                }
                case rmq.DigestType.Md5:
                {
                    var expectedCheckSum = Convert.ToHexString(MD5.HashData(raw));
                    if (!expectedCheckSum.Equals(checkSum))
                    {
                        corrupted = true;
                    }

                    break;
                }
                case rmq.DigestType.Sha1:
                {
                    var expectedCheckSum = Convert.ToHexString(SHA1.HashData(raw));
                    if (!expectedCheckSum.Equals(checkSum))
                    {
                        corrupted = true;
                    }

                    break;
                }
                default:
                {
                    Logger.Error(
                        $"Unsupported message body digest algorithm," +
                        $"digestType={type}, topic={topic}, messageId={messageId}");
                    break;
                }
            }

            var bodyEncoding = systemProperties.BodyEncoding;
            byte[] body = raw;
            switch (bodyEncoding)
            {
                case rmq.Encoding.Gzip:
                {
                    body = Utilities.uncompressBytesGzip(message.Body.ToByteArray());
                    break;
                }
                case rmq.Encoding.Identity:
                {
                    break;
                }
                default:
                {
                    Logger.Error($"Unsupported message encoding algorithm," +
                                 $" topic={topic}, messageId={messageId}, bodyEncoding={bodyEncoding}");
                    break;
                }
            }

            string tag = systemProperties.HasTag ? systemProperties.Tag : null;
            string messageGroup = systemProperties.HasMessageGroup ? systemProperties.MessageGroup : null;
            var deliveryTime = systemProperties.DeliveryTimestamp.ToDateTime();
            List<string> keys = new List<string>();
            foreach (var key in systemProperties.Keys)
            {
                keys.Add(key);
            }

            var bornHost = systemProperties.BornHost;
            var bornTime = systemProperties.BornTimestamp.ToDateTime();
            var deliveryAttempt = systemProperties.DeliveryAttempt;
            var queueOffset = systemProperties.QueueOffset;
            Dictionary<string, string> properties = new Dictionary<string, string>();
            foreach (var (key, value) in message.UserProperties)
            {
                properties.Add(key, value);
            }

            var receiptHandle = systemProperties.ReceiptHandle;
            return new MessageView(messageId, topic, body, tag, messageGroup, deliveryTime, keys, properties, bornHost,
                bornTime, deliveryAttempt, messageQueue, receiptHandle, queueOffset, corrupted);
        }

        public override string ToString()
        {
            return
                $"{nameof(MessageId)}: {MessageId}, {nameof(Topic)}: {Topic}, {nameof(Tag)}: {Tag}," +
                $" {nameof(MessageGroup)}: {MessageGroup}, {nameof(DeliveryTime)}: {DeliveryTime}," +
                $" {nameof(Keys)}: {Keys}, {nameof(Properties)}: {Properties}, {nameof(BornHost)}: {BornHost}, " +
                $"{nameof(BornTime)}: {BornTime}, {nameof(DeliveryAttempt)}: {DeliveryAttempt}";
        }
    }
}