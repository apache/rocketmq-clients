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
    public class Message
    {
        private Message(string topic, byte[] body, string tag, List<string> keys,
            Dictionary<string, string> properties, DateTime? deliveryTimestamp, string messageGroup)
        {
            Topic = topic;
            Tag = tag;
            Keys = keys;
            Body = body;
            Properties = properties;
            DeliveryTimestamp = deliveryTimestamp;
            MessageGroup = messageGroup;
        }

        internal Message(Message message)
        {
            Topic = message.Topic;
            Tag = message.Tag;
            Keys = message.Keys;
            Body = message.Body;
            Properties = message.Properties;
            MessageGroup = message.MessageGroup;
            DeliveryTimestamp = message.DeliveryTimestamp;
        }

        public string Topic { get; }

        public byte[] Body { get; }

        public string Tag { get; }

        public List<string> Keys { get; }
        public Dictionary<string, string> Properties { get; }

        public DateTime? DeliveryTimestamp { get; }

        public string MessageGroup { get; }

        public class Builder
        {
            private string _topic;
            private byte[] _body;
            private string _tag;
            private List<string> _keys = new();
            private readonly Dictionary<string, string> _properties = new();
            private DateTime? _deliveryTimestamp;
            private string _messageGroup;

            public Builder SetTopic(string topic)
            {
                Preconditions.CheckArgument(null != topic, "topic should not be null");
                _topic = topic;
                return this;
            }

            public Builder SetBody(byte[] body)
            {
                Preconditions.CheckArgument(null != body, "body should not be null");
                _body = body;
                return this;
            }

            public Builder SetTag(string tag)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(tag), "tag should not be null or white space");
                _tag = tag;
                return this;
            }

            public Builder SetKeys(params string[] keys)
            {
                _keys = new List<string>();
                foreach (var key in keys)
                {
                    Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(key),
                        "key should not be null or white space");
                    _keys.Add(key);
                }

                return this;
            }

            public Builder AddProperty(string key, string value)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(key),
                    "key should not be null or white space");
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(value),
                    "value should not be null or white space");
                _properties[key!] = value;
                return this;
            }

            public Builder SetDeliveryTimestamp(DateTime deliveryTimestamp)
            {
                _deliveryTimestamp = deliveryTimestamp;
                return this;
            }

            public Builder SetMessageGroup(string messageGroup)
            {
                Preconditions.CheckArgument(!string.IsNullOrWhiteSpace(messageGroup),
                    "messageGroup should not be null or white space");
                _messageGroup = messageGroup;
                return this;
            }

            public Message Build()
            {
                Preconditions.CheckArgument(null != _topic, "topic has not been set yet");
                Preconditions.CheckArgument(null != _body, "body has not been set yet");
                return new Message(_topic, _body, _tag, _keys, _properties, _deliveryTimestamp, _messageGroup);
            }
        }
    }
}