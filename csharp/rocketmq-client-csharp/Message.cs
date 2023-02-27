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
        public Message() : this(null, null)
        {
        }

        public Message(string topic, byte[] body) : this(topic, null, new List<string>(), body)
        {
        }

        public Message(string topic, string tag, byte[] body) : this(topic, tag, new List<string>(), body)
        {
        }

        public Message(string topic, string tag, List<string> keys, byte[] body)
        {
            Topic = topic;
            Tag = tag;
            Keys = keys;
            Body = body;
            UserProperties = new Dictionary<string, string>();
            DeliveryTimestamp = null;
        }

        internal Message(Message message)
        {
            Topic = message.Topic;
            Tag = message.Tag;
            Keys = message.Keys;
            Body = message.Body;
            MessageGroup = message.MessageGroup;
            UserProperties = message.UserProperties;
            DeliveryTimestamp = message.DeliveryTimestamp;
        }

        public string Topic { get; set; }

        public byte[] Body { get; set; }

        public string Tag { get; set; }

        public List<string> Keys { get; set; }
        public Dictionary<string, string> UserProperties { get; set; }


        public DateTime? DeliveryTimestamp { get; set; }

        public int DeliveryAttempt { get; internal set; }

        public string MessageGroup { get; set; }
    }
}