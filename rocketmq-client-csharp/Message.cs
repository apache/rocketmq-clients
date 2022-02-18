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
namespace org.apache.rocketmq
{

    public class Message {
        public Message() : this(null, null) {
        }

        public Message(string topic, byte[] body) : this(topic, null, new List<string>(), body) {}

        public Message(string topic, string tag, byte[] body) : this(topic, tag, new List<string>(), body) {
        }

        public Message(string topic, string tag, List<string> keys, byte[] body) {
            this.maxAttemptTimes = 3;
            this.topic = topic;
            this.tag = tag;
            this.keys = keys;
            this.body = body;
            this.userProperties = new Dictionary<string, string>();
            this.systemProperties = new Dictionary<string, string>();
        }

        private string topic;

        public string Topic {
            get { return topic; }
            set { this.topic = value; }
        }

        private byte[] body;
        public byte[] Body {
            get { return body; }
            set { this.body = value; }
        }

        private string tag;
        public string Tag {
            get { return tag; }
            set { this.tag = value; }
        }

        private List<string> keys;
        public List<string> Keys{
            get { return keys; }
            set { this.keys = value; }
        }

        private Dictionary<string, string> userProperties;
        public Dictionary<string, string> UserProperties {
            get { return userProperties; }
            set { this.userProperties = value; }
        }

        private Dictionary<string, string> systemProperties;
        internal Dictionary<string, string> SystemProperties {
            get { return systemProperties; }
            set { this.systemProperties = value; }
        }

        private int maxAttemptTimes;
        public int MaxAttemptTimes
        {
            get { return maxAttemptTimes; }
            set { maxAttemptTimes = value; }
        }
    }

}