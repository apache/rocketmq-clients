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
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class MessageTest
    {
        [TestMethod]
        public void TestIllegalTopic0()
        {
            const string topic = null;
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTopic(topic));
        }

        [TestMethod]
        public void TestIllegalTopic1()
        {
            const string topic = "";
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTopic(topic));
        }

        [TestMethod]
        public void TestIllegalTag0()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTag(null));
        }

        [TestMethod]
        public void TestIllegalTag1()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTag(""));
        }

        [TestMethod]
        public void TestIllegalTag2()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTag("\t"));
        }

        [TestMethod]
        public void TestIllegalTag3()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTag("\t\n"));
        }

        [TestMethod]
        public void TestIllegalTag4()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetTag("abc|cde"));
        }

        [TestMethod]
        public void TestIllegalMessageGroup0()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetMessageGroup(null));
        }

        [TestMethod]
        public void TestIllegalMessageGroup1()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetMessageGroup(""));
        }

        [TestMethod]
        public void TestIllegalMessageGroup2()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetMessageGroup("\t"));
        }

        [TestMethod]
        public void TestIllegalMessageGroup3()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetMessageGroup("\t\n"));
        }

        [TestMethod]
        public void TestIllegalProperty0()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().AddProperty(null, "b"));
        }

        [TestMethod]
        public void TestIllegalProperty1()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().AddProperty("a", null));
        }

        [TestMethod]
        public void TestAddProperty()
        {
            var message = new Message.Builder()
                .SetTopic("topic")
                .AddProperty("a", "b")
                .SetBody(Encoding.UTF8.GetBytes("foobar"))
                .Build();
            var properties = new Dictionary<string, string>
            {
                ["a"] = "b"
            };
            Assert.AreEqual(1, message.Properties.Count);
            Assert.AreEqual(properties["a"], message.Properties["a"]);
        }

        [TestMethod]
        public void TestIllegalKey()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetKeys("\t"));
        }

        [TestMethod]
        public void TestKeys()
        {
            new Message.Builder().SetKeys("a", "b");
        }

        [TestMethod]
        public void TestSetDeliveryTimestampWithLocalTime()
        {
            var deliveryTimestamp = DateTime.Now;
            var message = new Message.Builder().SetTopic("yourTopic").SetDeliveryTimestamp(deliveryTimestamp)
                .SetBody(Encoding.UTF8.GetBytes("foobar"))
                .Build();
            Assert.IsTrue(message.DeliveryTimestamp.HasValue);
            Assert.AreEqual(DateTimeKind.Local, message.DeliveryTimestamp.Value.Kind);
            Assert.AreEqual(deliveryTimestamp, message.DeliveryTimestamp.Value);
        }

        [TestMethod]
        public void TestSetDeliveryTimestampWithUtcTime()
        {
            var deliveryTimestamp = DateTime.UtcNow;
            var message = new Message.Builder().SetTopic("yourTopic").SetDeliveryTimestamp(deliveryTimestamp)
                .SetBody(Encoding.UTF8.GetBytes("foobar"))
                .Build();
            Assert.IsTrue(message.DeliveryTimestamp.HasValue);
            Assert.AreEqual(DateTimeKind.Local, message.DeliveryTimestamp.Value.Kind);
            var localTimestamp = TimeZoneInfo.ConvertTimeFromUtc(deliveryTimestamp, TimeZoneInfo.Local);
            Assert.AreEqual(localTimestamp, message.DeliveryTimestamp.Value);
        }

        [TestMethod]
        public void TestSetDeliveryTimestampAndMessageGroup()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetDeliveryTimestamp(DateTime.UtcNow + TimeSpan.FromSeconds(30))
                .SetMessageGroup("messageGroup").Build());
        }

        [TestMethod]
        public void TestSetMessageGroupAndDeliveryTimestamp()
        {
            Assert.ThrowsExactly<ArgumentException>(() => new Message.Builder().SetMessageGroup("messageGroup")
                .SetDeliveryTimestamp(DateTime.UtcNow + TimeSpan.FromSeconds(30))
                .Build());
        }
    }
}