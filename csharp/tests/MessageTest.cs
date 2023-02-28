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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;

namespace Org.Apache.Rocketmq
{
    [TestClass]
    public class MessageTest
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTopic0()
        {
            const string topic = null;
            new Message.Builder().SetTopic(topic);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTopic1()
        {
            const string topic = "";
            new Message.Builder().SetTopic(topic);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTag0()
        {
            new Message.Builder().SetTag(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTag1()
        {
            new Message.Builder().SetTag("");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTag2()
        {
            new Message.Builder().SetTag("\t");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTag3()
        {
            new Message.Builder().SetTag("\t\n");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalTag4()
        {
            new Message.Builder().SetTag("abc|cde");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalMessageGroup0()
        {
            new Message.Builder().SetMessageGroup(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalMessageGroup1()
        {
            new Message.Builder().SetMessageGroup("");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalMessageGroup2()
        {
            new Message.Builder().SetMessageGroup("\t");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalMessageGroup3()
        {
            new Message.Builder().SetMessageGroup("\t\n");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalProperty0()
        {
            new Message.Builder().AddProperty(null, "b");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalProperty1()
        {
            new Message.Builder().AddProperty("a", null);
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
        [ExpectedException(typeof(ArgumentException))]
        public void TestIllegalKey()
        {
            new Message.Builder().SetKeys("\t");
        }

        [TestMethod]
        public void TestKeys()
        {
            new Message.Builder().SetKeys("a", "b");
        }
    }
}