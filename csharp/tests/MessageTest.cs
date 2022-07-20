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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Text;
using System.Collections.Generic;

namespace Org.Apache.Rocketmq
{
    [TestClass]
    public class MessageTest
    {

        [TestMethod]
        public void testCtor()
        {
            var msg1 = new Message();
            Assert.IsNotNull(msg1.MessageId);
            Assert.IsTrue(msg1.MessageId.StartsWith("01"));
            Assert.IsNull(msg1.Topic);
            Assert.IsNull(msg1.Body);
            Assert.IsNull(msg1.Tag);
            Assert.AreEqual(msg1.Keys.Count, 0);
            Assert.AreEqual(msg1.UserProperties.Count, 0);
        }

        [TestMethod]
        public void testCtor2()
        {
            string topic = "T1";
            string bodyString = "body";
            byte[] body = Encoding.ASCII.GetBytes(bodyString);
            var msg1 = new Message(topic, body);
            Assert.AreEqual(msg1.Topic, topic);
            Assert.AreEqual(msg1.Body, body);
            Assert.IsNull(msg1.Tag);
            Assert.AreEqual(msg1.Keys.Count, 0);
            Assert.AreEqual(msg1.UserProperties.Count, 0);
        }

        [TestMethod]
        public void testCtor3()
        {
            string topic = "T1";
            string bodyString = "body";
            byte[] body = Encoding.ASCII.GetBytes(bodyString);
            string tag = "TagA";
            var msg1 = new Message(topic, tag, body);
            Assert.AreEqual(msg1.Topic, topic);
            Assert.AreEqual(msg1.Body, body);
            Assert.AreEqual(msg1.Tag, tag);
            Assert.AreEqual(msg1.Keys.Count, 0);
            Assert.AreEqual(msg1.UserProperties.Count, 0);
        }

        [TestMethod]
        public void testCtor4()
        {
            string topic = "T1";
            string bodyString = "body";
            byte[] body = Encoding.ASCII.GetBytes(bodyString);
            string tag = "TagA";
            List<string> keys = new List<string>();
            keys.Add("Key1");
            keys.Add("Key2");

            var msg1 = new Message(topic, tag, keys, body);
            Assert.AreEqual(msg1.Topic, topic);
            Assert.AreEqual(msg1.Body, body);
            Assert.AreEqual(msg1.Tag, tag);
            Assert.AreEqual(msg1.Keys, keys);
            Assert.AreEqual(msg1.UserProperties.Count, 0);
        }

        [TestMethod]
        public void testCtor5()
        {
            string topic = "T1";
            string bodyString = "body";
            byte[] body = Encoding.ASCII.GetBytes(bodyString);
            string tag = "TagA";
            List<string> keys = new List<string>();
            keys.Add("Key1");
            keys.Add("Key2");

            var msg1 = new Message(topic, tag, keys, body);

            msg1.UserProperties.Add("k", "v");
            msg1.UserProperties.Add("k2", "v2");

            Assert.AreEqual(msg1.Topic, topic);
            Assert.AreEqual(msg1.Body, body);
            Assert.AreEqual(msg1.Tag, tag);
            Assert.AreEqual(msg1.Keys, keys);
            Assert.AreEqual(msg1.UserProperties.Count, 2);

            string value;
            msg1.UserProperties.TryGetValue("k", out value);
            Assert.AreEqual(value, "v");

            msg1.UserProperties.TryGetValue("k2", out value);
            Assert.AreEqual(value, "v2");

        }

    }
}