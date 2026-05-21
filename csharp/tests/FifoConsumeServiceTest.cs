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
using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class FifoConsumeServiceTest
    {
        private static MessageView CreateMessageView(string messageGroup, string messageIdSuffix = "")
        {
            var sp = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Fifo,
                MessageId = MessageIdGenerator.GetInstance().Next() + messageIdSuffix,
                BornHost = "127.0.0.1:8080",
                BodyDigest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" },
                BornTimestamp = new Timestamp()
            };
            if (!string.IsNullOrEmpty(messageGroup))
            {
                sp.MessageGroup = messageGroup;
            }

            var msg = new Proto.Message
            {
                SystemProperties = sp,
                Topic = new Proto.Resource { Name = "t" },
                Body = ByteString.CopyFrom("foobar", Encoding.UTF8)
            };
            return MessageView.FromProtobuf(msg);
        }

        [TestMethod]
        public void TestGroupFifoBatch_AllWithoutGroup()
        {
            var list = new List<MessageView>
            {
                CreateMessageView(null, "a"),
                CreateMessageView(null, "b")
            };
            FifoConsumeService.GroupFifoBatchByMessageGroup(list, out var byGroup, out var withoutGroup);
            Assert.AreEqual(0, byGroup.Count);
            Assert.AreEqual(2, withoutGroup.Count);
        }

        [TestMethod]
        public void TestGroupFifoBatch_SingleGroupPreservesOrder()
        {
            var m0 = CreateMessageView("g1", "0");
            var m1 = CreateMessageView("g1", "1");
            var m2 = CreateMessageView("g1", "2");
            var list = new List<MessageView> { m0, m1, m2 };
            FifoConsumeService.GroupFifoBatchByMessageGroup(list, out var byGroup, out var withoutGroup);
            Assert.AreEqual(0, withoutGroup.Count);
            Assert.AreEqual(1, byGroup.Count);
            CollectionAssert.AreEqual(new[] { m0, m1, m2 }, byGroup["g1"]);
        }

        [TestMethod]
        public void TestGroupFifoBatch_MultipleGroupsAndWithout()
        {
            var a = CreateMessageView("g1");
            var b = CreateMessageView("g2");
            var c = CreateMessageView(null);
            var d = CreateMessageView("g1");
            var list = new List<MessageView> { a, b, c, d };
            FifoConsumeService.GroupFifoBatchByMessageGroup(list, out var byGroup, out var withoutGroup);
            Assert.AreEqual(1, withoutGroup.Count);
            Assert.AreSame(c, withoutGroup[0]);
            Assert.AreEqual(2, byGroup.Count);
            CollectionAssert.AreEqual(new[] { a, d }, byGroup["g1"]);
            CollectionAssert.AreEqual(new[] { b }, byGroup["g2"]);
        }
    }
}
