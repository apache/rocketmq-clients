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
using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class MessageViewTests
    {
        private const string FakeHost = "127.0.0.1";
        private const string FakeTopic = "test-topic";

        [TestMethod]
        public void TestFromProtobufWithCrc32()
        {
            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "9EF61F95" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.AreEqual(FakeHost, messageView.BornHost);
            Assert.IsFalse(messageView.IsCorrupted());
        }

        [TestMethod]
        public void TestFromProtobufWithWrongCrc32()
        {
            var digest = new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "00000000" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.IsTrue(messageView.IsCorrupted());
        }

        [TestMethod]
        public void TestFromProtobufWithMd5()
        {
            var digest = new Proto.Digest
            { Type = Proto.DigestType.Md5, Checksum = "3858F62230AC3C915F300C664312C63F" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.IsFalse(messageView.IsCorrupted());
        }

        [TestMethod]
        public void TestFromProtobufWithWrongMd5()
        {
            var digest = new Proto.Digest
            { Type = Proto.DigestType.Md5, Checksum = "00000000000000000000000000000000" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.IsTrue(messageView.IsCorrupted());
        }

        [TestMethod]
        public void TestFromProtobufWithSha1()
        {
            var digest = new Proto.Digest
            { Type = Proto.DigestType.Sha1, Checksum = "8843D7F92416211DE9EBB963FF4CE28125932878" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.IsFalse(messageView.IsCorrupted());
        }

        [TestMethod]
        public void TestFromProtobufWithWrongSha1()
        {
            var digest = new Proto.Digest
            { Type = Proto.DigestType.Sha1, Checksum = "0000000000000000000000000000000000000000" };
            var systemProperties = new Proto.SystemProperties
            {
                MessageType = Proto.MessageType.Normal,
                MessageId = MessageIdGenerator.GetInstance().Next(),
                BornHost = FakeHost,
                BodyDigest = digest,
                BornTimestamp = new Timestamp()
            };
            var body = ByteString.CopyFrom("foobar", Encoding.UTF8);
            var message = new Proto.Message
            {
                SystemProperties = systemProperties,
                Topic = new Proto.Resource { Name = FakeTopic },
                Body = body
            };

            var messageView = MessageView.FromProtobuf(message);

            CollectionAssert.AreEqual(body.ToByteArray(), messageView.Body);
            Assert.AreEqual(FakeTopic, messageView.Topic);
            Assert.IsTrue(messageView.IsCorrupted());
        }
    }
}
