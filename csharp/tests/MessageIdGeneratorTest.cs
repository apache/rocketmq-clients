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
using Microsoft.Extensions.Time.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class MessageIdGeneratorTest
    {
        [TestMethod]
        public void TestNext()
        {
            MessageIdGenerator instance = MessageIdGenerator.GetInstance();
            var firstMessageId = instance.Next();
            Assert.AreEqual(34, firstMessageId.Length);
            Assert.AreEqual(MessageIdGenerator.Version, firstMessageId.Substring(0, 2));

            var secondMessageId = instance.Next();
            Assert.AreEqual(34, secondMessageId.Length);
            Assert.AreEqual(MessageIdGenerator.Version, secondMessageId.Substring(0, 2));

            Assert.AreNotEqual(firstMessageId, secondMessageId);
            Assert.AreEqual(firstMessageId.Substring(0, 24), secondMessageId.Substring(0, 24));
        }

        [TestMethod]
        public void TestNextId()
        {
            var fakeTimeProvider = new FakeTimeProvider();
            fakeTimeProvider.SetUtcNow(new DateTime(2023, 1, 1, 0, 0, 1, DateTimeKind.Utc));

            var instance = new MessageIdGenerator(fakeTimeProvider, new FakeUtilities());
            var firstMessageId = instance.Next();
            Assert.AreEqual("01000102030405002A03C2670100000001", firstMessageId);

            fakeTimeProvider.SetUtcNow(new DateTime(2023, 1, 1, 0, 0, 2, DateTimeKind.Utc));
            var secondMessageId = instance.Next();
            Assert.AreEqual("01000102030405002A03C2670200000002", secondMessageId);
        }

        private class FakeUtilities : IUtilities
        {
            public byte[] GetMacAddress()
            {
                return new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05 };
            }

            public int GetProcessId()
            {
                return 42;
            }

            public string ByteArrayToHexString(ReadOnlySpan<byte> bytes)
            {
                return Utilities.ByteArrayToHexString(bytes);
            }
        }
    }
}