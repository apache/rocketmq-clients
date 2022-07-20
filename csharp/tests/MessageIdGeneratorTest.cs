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
            Assert.AreEqual(MessageIdGenerator.version, firstMessageId.Substring(0, 2));

            var secondMessageId = instance.Next();
            Assert.AreEqual(34, secondMessageId.Length);
            Assert.AreEqual(MessageIdGenerator.version, secondMessageId.Substring(0, 2));

            Assert.AreNotEqual(firstMessageId, secondMessageId);
            Assert.AreEqual(firstMessageId.Substring(0, 24), secondMessageId.Substring(0, 24));
        }
    }
}