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

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class SendResultTest
    {

        [TestMethod]
        public void testCtor()
        {
            string messageId = new string("abc");
            var sendResult = new SendReceipt(messageId);
            Assert.AreEqual(messageId, sendResult.MessageId);
            Assert.AreEqual(SendStatus.SEND_OK, sendResult.Status);
        }


        [TestMethod]
        public void testCtor2()
        {
            string messageId = new string("abc");
            var sendResult = new SendReceipt(messageId, SendStatus.FLUSH_DISK_TIMEOUT);
            Assert.AreEqual(messageId, sendResult.MessageId);
            Assert.AreEqual(SendStatus.FLUSH_DISK_TIMEOUT, sendResult.Status);
        }

    }

}