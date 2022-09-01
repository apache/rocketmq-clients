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
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using rmq = Apache.Rocketmq.V2;
using System.Threading.Tasks;
using Org.Apache.Rocketmq;

namespace tests
{

    [TestClass]
    public class SimpleConsumerTest
    {

        private static string _group = "GID_cpp_sdk_standard";
        private static string _topic = "cpp_sdk_standard";
        private const string HOST = "127.0.0.1";
        private const int PORT = 8081;
        

        [TestMethod]
        public async Task TestLifecycle()
        {
            var simpleConsumer = new SimpleConsumer($"{HOST}:{PORT}", _group);
            simpleConsumer.Subscribe(_topic, new FilterExpression("*", ExpressionType.TAG));
            await simpleConsumer.Start();
            Thread.Sleep(1_000);
            await simpleConsumer.Shutdown();
        }

        [TestMethod]
        public async Task TestReceive()
        {
            var simpleConsumer = new SimpleConsumer($"{HOST}:{PORT}", _group);
            simpleConsumer.Subscribe(_topic, new FilterExpression("*", ExpressionType.TAG));
            await simpleConsumer.Start();
            var batchSize = 32;
            var invisibleDuration = TimeSpan.FromSeconds(10);
            var messages  = await simpleConsumer.Receive(batchSize, invisibleDuration);
            Assert.IsTrue(messages.Count > 0);
            Assert.IsTrue(messages.Count <= batchSize);
            await simpleConsumer.Shutdown();
        }
        
        
        [TestMethod]
        public async Task TestAck()
        {
            var simpleConsumer = new SimpleConsumer($"{HOST}:{PORT}", _group);
            simpleConsumer.Subscribe(_topic, new FilterExpression("*", ExpressionType.TAG));
            await simpleConsumer.Start();
            var batchSize = 32;
            var invisibleDuration = TimeSpan.FromSeconds(10);
            var messages  = await simpleConsumer.Receive(batchSize, invisibleDuration);
            foreach (var message in messages)
            {
                await simpleConsumer.Ack(message);
                Console.WriteLine($"Ack {message.MessageId} OK");
            }
            await simpleConsumer.Shutdown();
        }
        
        [TestMethod]
        public async Task TestChangeInvisibleDuration()
        {
            var simpleConsumer = new SimpleConsumer($"{HOST}:{PORT}", _group);
            simpleConsumer.Subscribe(_topic, new FilterExpression("*", ExpressionType.TAG));
            await simpleConsumer.Start();
            var batchSize = 32;
            var invisibleDuration = TimeSpan.FromSeconds(10);
            var messages  = await simpleConsumer.Receive(batchSize, invisibleDuration);
            foreach (var message in messages)
            {
                await simpleConsumer.ChangeInvisibleDuration(message, TimeSpan.FromSeconds(10));
                Console.WriteLine($"ChangeInvisibleDuration for message[MsgId={message.MessageId}] OK");
            }
            await simpleConsumer.Shutdown();
        }
    }
}