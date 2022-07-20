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
using System.Collections.Generic;
using System.Threading.Tasks;
using Org.Apache.Rocketmq;


namespace tests
{
    [TestClass]
    public class ProducerTest
    {

        private static AccessPoint _accessPoint;

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            _accessPoint = new AccessPoint
            {
                Host = HOST,
                Port = PORT
            };
        }

        [ClassCleanup]
        public static void TearDown()
        {
        }

        [TestMethod]
        public async Task TestLifecycle()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            await producer.Shutdown();
        }

        [TestMethod]
        public async Task TestSendStandardMessage()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            
            // Tag the massage. A message has at most one tag.
            msg.Tag = "Tag-0";
            
            // Associate the message with one or multiple keys
            var keys = new List<string>();
            keys.Add("k1");
            keys.Add("k2");
            msg.Keys = keys;
            
            var sendResult = await producer.Send(msg);
            Assert.IsNotNull(sendResult);
            await producer.Shutdown();
        }
        
        [TestMethod]
        public async Task TestSendMultipleMessages()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            for (var i = 0; i < 128; i++)
            {
                var msg = new Message(topic, body);
            
                // Tag the massage. A message has at most one tag.
                msg.Tag = "Tag-0";
            
                // Associate the message with one or multiple keys
                var keys = new List<string>();
                keys.Add("k1");
                keys.Add("k2");
                msg.Keys = keys;
                var sendResult = await producer.Send(msg);
                Assert.IsNotNull(sendResult);                
            }
            await producer.Shutdown();
        }
        
        [TestMethod]
        public async Task TestSendFifoMessage()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            
            // Messages of the same group will get delivered one after another. 
            msg.MessageGroup = "message-group-0";
            
            // Verify messages are FIFO iff their message group is not null or empty.
            Assert.IsTrue(msg.Fifo());

            var sendResult = await producer.Send(msg);
            Assert.IsNotNull(sendResult);
            await producer.Shutdown();
        }
        
        [TestMethod]
        public async Task TestSendScheduledMessage()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            
            msg.DeliveryTimestamp = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            Assert.IsTrue(msg.Scheduled());
            
            var sendResult = await producer.Send(msg);
            Assert.IsNotNull(sendResult);
            await producer.Shutdown();
        }
        
        
        /**
         * Trying send a message that is both FIFO and Scheduled should fail.
         */
        [TestMethod]
        public async Task TestSendMessage_Failure()
        {
            var producer = new Producer(_accessPoint, resourceNamespace);
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            await producer.Start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            msg.MessageGroup = "Group-0";
            msg.DeliveryTimestamp = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            Assert.IsTrue(msg.Scheduled());

            try
            {
                await producer.Send(msg);
                Assert.Fail("Should have raised an exception");
            }
            catch (MessageException e)
            {
            }
            await producer.Shutdown();
        }

        private static string resourceNamespace = "";

        private static string topic = "cpp_sdk_standard";

        private static string HOST = "127.0.0.1";
        private static int PORT = 8081;
    }

}