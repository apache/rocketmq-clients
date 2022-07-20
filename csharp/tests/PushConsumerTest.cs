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
using System.Collections.Generic;
using System;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{

    public class TestMessageListener : IMessageListener
    {
        public Task Consume(List<Message> messages, List<Message> failed)
        {
            foreach (var message in messages)
            {
                Console.WriteLine("");
            }

            return Task.CompletedTask;
        }
    }

    public class CountableMessageListener : IMessageListener
    {
        public Task Consume(List<Message> messages, List<Message> failed)
        {
            foreach (var message in messages)
            {
                Console.WriteLine("{}", message.MessageId);
            }

            return Task.CompletedTask;
        }
    }

    [TestClass]
    public class PushConsumerTest
    {

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            credentialsProvider = new ConfigFileCredentialsProvider();

        }

        [ClassCleanup]
        public static void TearDown()
        {

        }

        [TestInitialize]
        public void SetUp()
        {
            accessPoint = new AccessPoint();
            accessPoint.Host = host;
            accessPoint.Port = port;
        }

        [TestMethod]
        public void testLifecycle()
        {
            var consumer = new PushConsumer(accessPoint, resourceNamespace, group);
            consumer.CredentialsProvider = new ConfigFileCredentialsProvider();
            consumer.Region = "cn-hangzhou-pre";
            consumer.Subscribe(topic, "*", ExpressionType.TAG);
            consumer.RegisterListener(new TestMessageListener());
            consumer.Start();

            consumer.Shutdown();
        }


        // [Ignore]
        [TestMethod]
        public void testConsumeMessage()
        {
            var consumer = new PushConsumer(accessPoint, resourceNamespace, group);
            consumer.CredentialsProvider = new ConfigFileCredentialsProvider();
            consumer.Region = "cn-hangzhou-pre";
            consumer.Subscribe(topic, "*", ExpressionType.TAG);
            consumer.RegisterListener(new CountableMessageListener());
            consumer.Start();
            System.Threading.Thread.Sleep(System.TimeSpan.FromSeconds(300));
            consumer.Shutdown();
        }


        private static string resourceNamespace = "MQ_INST_1080056302921134_BXuIbML7";

        private static string topic = "cpp_sdk_standard";

        private static string group = "GID_cpp_sdk_standard";

        private static ICredentialsProvider credentialsProvider;
        private static string host = "116.62.231.199";
        private static int port = 80;

        private AccessPoint accessPoint;

    }

}