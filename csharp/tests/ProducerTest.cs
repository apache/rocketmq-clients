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

namespace org.apache.rocketmq
{

    [TestClass]
    public class ProducerTest
    {

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            List<string> nameServerAddress = new List<string>();
            nameServerAddress.Add(string.Format("{0}:{1}", host, port));
            resolver = new StaticNameServerResolver(nameServerAddress);

            credentialsProvider = new ConfigFileCredentialsProvider();
        }

        [ClassCleanup]
        public static void TearDown()
        {

        }


        [TestMethod]
        public void testSendMessage()
        {
            var producer = new Producer(resolver, resourceNamespace);
            producer.ResourceNamespace = resourceNamespace;
            producer.CredentialsProvider = new ConfigFileCredentialsProvider();
            producer.Region = "cn-hangzhou-pre";
            producer.start();
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            var msg = new Message(topic, body);
            var sendResult = producer.send(msg).GetAwaiter().GetResult();
            Assert.IsNotNull(sendResult);
            producer.shutdown();
        }

        private static string resourceNamespace = "MQ_INST_1080056302921134_BXuIbML7";

        private static string topic = "cpp_sdk_standard";

        private static string clientId = "C001";
        private static string group = "GID_cpp_sdk_standard";

        private static INameServerResolver resolver;
        private static ICredentialsProvider credentialsProvider;
        private static string host = "116.62.231.199";
        private static int port = 80;
    }

}