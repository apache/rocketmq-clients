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
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class ProducerBuilderTest
    {
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetClientConfigurationWithNull()
        {
            var builder = new Producer.Builder();
            builder.SetClientConfig(null);
        }

        [TestMethod]
        [ExpectedException(typeof(NullReferenceException))]
        public void TestSetTopicWithNull()
        {
            var builder = new Producer.Builder();
            builder.SetTopics(null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetIllegalTopic()
        {
            var builder = new Producer.Builder();
            builder.SetTopics("\t");
        }

        [TestMethod]
        public void TestSetTopic()
        {
            var builder = new Producer.Builder();
            builder.SetTopics("abc");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetNegativeMaxAttempts()
        {
            var builder = new Producer.Builder();
            builder.SetMaxAttempts(-1);
        }

        [TestMethod]
        public void TestSetMaxAttempts()
        {
            var builder = new Producer.Builder();
            builder.SetMaxAttempts(3);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestSetTransactionCheckerWithNull()
        {
            var builder = new Producer.Builder();
            builder.SetTransactionChecker(null);
        }

        [TestMethod]
        public void TestSetTransactionChecker()
        {
            var builder = new Producer.Builder();
            builder.SetTransactionChecker(new TestTransactionChecker());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task TestBuildWithoutClientConfiguration()
        {
            var builder = new Producer.Builder();
            await builder.Build();
        }

        [TestMethod]
        public void TestBuild()
        {
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints("127.0.0.1:9876").Build();
            var builder = new Producer.Builder();
            builder.SetClientConfig(clientConfig).Build();
        }

        private class TestTransactionChecker : ITransactionChecker
        {
            public TransactionResolution Check(MessageView messageView)
            {
                return TransactionResolution.Commit;
            }
        }
    }
}