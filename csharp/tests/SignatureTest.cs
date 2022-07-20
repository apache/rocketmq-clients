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
using grpc = global::Grpc.Core;
using Moq;
using System;

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class SignatureTest
    {

        [TestMethod]
        public void testSign()
        {
            var mock = new Mock<IClientConfig>();
            mock.Setup(x => x.getGroupName()).Returns("G1");
            mock.Setup(x => x.tenantId()).Returns("Tenant-id");
            mock.Setup(x => x.resourceNamespace()).Returns("mq:arn:test:");
            mock.Setup(x => x.serviceName()).Returns("mq");
            mock.Setup(x => x.region()).Returns("cn-hangzhou");

            string accessKey = "key";
            string accessSecret = "secret";
            var credentialsProvider = new StaticCredentialsProvider(accessKey, accessSecret);
            mock.Setup(x => x.credentialsProvider()).Returns(credentialsProvider);

            var metadata = new grpc::Metadata();
            Signature.sign(mock.Object, metadata);
            Assert.IsNotNull(metadata.Get(MetadataConstants.AUTHORIZATION));
        }
    }

}