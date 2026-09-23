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

using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    /// <summary>
    /// Transport level RESOURCE_EXHAUSTED must reach the callers as the throttling exception of the SDK, so that the
    /// producer takes its backoff path instead of retrying immediately.
    /// </summary>
    [TestClass]
    public class ClientManagerRpcExceptionTest
    {
        private const string RequestId = "fake-request-id";

        private static Metadata CreateMetadata()
        {
            return new Metadata { { MetadataConstants.RequestIdKey, RequestId } };
        }

        [TestMethod]
        public async Task TestResourceExhaustedIsNormalized()
        {
            var rpcException = new RpcException(new Status(StatusCode.ResourceExhausted, "flow controlled"));
            var task = Task.FromException<Proto.HeartbeatResponse>(rpcException);

            try
            {
                await ClientManager.NormalizeTransportException(CreateMetadata(), task);
                Assert.Fail("Expected the transport failure to be normalized");
            }
            catch (TooManyRequestsException e)
            {
                StringAssert.Contains(e.Message, "response-code=42900");
                StringAssert.Contains(e.Message, $"request-id={RequestId}");
                StringAssert.Contains(e.Message, "flow controlled");
                Assert.AreSame(rpcException, e.InnerException);
            }
        }

        [TestMethod]
        public async Task TestResourceExhaustedWithoutDetailFallsBackToTheExceptionMessage()
        {
            var rpcException = new RpcException(new Status(StatusCode.ResourceExhausted, string.Empty));
            var task = Task.FromException<Proto.HeartbeatResponse>(rpcException);

            try
            {
                await ClientManager.NormalizeTransportException(CreateMetadata(), task);
                Assert.Fail("Expected the transport failure to be normalized");
            }
            catch (TooManyRequestsException e)
            {
                StringAssert.Contains(e.Message, rpcException.Message);
                Assert.AreSame(rpcException, e.InnerException);
            }
        }

        [TestMethod]
        public async Task TestOtherTransportExceptionIsUnchanged()
        {
            var rpcException = new RpcException(new Status(StatusCode.Unavailable, "connection refused"));
            var task = Task.FromException<Proto.HeartbeatResponse>(rpcException);

            try
            {
                await ClientManager.NormalizeTransportException(CreateMetadata(), task);
                Assert.Fail("Expected the transport failure to be passed through");
            }
            catch (RpcException e)
            {
                Assert.AreSame(rpcException, e);
            }
        }

        [TestMethod]
        public async Task TestSuccessfulResponseIsUnchanged()
        {
            var response = new Proto.HeartbeatResponse();
            var task = Task.FromResult(response);

            Assert.AreSame(response, await ClientManager.NormalizeTransportException(CreateMetadata(), task));
        }
    }
}
