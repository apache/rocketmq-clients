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

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Org.Apache.Rocketmq;
using Endpoints = Org.Apache.Rocketmq.Endpoints;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class SessionTests
    {
        private static Client CreateTestClient()
        {
            var clientConfig = new ClientConfig.Builder().SetEndpoints("127.0.0.1:9876").Build();
            return new Producer(clientConfig, new ConcurrentDictionary<string, bool>(), 1, null);
        }

        [TestMethod]
        public async Task TestSyncSettings()
        {
            var testClient = CreateTestClient();
            var endpoints = new Endpoints(testClient.GetClientConfig().Endpoints);

            var mockStreamWriter = new Mock<IClientStreamWriter<Proto.TelemetryCommand>>();
            var mockAsyncStreamReader = new Mock<IAsyncStreamReader<Proto.TelemetryCommand>>();
            var mockClientManager = new Mock<IClientManager>();
            var mockGrpcCall = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
                mockStreamWriter.Object, mockAsyncStreamReader.Object, null, null, null, null);

            mockClientManager.Setup(cm => cm.Telemetry(endpoints)).Returns(mockGrpcCall);
            var session = new Session(endpoints, mockGrpcCall, testClient);

            var settings = new Proto.Settings();
            mockStreamWriter.Setup(m => m.WriteAsync(It.Is<Proto.TelemetryCommand>(tc => tc.Settings == settings)))
                .Returns(Task.CompletedTask);
            testClient.SetClientManager(mockClientManager.Object);

            await session.SyncSettings(true);

            mockStreamWriter.Verify(m => m.WriteAsync(It.IsAny<Proto.TelemetryCommand>()), Times.Once);
        }
    }
}