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
using Grpc.Core.Interceptors;
using System.Net.Http;
using Grpc.Net.Client;
using rmq = global::apache.rocketmq.v1;
using grpc = global::Grpc.Core;
using System;

namespace org.apache.rocketmq
{
    [TestClass]
    public class RpcClientTest
    {


        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            string target = string.Format("https://{0}:{1}", host, port);
            var channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions
            {
                HttpHandler = ClientManager.createHttpHandler()
            });
            var invoker = channel.Intercept(new ClientLoggerInterceptor());
            var client = new rmq::MessagingService.MessagingServiceClient(invoker);
            rpcClient = new RpcClient(client);

            clientConfig = new ClientConfig();
            var credentialsProvider = new ConfigFileCredentialsProvider();
            clientConfig.CredentialsProvider = credentialsProvider;
            clientConfig.ResourceNamespace = resourceNamespace;
            clientConfig.Region = "cn-hangzhou-pre";
        }

        [ClassCleanup]
        public static void TearDown()
        {

        }

        [TestMethod]
        public void testQueryRoute()
        {
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = resourceNamespace;
            request.Topic.Name = topic;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Host = host;
            address.Port = port;
            request.Endpoints.Addresses.Add(address);

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var deadline = DateTime.UtcNow.Add(TimeSpan.FromSeconds(3));
            var callOptions = new grpc::CallOptions(metadata, deadline);
            var response = rpcClient.queryRoute(request, callOptions).GetAwaiter().GetResult();
        }


        [TestMethod]
        public void testHeartbeat()
        {

            var request = new rmq::HeartbeatRequest();
            request.ClientId = clientId;
            request.ProducerData = new rmq::ProducerData();
            request.ProducerData.Group = new rmq::Resource();
            request.ProducerData.Group.ResourceNamespace = resourceNamespace;
            request.ProducerData.Group.Name = topic;
            request.FifoFlag = false;

            var metadata = new grpc::Metadata();
            Signature.sign(clientConfig, metadata);

            var deadline = DateTime.UtcNow.Add(TimeSpan.FromSeconds(3));
            var callOptions = new grpc::CallOptions(metadata, deadline);
            var response = rpcClient.heartbeat(request, callOptions).GetAwaiter().GetResult();
        }

        private static string resourceNamespace = "MQ_INST_1080056302921134_BXuIbML7";

        private static string topic = "cpp_sdk_standard";

        private static string clientId = "C001";
        private static string group = "GID_cpp_sdk_standard";

        private static string host = "116.62.231.199";
        private static int port = 80;

        private static IRpcClient rpcClient;
        private static ClientConfig clientConfig;
    }
}