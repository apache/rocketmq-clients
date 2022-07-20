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
using Grpc.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class ClientManagerTest
    {

        [TestMethod]
        public void TestResolveRoute()
        {
            string topic = "cpp_sdk_standard";
            string resourceNamespace = "MQ_INST_1080056302921134_BXuIbML7";
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.ResourceNamespace = resourceNamespace;
            request.Topic.Name = topic;
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Host = "116.62.231.199";
            address.Port = 80;
            request.Endpoints.Addresses.Add(address);

            var metadata = new Metadata();
            var clientConfig = new ClientConfig();
            var credentialsProvider = new ConfigFileCredentialsProvider();
            clientConfig.CredentialsProvider = credentialsProvider;
            clientConfig.ResourceNamespace = resourceNamespace;
            clientConfig.Region = "cn-hangzhou-pre";
            Signature.sign(clientConfig, metadata);
            var clientManager = new ClientManager();
            string target = "https://116.62.231.199:80";
            var topicRouteData = clientManager.ResolveRoute(target, metadata, request, TimeSpan.FromSeconds(3)).GetAwaiter().GetResult();
            Console.WriteLine(topicRouteData);
        }
    }
}