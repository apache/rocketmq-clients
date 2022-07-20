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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using grpc = Grpc.Core;
using rmq = Apache.Rocketmq.V2;


namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class RpcClientTest
    {

        [TestMethod]
        public async Task testTelemetry()
        {
            Console.WriteLine("Test Telemetry streaming");
            string target = "https://11.166.42.94:8081";
            var rpc_client = new RpcClient(target);
            var client_config = new ClientConfig();
            var metadata = new grpc::Metadata();
            Signature.sign(client_config, metadata);

            var cmd = new rmq::TelemetryCommand();
            cmd.Settings = new rmq::Settings();
            cmd.Settings.ClientType = rmq::ClientType.Producer;
            cmd.Settings.AccessPoint = new rmq::Endpoints();
            cmd.Settings.AccessPoint.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Port = 8081;
            address.Host = "11.166.42.94";
            cmd.Settings.AccessPoint.Addresses.Add(address);
            cmd.Settings.RequestTimeout = new Google.Protobuf.WellKnownTypes.Duration();
            cmd.Settings.RequestTimeout.Seconds = 3;
            cmd.Settings.RequestTimeout.Nanos = 0;
            cmd.Settings.Publishing = new rmq::Publishing();
            var topic = new rmq::Resource();
            topic.Name = "cpp_sdk_standard";
            cmd.Settings.Publishing.Topics.Add(topic);
            cmd.Settings.UserAgent = new rmq::UA();
            cmd.Settings.UserAgent.Language = rmq::Language.DotNet;
            cmd.Settings.UserAgent.Version = "1.0";
            cmd.Settings.UserAgent.Hostname = System.Net.Dns.GetHostName();
            cmd.Settings.UserAgent.Platform = System.Environment.OSVersion.ToString();

            var duplexStreaming = rpc_client.Telemetry(metadata);
            var reader = duplexStreaming.ResponseStream;
            var writer = duplexStreaming.RequestStream;

            var cts = new CancellationTokenSource();
            await writer.WriteAsync(cmd);
            Console.WriteLine("Command written");
            if (await reader.MoveNext(cts.Token))
            {
                var response = reader.Current;
                switch (response.CommandCase)
                {
                    case rmq::TelemetryCommand.CommandOneofCase.Settings:
                        {
                            var responded_settings = response.Settings;
                            Console.WriteLine($"{responded_settings.ToString()}");
                            break;
                        }
                    case rmq::TelemetryCommand.CommandOneofCase.None:
                        {
                            Console.WriteLine($"Unknown response command type: {response.Status.ToString()}");
                            break;
                        }
                }
                Console.WriteLine("Server responded ");
            }
            else
            {
                Console.WriteLine("Server is not responding");
                var status = duplexStreaming.GetStatus();
                Console.WriteLine($"status={status.ToString()}");

                var trailers = duplexStreaming.GetTrailers();
                Console.WriteLine($"trailers={trailers.ToString()}");
            }
        }

        [TestMethod]
        public void testQueryRoute()
        {
            string target = "https://11.166.42.94:8081";
            var rpc_client = new RpcClient(target);
            var client_config = new ClientConfig();
            var metadata = new grpc::Metadata();
            Signature.sign(client_config, metadata);
            var request = new rmq::QueryRouteRequest();
            request.Topic = new rmq::Resource();
            request.Topic.Name = "cpp_sdk_standard";
            request.Endpoints = new rmq::Endpoints();
            request.Endpoints.Scheme = rmq::AddressScheme.Ipv4;
            var address = new rmq::Address();
            address.Port = 8081;
            address.Host = "11.166.42.94";
            request.Endpoints.Addresses.Add(address);
            var response = rpc_client.QueryRoute(metadata, request, client_config.RequestTimeout);
            var result = response.GetAwaiter().GetResult();
        }

        [TestMethod]
        public async Task TestSend()
        {
            string target = "https://11.166.42.94:8081";
            var rpc_client = new RpcClient(target);
            var client_config = new ClientConfig();
            var metadata = new grpc::Metadata();
            Signature.sign(client_config, metadata);

            var request = new rmq::SendMessageRequest();
            var message = new rmq::Message();
            message.Topic = new rmq::Resource();
            message.Topic.Name = "cpp_sdk_standard";
            message.Body = Google.Protobuf.ByteString.CopyFromUtf8("Test Body");
            message.SystemProperties = new rmq::SystemProperties();
            message.SystemProperties.Tag = "TagA";
            message.SystemProperties.MessageId = "abc";
            request.Messages.Add(message);
            var response = await rpc_client.SendMessage(metadata, request, TimeSpan.FromSeconds(3));
            Assert.AreEqual(rmq::Code.Ok, response.Status.Code);
        }
    }
}