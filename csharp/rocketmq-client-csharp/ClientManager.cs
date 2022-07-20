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

using rmq = Apache.Rocketmq.V2;
using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using grpc = Grpc.Core;
using System.Collections.Generic;
using System.Security.Cryptography;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class ClientManager : IClientManager
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public ClientManager()
        {
            _rpcClients = new Dictionary<string, RpcClient>();
            _clientLock = new ReaderWriterLockSlim();
        }

        public IRpcClient GetRpcClient(string target)
        {
            _clientLock.EnterReadLock();
            try
            {
                // client exists, return in advance.
                if (_rpcClients.ContainsKey(target))
                {
                    return _rpcClients[target];
                }
            }
            finally
            {
                _clientLock.ExitReadLock();
            }

            _clientLock.EnterWriteLock();
            try
            {
                // client exists, return in advance.
                if (_rpcClients.ContainsKey(target))
                {
                    return _rpcClients[target];
                }

                // client does not exist, generate a new one
                var client = new RpcClient(target);
                _rpcClients.Add(target, client);
                return client;
            }
            finally
            {
                _clientLock.ExitWriteLock();
            }
        }

        public grpc::AsyncDuplexStreamingCall<rmq::TelemetryCommand, rmq::TelemetryCommand> Telemetry(string target, grpc::Metadata metadata)
        {
            var rpcClient = GetRpcClient(target);
            return rpcClient.Telemetry(metadata);
        }

        public async Task<TopicRouteData> ResolveRoute(string target, grpc::Metadata metadata,
            rmq::QueryRouteRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            Logger.Debug($"QueryRouteRequest: {request}");
            var queryRouteResponse = await rpcClient.QueryRoute(metadata, request, timeout);

            if (queryRouteResponse.Status.Code != rmq::Code.Ok)
            {
                Logger.Warn($"Failed to query route entries for topic={request.Topic.Name} from {target}: {queryRouteResponse.Status}");
                // Raise an application layer exception
            }
            Logger.Debug($"QueryRouteResponse: {queryRouteResponse}");

            var messageQueues = new List<rmq::MessageQueue>();
            foreach (var messageQueue in queryRouteResponse.MessageQueues)
            {
                messageQueues.Add(messageQueue);
            }
            var topicRouteData = new TopicRouteData(messageQueues);
            return topicRouteData;
        }

        public async Task<Boolean> Heartbeat(string target, grpc::Metadata metadata, rmq::HeartbeatRequest request,
            TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            Logger.Debug($"Heartbeat to {target}, Request: {request}");
            var response = await rpcClient.Heartbeat(metadata, request, timeout);
            Logger.Debug($"Heartbeat to {target} response status: {response.Status}");
            return response.Status.Code == rmq::Code.Ok;
        }

        public async Task<rmq::SendMessageResponse> SendMessage(string target, grpc::Metadata metadata,
            rmq::SendMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.SendMessage(metadata, request, timeout);
            return response;
        }

        public async Task<Boolean> NotifyClientTermination(string target, grpc::Metadata metadata,
            rmq::NotifyClientTerminationRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            rmq::NotifyClientTerminationResponse response =
                await rpcClient.NotifyClientTermination(metadata, request, timeout);
            return response.Status.Code == rmq::Code.Ok;
        }

        public async Task<List<rmq::Assignment>> QueryLoadAssignment(string target, grpc::Metadata metadata, rmq::QueryAssignmentRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            rmq::QueryAssignmentResponse response = await rpcClient.QueryAssignment(metadata, request, timeout);
            if (response.Status.Code != rmq::Code.Ok)
            {
                // TODO: Build exception hierarchy
                throw new Exception($"Failed to query load assignment from server. Cause: {response.Status.Message}");
            }

            List<rmq::Assignment> assignments = new List<rmq.Assignment>();
            foreach (var item in response.Assignments)
            {
                assignments.Add(item);
            }
            return assignments;
        }

        public async Task<List<Message>> ReceiveMessage(string target, grpc::Metadata metadata, 
            rmq::ReceiveMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            List<rmq::ReceiveMessageResponse> response = await rpcClient.ReceiveMessage(metadata, request, timeout);

            if (null == response || 0 == response.Count)
            {
                // TODO: throw an exception to propagate this error?
                return new List<Message>();
            }

            List<Message> messages = new List<Message>();
            
            foreach (var entry in response)
            {
                switch (entry.ContentCase)
                {
                    case rmq.ReceiveMessageResponse.ContentOneofCase.None:
                    {
                        Logger.Warn("Unexpected ReceiveMessageResponse content type");
                        break;
                    }
                    
                    case rmq.ReceiveMessageResponse.ContentOneofCase.Status:
                    {
                        switch (entry.Status.Code)
                        {
                            case rmq.Code.Ok:
                            {
                                break;
                            }

                            case rmq.Code.Forbidden:
                            {
                                Logger.Warn("Receive message denied");
                                break;
                            }
                            case rmq.Code.TooManyRequests:
                            {
                                Logger.Warn("TooManyRequest: servers throttled");
                                break;
                            }
                            default:
                            {
                                Logger.Warn("Unknown error status");
                                break;
                            }
                        }
                        break;
                    }

                    case rmq.ReceiveMessageResponse.ContentOneofCase.Message:
                    {
                        var message = Convert(target, entry.Message);
                        messages.Add(message);
                        break;
                    }

                    case rmq.ReceiveMessageResponse.ContentOneofCase.DeliveryTimestamp:
                    {
                        var begin = entry.DeliveryTimestamp;
                        var costs = DateTime.UtcNow - begin.ToDateTime();
                        // TODO: Collect metrics
                        break;
                    }
                }
            }
            return messages;
        }

        private Message Convert(string sourceHost, rmq::Message message)
        {
            var msg = new Message();
            msg.Topic = message.Topic.Name;
            msg.MessageId = message.SystemProperties.MessageId;
            msg.Tag = message.SystemProperties.Tag;

            // Validate message body checksum
            byte[] raw = message.Body.ToByteArray();
            if (rmq::DigestType.Crc32 == message.SystemProperties.BodyDigest.Type)
            {
                uint checksum = Force.Crc32.Crc32Algorithm.Compute(raw, 0, raw.Length);
                if (!message.SystemProperties.BodyDigest.Checksum.Equals(checksum.ToString("X")))
                {
                    msg._checksumVerifiedOk = false;
                }
            }
            else if (rmq::DigestType.Md5 == message.SystemProperties.BodyDigest.Type)
            {
                var checksum = MD5.HashData(raw);
                if (!message.SystemProperties.BodyDigest.Checksum.Equals(System.Convert.ToHexString(checksum)))
                {
                    msg._checksumVerifiedOk = false;
                }
            }
            else if (rmq::DigestType.Sha1 == message.SystemProperties.BodyDigest.Type)
            {
                var checksum = SHA1.HashData(raw);
                if (!message.SystemProperties.BodyDigest.Checksum.Equals(System.Convert.ToHexString(checksum)))
                {
                    msg._checksumVerifiedOk = false;
                }
            }

            foreach (var entry in message.UserProperties)
            {
                msg.UserProperties.Add(entry.Key, entry.Value);
            }

            msg._receiptHandle = message.SystemProperties.ReceiptHandle;
            msg._sourceHost = sourceHost;

            foreach (var key in message.SystemProperties.Keys)
            {
                msg.Keys.Add(key);
            }

            msg.DeliveryAttempt = message.SystemProperties.DeliveryAttempt;

            if (message.SystemProperties.BodyEncoding == rmq::Encoding.Gzip)
            {
                // Decompress/Inflate message body
                var inputStream = new MemoryStream(message.Body.ToByteArray());
                var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
                var outputStream = new MemoryStream();
                gzipStream.CopyTo(outputStream);
                msg.Body = outputStream.ToArray();
            }
            else
            {
                msg.Body = message.Body.ToByteArray();
            }

            return msg;
        }

        public async Task<Boolean> Ack(string target, grpc::Metadata metadata, rmq::AckMessageRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.AckMessage(metadata, request, timeout);
            return response.Status.Code == rmq::Code.Ok;
        }

        public async Task<Boolean> ChangeInvisibleDuration(string target, grpc::Metadata metadata, rmq::ChangeInvisibleDurationRequest request, TimeSpan timeout)
        {
            var rpcClient = GetRpcClient(target);
            var response = await rpcClient.ChangeInvisibleDuration(metadata, request, timeout);
            return response.Status.Code == rmq::Code.Ok;
        }

        public async Task Shutdown()
        {
            _clientLock.EnterReadLock();
            try
            {
                List<Task> tasks = new List<Task>();
                foreach (var item in _rpcClients)
                {
                    tasks.Add(item.Value.Shutdown());
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                _clientLock.ExitReadLock();
            }
        }

        private readonly Dictionary<string, RpcClient> _rpcClients;
        private readonly ReaderWriterLockSlim _clientLock;
    }
}