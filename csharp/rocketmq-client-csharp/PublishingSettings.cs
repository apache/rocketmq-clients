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
using System.Collections.Concurrent;
using System.Linq;
using Google.Protobuf.WellKnownTypes;
using NLog;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class PublishingSettings : Settings
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private volatile int _maxBodySizeBytes = 4 * 1024 * 1024;
        private volatile bool _validateMessageType = true;

        public PublishingSettings(string clientId, Endpoints endpoints, IRetryPolicy retryPolicy,
            TimeSpan requestTimeout, ConcurrentDictionary<string, bool> topics) : base(clientId, ClientType.Producer,
            endpoints, retryPolicy, requestTimeout)
        {
            Topics = topics;
        }

        private ConcurrentDictionary<string, bool> Topics { get; }

        public int GetMaxBodySizeBytes()
        {
            return _maxBodySizeBytes;
        }

        public bool IsValidateMessageType()
        {
            return _validateMessageType;
        }

        public override void Sync(Proto::Settings settings)
        {
            if (Proto.Settings.PubSubOneofCase.Publishing != settings.PubSubCase)
            {
                Logger.Error($"[Bug] Issued settings does not match with the client type, clientId={ClientId}, " +
                             $"pubSubCase={settings.PubSubCase}, clientType={ClientType}");
                return;
            }

            RetryPolicy = RetryPolicy.InheritBackoff(settings.BackoffPolicy);
            _validateMessageType = settings.Publishing.ValidateMessageType;
            _maxBodySizeBytes = settings.Publishing.MaxBodySize;
        }

        public override Proto.Settings ToProtobuf()
        {
            var topics = Topics.Select(topic => new Proto.Resource { Name = topic.Key }).ToList();

            var publishing = new Proto.Publishing();
            publishing.Topics.Add(topics);
            publishing.ValidateMessageType = _validateMessageType;
            return new Proto.Settings
            {
                Publishing = publishing,
                AccessPoint = Endpoints.ToProtobuf(),
                ClientType = ClientTypeHelper.ToProtobuf(ClientType),
                RequestTimeout = Duration.FromTimeSpan(RequestTimeout),
                BackoffPolicy = RetryPolicy.ToProtobuf(),
                UserAgent = UserAgent.Instance.ToProtobuf()
            };
        }
    }
}