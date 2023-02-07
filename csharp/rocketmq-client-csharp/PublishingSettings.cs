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
using System.Collections.Generic;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class PublishingSettings : Settings
    {
        private volatile int _maxBodySizeBytes = 4 * 1024 * 1024;
        private volatile bool _validateMessageType = true;

        public PublishingSettings(string clientId, Endpoints accessPoint, ExponentialBackoffRetryPolicy retryPolicy,
            TimeSpan requestTimeout, ConcurrentDictionary<string, bool> topics) : base(clientId, ClientType.Producer, accessPoint,
            retryPolicy, requestTimeout)
        {
            Topics = topics;
        }

        public ConcurrentDictionary<string, bool> Topics { get; }

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
            // TODO
        }

        public override Proto.Settings ToProtobuf()
        {
            List<Proto.Resource> topics = new List<Proto.Resource>();
            foreach (var topic in Topics)
            {
                topics.Add(new Proto.Resource
                {
                    Name = topic.Key
                });
            }

            var publishing = new Proto.Publishing();
            publishing.Topics.Add(topics);
            publishing.ValidateMessageType = _validateMessageType;
            return new Proto.Settings
            {
                Publishing = publishing,
                AccessPoint = AccessPoint.ToProtobuf(),
                ClientType = ClientTypeHelper.ToProtobuf(ClientType),
                RequestTimeout = Duration.FromTimeSpan(RequestTimeout),
                BackoffPolicy = RetryPolicy.ToProtobuf(),
                UserAgent = UserAgent.Instance.ToProtobuf()
            };
        }
    }
}