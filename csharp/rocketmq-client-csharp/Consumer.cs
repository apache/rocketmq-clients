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
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public abstract class Consumer : Client
    {
        internal static readonly Regex ConsumerGroupRegex = new Regex("^[%a-zA-Z0-9_-]+$");
        protected readonly string ConsumerGroup;

        protected Consumer(ClientConfig clientConfig, string consumerGroup) : base(
            clientConfig)
        {
            ConsumerGroup = consumerGroup;
        }

        protected async Task<ReceiveMessageResult> ReceiveMessage(Proto.ReceiveMessageRequest request, MessageQueue mq,
            TimeSpan awaitDuration)
        {
            var tolerance = ClientConfig.RequestTimeout;
            var timeout = tolerance.Add(awaitDuration);
            var invocation = await ClientManager.ReceiveMessage(mq.Broker.Endpoints, request, timeout);
            var status = new Proto.Status
            {
                Code = Proto.Code.InternalServerError,
                Message = "Status was not set by server"
            };
            var messageList = new List<Proto.Message>();
            foreach (var entry in invocation.Response)
            {
                switch (entry.ContentCase)
                {
                    case Proto.ReceiveMessageResponse.ContentOneofCase.Status:
                        status = entry.Status;
                        break;
                    case Proto.ReceiveMessageResponse.ContentOneofCase.Message:
                        messageList.Add(entry.Message);
                        break;
                    case Proto.ReceiveMessageResponse.ContentOneofCase.DeliveryTimestamp:
                    case Proto.ReceiveMessageResponse.ContentOneofCase.None:
                    default:
                        break;
                }
            }

            var messages = messageList.Select(message => MessageView.FromProtobuf(message, mq)).ToList();
            StatusChecker.Check(status, request, invocation.RequestId);
            return new ReceiveMessageResult(mq.Broker.Endpoints, messages);
        }

        private static Proto.FilterExpression WrapFilterExpression(FilterExpression filterExpression)
        {
            var filterType = Proto.FilterType.Tag;
            if (ExpressionType.Sql92.Equals(filterExpression.Type))
            {
                filterType = Proto.FilterType.Sql;
            }

            return new Proto.FilterExpression
            {
                Type = filterType,
                Expression = filterExpression.Expression
            };
        }

        protected Proto.ReceiveMessageRequest WrapReceiveMessageRequest(int batchSize, MessageQueue mq,
            FilterExpression filterExpression, TimeSpan awaitDuration, TimeSpan invisibleDuration)
        {
            var group = new Proto.Resource
            {
                Name = ConsumerGroup
            };
            return new Proto.ReceiveMessageRequest
            {
                Group = group,
                MessageQueue = mq.ToProtobuf(),
                FilterExpression = WrapFilterExpression(filterExpression),
                LongPollingTimeout = Duration.FromTimeSpan(awaitDuration),
                BatchSize = batchSize,
                AutoRenew = false,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration)
            };
        }
    }
}