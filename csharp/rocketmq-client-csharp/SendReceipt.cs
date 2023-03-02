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

using System.Collections.Generic;
using System.Linq;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public sealed class SendReceipt : ISendReceipt
    {
        private SendReceipt(string messageId, string transactionId, MessageQueue messageQueue)
        {
            MessageId = messageId;
            TransactionId = transactionId;
            MessageQueue = messageQueue;
        }

        public string MessageId { get; }

        public string TransactionId { get; }

        private MessageQueue MessageQueue { get; }

        public Endpoints Endpoints => MessageQueue.Broker.Endpoints;

        public override string ToString()
        {
            return $"{nameof(MessageId)}: {MessageId}";
        }

        public static IEnumerable<SendReceipt> ProcessSendMessageResponse(MessageQueue mq,
            RpcInvocation<Proto.SendMessageRequest, Proto.SendMessageResponse>
                invocation)
        {
            var status = invocation.Response.Status;
            foreach (var entry in invocation.Response.Entries)
            {
                if (Proto.Code.Ok.Equals(entry.Status.Code))
                {
                    status = entry.Status;
                }
            }

            // May throw exception.
            StatusChecker.Check(status, invocation.Request, invocation.RequestId);
            return invocation.Response.Entries.Select(entry => new SendReceipt(entry.MessageId, entry.TransactionId, mq)).ToList();
        }
    }
}