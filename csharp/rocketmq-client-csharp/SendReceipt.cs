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
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public sealed class SendReceipt
    {
        public SendReceipt(string messageId)
        {
            MessageId = messageId;
        }

        public string MessageId { get; }

        public override string ToString()
        {
            return $"{nameof(MessageId)}: {MessageId}";
        }

        public static List<SendReceipt> processSendMessageResponse(rmq.SendMessageResponse response)
        {
            rmq.Status status = response.Status;
            foreach (var entry in response.Entries)
            {
                if (rmq.Code.Ok.Equals(entry.Status.Code))
                {
                    status = entry.Status;
                }
            }

            // May throw exception.
            StatusChecker.Check(status, response);
            List<SendReceipt> sendReceipts = new List<SendReceipt>();
            foreach (var entry in response.Entries)
            {
                sendReceipts.Add(new SendReceipt(entry.MessageId));
            }

            return sendReceipts;
        }
    }
}