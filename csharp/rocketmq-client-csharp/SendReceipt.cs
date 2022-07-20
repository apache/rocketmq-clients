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

namespace Org.Apache.Rocketmq
{
    public sealed class SendReceipt
    {
        public SendReceipt(string messageId)
        {
            status_ = SendStatus.SEND_OK;
            messageId_ = messageId;
        }

        public SendReceipt(string messageId, SendStatus status)
        {
            status_ = status;
            messageId_ = messageId;
        }

        private string messageId_;

        public string MessageId
        {
            get { return messageId_; }
        }


        private SendStatus status_;

        public SendStatus Status
        {
            get { return status_; }
        }
    }
}