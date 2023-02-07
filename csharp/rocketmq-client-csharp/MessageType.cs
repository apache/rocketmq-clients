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

using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public enum MessageType
    {
        Normal,
        Fifo,
        Delay,
        Transaction
    }

    public static class MessageTypeHelper
    {
        public static MessageType FromProtobuf(Proto.MessageType messageType)
        {
            switch (messageType)
            {
                case Proto.MessageType.Normal:
                    return MessageType.Normal;
                case Proto.MessageType.Fifo:
                    return MessageType.Fifo;
                case Proto.MessageType.Delay:
                    return MessageType.Delay;
                case Proto.MessageType.Transaction:
                    return MessageType.Transaction;
                case Proto.MessageType.Unspecified:
                default:
                    throw new InternalErrorException("MessageType is not specified");
            }
        }

        public static Proto.MessageType ToProtobuf(MessageType messageType)
        {
            return messageType switch
            {
                MessageType.Normal => Proto.MessageType.Normal,
                MessageType.Fifo => Proto.MessageType.Fifo,
                MessageType.Delay => Proto.MessageType.Delay,
                MessageType.Transaction => Proto.MessageType.Transaction,
                _ => Proto.MessageType.Unspecified
            };
        }
    }
}