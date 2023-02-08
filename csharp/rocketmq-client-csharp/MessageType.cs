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
using rmq = Apache.Rocketmq.V2;

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
        public static MessageType FromProtobuf(rmq.MessageType messageType)
        {
            switch (messageType)
            {
                case rmq.MessageType.Normal:
                    return MessageType.Normal;
                case rmq.MessageType.Fifo:
                    return MessageType.Fifo;
                case rmq.MessageType.Delay:
                    return MessageType.Delay;
                case rmq.MessageType.Transaction:
                    return MessageType.Transaction;
                default:
                    throw new InternalErrorException("MessageType is not specified");
            }
        }

        public static rmq.MessageType ToProtobuf(MessageType messageType)
        {
            switch (messageType)
            {
                case MessageType.Normal:
                    return rmq.MessageType.Normal;
                case MessageType.Fifo:
                    return rmq.MessageType.Fifo;
                case MessageType.Delay:
                    return rmq.MessageType.Delay;
                case MessageType.Transaction:
                    return rmq.MessageType.Transaction;
                default:
                    return rmq.MessageType.Unspecified;
            }
        }
    }
}