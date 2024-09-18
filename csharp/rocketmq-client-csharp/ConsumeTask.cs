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
using Microsoft.Extensions.Logging;

namespace Org.Apache.Rocketmq
{
    public class ConsumeTask
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ConsumeTask>();

        private readonly string _clientId;
        private readonly IMessageListener _messageListener;
        private readonly MessageView _messageView;

        public ConsumeTask(string clientId, IMessageListener messageListener, MessageView messageView)
        {
            _clientId = clientId;
            _messageListener = messageListener;
            _messageView = messageView;
        }

        /// <summary>
        /// Invoke IMessageListener to consume the message.
        /// </summary>
        /// <returns>Message(s) which are consumed successfully.</returns>
        public ConsumeResult Call()
        {
            try
            {
                var consumeResult = _messageListener.Consume(_messageView);
                return consumeResult;
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Message listener raised an exception while consuming messages, clientId={_clientId}," +
                                   $" mq={_messageView.MessageQueue}, messageId={_messageView.MessageId}");
                return ConsumeResult.FAILURE;
            }
        }
    }
}