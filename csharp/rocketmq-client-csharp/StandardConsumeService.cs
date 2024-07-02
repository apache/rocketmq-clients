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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Org.Apache.Rocketmq
{
    public class StandardConsumeService : ConsumeService
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<StandardConsumeService>();
        
        public StandardConsumeService(string clientId, IMessageListener messageListener,
            TaskScheduler consumptionExecutor, CancellationToken consumptionCtsToken) :
            base(clientId, messageListener, consumptionExecutor, consumptionCtsToken)
        {
        }

        public override async Task Consume(ProcessQueue pq, List<MessageView> messageViews)
        {
            foreach (var messageView in messageViews)
            {
                // Discard corrupted message.
                if (messageView.IsCorrupted())
                {
                    Logger.LogError("Message is corrupted for standard consumption, prepare to discard it," +
                                    $" mq={pq.GetMessageQueue()}, messageId={messageView.MessageId}, clientId={ClientId}");
                    await pq.DiscardMessage(messageView);
                    continue;
                }
            
                try
                {
                    var consumeResult = await Consume(messageView);
                    await pq.EraseMessage(messageView, consumeResult);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"[Bug] Exception raised in consumption callback, clientId={ClientId}");
                }
            }
        }
    }
}