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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Org.Apache.Rocketmq
{
    public class FifoConsumeService : ConsumeService
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<FifoConsumeService>();

        public FifoConsumeService(string clientId, IMessageListener messageListener,
            TaskScheduler consumptionExecutor, CancellationToken consumptionCtsToken) :
            base(clientId, messageListener, consumptionExecutor, consumptionCtsToken)
        {
        }

        public override void Consume(ProcessQueue pq, List<MessageView> messageViews)
        {
            ConsumeIteratively(pq, messageViews.GetEnumerator());
        }

        public void ConsumeIteratively(ProcessQueue pq, IEnumerator<MessageView> iterator)
        {
            if (!iterator.MoveNext())
            {
                return;
            }

            var messageView = iterator.Current;

            if (messageView != null && messageView.IsCorrupted())
            {
                // Discard corrupted message.
                Logger.LogError($"Message is corrupted for FIFO consumption, prepare to discard it," +
                                $" mq={pq.GetMessageQueue()}, messageId={messageView.MessageId}, clientId={ClientId}");
                pq.DiscardFifoMessage(messageView);
                ConsumeIteratively(pq, iterator); // Recursively consume the next message
                return;
            }

            var consumeTask = Consume(messageView);
            consumeTask.ContinueWith(async t =>
            {
                var result = await t;
                await pq.EraseFifoMessage(messageView, result);
            }, TaskContinuationOptions.ExecuteSynchronously).ContinueWith(_ => ConsumeIteratively(pq, iterator),
                TaskContinuationOptions.ExecuteSynchronously);
        }
    }
}