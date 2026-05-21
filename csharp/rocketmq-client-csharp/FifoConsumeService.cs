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
    public class FifoConsumeService : ConsumeService
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<FifoConsumeService>();

        private readonly bool _enableFifoConsumeAccelerator;

        public FifoConsumeService(string clientId, IMessageListener messageListener,
            TaskScheduler consumptionExecutor, CancellationToken consumptionCtsToken,
            bool enableFifoConsumeAccelerator = false) :
            base(clientId, messageListener, consumptionExecutor, consumptionCtsToken)
        {
            _enableFifoConsumeAccelerator = enableFifoConsumeAccelerator;
        }

        /// <summary>
        /// Groups FIFO batch by <see cref="MessageView.MessageGroup"/> (aligned with golang fifoConsumeService / groupMessageBy).
        /// Messages with null or empty group go to <paramref name="withoutGroup"/> and share one serial chain.
        /// </summary>
        internal static void GroupFifoBatchByMessageGroup(IReadOnlyList<MessageView> messageViews,
            out Dictionary<string, List<MessageView>> byGroup, out List<MessageView> withoutGroup)
        {
            byGroup = new Dictionary<string, List<MessageView>>();
            withoutGroup = new List<MessageView>();
            foreach (var mv in messageViews)
            {
                var g = mv.MessageGroup;
                if (string.IsNullOrEmpty(g))
                {
                    withoutGroup.Add(mv);
                }
                else if (!byGroup.TryGetValue(g, out var list))
                {
                    list = new List<MessageView>();
                    byGroup[g] = list;
                    list.Add(mv);
                }
                else
                {
                    list.Add(mv);
                }
            }
        }

        public override void Consume(ProcessQueue pq, List<MessageView> messageViews)
        {
            if (!_enableFifoConsumeAccelerator || messageViews.Count <= 1)
            {
                ConsumeIteratively(pq, messageViews.GetEnumerator());
                return;
            }

            GroupFifoBatchByMessageGroup(messageViews, out var byGroup, out var withoutGroup);

            var groupNum = byGroup.Count;
            if (withoutGroup.Count > 0)
            {
                groupNum++;
            }

            Logger.LogDebug(
                "FifoConsumeService parallel consume, messageViewsNum={Count}, groupNum={GroupNum}, clientId={ClientId}",
                messageViews.Count, groupNum, ClientId);

            foreach (var group in byGroup.Values)
            {
                ConsumeIteratively(pq, group.GetEnumerator());
            }

            if (withoutGroup.Count > 0)
            {
                ConsumeIteratively(pq, withoutGroup.GetEnumerator());
            }
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
