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
    public abstract class ConsumeService
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ConsumeService>();

        protected readonly string ClientId;
        private readonly IMessageListener _messageListener;
        private readonly TaskScheduler _consumptionTaskScheduler;
        private readonly CancellationToken _consumptionCtsToken;

        public ConsumeService(string clientId, IMessageListener messageListener, TaskScheduler consumptionTaskScheduler,
            CancellationToken consumptionCtsToken)
        {
            ClientId = clientId;
            _messageListener = messageListener;
            _consumptionTaskScheduler = consumptionTaskScheduler;
            _consumptionCtsToken = consumptionCtsToken;
        }

        public abstract void Consume(ProcessQueue pq, List<MessageView> messageViews);

        public Task<ConsumeResult> Consume(MessageView messageView)
        {
            return Consume(messageView, TimeSpan.Zero);
        }

        public Task<ConsumeResult> Consume(MessageView messageView, TimeSpan delay)
        {
            var task = new ConsumeTask(ClientId, _messageListener, messageView);
            var delayMilliseconds = (int)delay.TotalMilliseconds;

            if (delayMilliseconds <= 0)
            {
                return Task.Factory.StartNew(() => task.Call(), _consumptionCtsToken, TaskCreationOptions.None,
                    _consumptionTaskScheduler);
            }

            var tcs = new TaskCompletionSource<ConsumeResult>();

            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(delay, _consumptionCtsToken);
                    var result = await Task.Factory.StartNew(() => task.Call(), _consumptionCtsToken,
                        TaskCreationOptions.None, _consumptionTaskScheduler);
                    tcs.SetResult(result);
                }
                catch (Exception e)
                {
                    Logger.LogError(e, $"Error while consuming message, clientId={ClientId}");
                    tcs.SetException(e);
                }
            }, _consumptionCtsToken);

            return tcs.Task;
        }
    }
}