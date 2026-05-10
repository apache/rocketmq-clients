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
        private readonly SemaphoreSlim _concurrencySemaphore;
        private readonly CancellationToken _consumptionCtsToken;

        public ConsumeService(string clientId, IMessageListener messageListener, SemaphoreSlim concurrencySemaphore,
            CancellationToken consumptionCtsToken)
        {
            ClientId = clientId;
            _messageListener = messageListener;
            _concurrencySemaphore = concurrencySemaphore;
            _consumptionCtsToken = consumptionCtsToken;
        }

        public abstract void Consume(ProcessQueue pq, List<MessageView> messageViews);

        public Task<ConsumeResult> Consume(MessageView messageView)
        {
            return Consume(messageView, TimeSpan.Zero);
        }

        public async Task<ConsumeResult> Consume(MessageView messageView, TimeSpan delay)
        {
            var task = new ConsumeTask(ClientId, _messageListener, messageView);

            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, _consumptionCtsToken).ConfigureAwait(false);
            }

            await _concurrencySemaphore.WaitAsync(_consumptionCtsToken).ConfigureAwait(false);
            try
            {
                return await Task.Run(() => task.Call(), _consumptionCtsToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error while consuming message, clientId={ClientId}", ClientId);
                throw;
            }
            finally
            {
                _concurrencySemaphore.Release();
            }
        }
    }
}