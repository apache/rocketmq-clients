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
using Apache.Rocketmq.V2;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Process queue is a cache to store fetched messages from remote for <c>PushConsumer</c>.
    /// 
    /// <c>PushConsumer</c> queries assignments periodically and converts them into message queues, each message queue is
    /// mapped into one process queue to fetch message from remote. If the message queue is removed from the newest 
    /// assignment, the corresponding process queue is marked as expired soon, which means its lifecycle is over.
    /// </summary>
    public class ProcessQueue
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ProcessQueue>();

        internal static readonly TimeSpan AckMessageFailureBackoffDelay = TimeSpan.FromSeconds(1);
        internal static readonly TimeSpan ChangeInvisibleDurationFailureBackoffDelay = TimeSpan.FromSeconds(1);
        internal static readonly TimeSpan ForwardMessageToDeadLetterQueueFailureBackoffDelay = TimeSpan.FromSeconds(1);

        private static readonly TimeSpan ReceivingFlowControlBackoffDelay = TimeSpan.FromMilliseconds(20);
        private static readonly TimeSpan ReceivingFailureBackoffDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan ReceivingBackoffDelayWhenCacheIsFull = TimeSpan.FromSeconds(1);

        private readonly PushConsumer _consumer;

        /// <summary>
        /// Dropped means ProcessQueue is deprecated, which means no message would be fetched from remote anymore.
        /// </summary>
        private volatile bool _dropped;
        private readonly MessageQueue _mq;
        private readonly FilterExpression _filterExpression;

        /// <summary>
        /// Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
        /// </summary>
        private readonly List<MessageView> _cachedMessages;
        private readonly ReaderWriterLockSlim _cachedMessageLock;
        private long _cachedMessagesBytes;

        private long _activityTime = DateTime.UtcNow.Ticks;
        private long _cacheFullTime = long.MinValue;

        private readonly CancellationTokenSource _receiveMsgCts;
        private readonly CancellationTokenSource _ackMsgCts;
        private readonly CancellationTokenSource _changeInvisibleDurationCts;
        private readonly CancellationTokenSource _forwardMessageToDeadLetterQueueCts;

        public ProcessQueue(PushConsumer consumer, MessageQueue mq, FilterExpression filterExpression,
            CancellationTokenSource receiveMsgCts, CancellationTokenSource ackMsgCts,
            CancellationTokenSource changeInvisibleDurationCts, CancellationTokenSource forwardMessageToDeadLetterQueueCts)
        {
            _consumer = consumer;
            _dropped = false;
            _mq = mq;
            _filterExpression = filterExpression;
            _cachedMessages = new List<MessageView>();
            _cachedMessageLock = new ReaderWriterLockSlim();
            _cachedMessagesBytes = 0;
            _receiveMsgCts = receiveMsgCts;
            _ackMsgCts = ackMsgCts;
            _changeInvisibleDurationCts = changeInvisibleDurationCts;
            _forwardMessageToDeadLetterQueueCts = forwardMessageToDeadLetterQueueCts;
        }

        /// <summary>
        /// Get the mapped message queue.
        /// </summary>
        /// <returns>mapped message queue.</returns>
        public MessageQueue GetMessageQueue()
        {
            return _mq;
        }

        /// <summary>
        /// Drop the current process queue, which means the process queue's lifecycle is over,
        /// thus it would not fetch messages from the remote anymore if dropped.
        /// </summary>
        public void Drop()
        {
            _dropped = true;
        }

        /// <summary>
        /// ProcessQueue would be regarded as expired if no fetch message for a long time.
        /// </summary>
        /// <returns>if it is expired.</returns>
        public bool Expired()
        {
            var longPollingTimeout = _consumer.GetPushConsumerSettings().GetLongPollingTimeout();
            var requestTimeout = _consumer.GetClientConfig().RequestTimeout;
            var maxIdleDuration = longPollingTimeout.Add(requestTimeout).Multiply(3);
            var idleDuration = DateTime.UtcNow.Ticks - Interlocked.Read(ref _activityTime);
            if (idleDuration < maxIdleDuration.Ticks)
            {
                return false;
            }
            var afterCacheFullDuration = DateTime.UtcNow.Ticks - Interlocked.Read(ref _cacheFullTime);
            if (afterCacheFullDuration < maxIdleDuration.Ticks)
            {
                return false;
            }
            Logger.LogWarning(
                $"Process queue is idle, idleDuration={idleDuration}, maxIdleDuration={maxIdleDuration}," +
                $" afterCacheFullDuration={afterCacheFullDuration}, mq={_mq}, clientId={_consumer.GetClientId()}");
            return true;
        }

        internal void CacheMessages(List<MessageView> messageList)
        {
            _cachedMessageLock.EnterWriteLock();
            try
            {
                foreach (var messageView in messageList)
                {
                    _cachedMessages.Add(messageView);
                    Interlocked.Add(ref _cachedMessagesBytes, messageView.Body.Length);
                }
            }
            finally
            {
                _cachedMessageLock.ExitWriteLock();
            }
        }

        private int GetReceptionBatchSize()
        {
            var bufferSize = _consumer.CacheMessageCountThresholdPerQueue() - CachedMessagesCount();
            bufferSize = Math.Max(bufferSize, 1);
            return Math.Min(bufferSize, _consumer.GetPushConsumerSettings().GetReceiveBatchSize());
        }

        /// <summary>
        /// Start to fetch messages from remote immediately.
        /// </summary>
        public void FetchMessageImmediately()
        {
            ReceiveMessageImmediately();
        }

        /// <summary>
        /// Receive message later by message queue.
        /// </summary>
        /// <remarks>
        /// Make sure that no exception will be thrown.
        /// </remarks>
        public void OnReceiveMessageException(Exception t, string attemptId)
        {
            var delay = t is TooManyRequestsException ? ReceivingFlowControlBackoffDelay : ReceivingFailureBackoffDelay;
            ReceiveMessageLater(delay, attemptId);
        }

        private void ReceiveMessageLater(TimeSpan delay, string attemptId)
        {
            var clientId = _consumer.GetClientId();
            Logger.LogInformation($"Try to receive message later, mq={_mq}, delay={delay}, clientId={clientId}");
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(delay, _receiveMsgCts.Token);
                    ReceiveMessage(attemptId);
                }
                catch (Exception ex)
                {
                    if (_receiveMsgCts.IsCancellationRequested)
                    {
                        return;
                    }
                    Logger.LogError(ex, $"[Bug] Failed to schedule message receiving request, mq={_mq}, clientId={clientId}");
                    OnReceiveMessageException(ex, attemptId);
                }
            });
        }

        private string GenerateAttemptId()
        {
            return Guid.NewGuid().ToString();
        }

        public void ReceiveMessage()
        {
            ReceiveMessage(GenerateAttemptId());
        }

        public void ReceiveMessage(string attemptId)
        {
            var clientId = _consumer.GetClientId();
            if (_dropped)
            {
                Logger.LogInformation($"Process queue has been dropped, no longer receive message, mq={_mq}, clientId={clientId}");
                return;
            }
            if (IsCacheFull())
            {
                Logger.LogWarning($"Process queue cache is full, would receive message later, mq={_mq}, clientId={clientId}");
                ReceiveMessageLater(ReceivingBackoffDelayWhenCacheIsFull, attemptId);
                return;
            }
            ReceiveMessageImmediately(attemptId);
        }

        private void ReceiveMessageImmediately()
        {
            ReceiveMessageImmediately(GenerateAttemptId());
        }

        private void ReceiveMessageImmediately(string attemptId)
        {
            var clientId = _consumer.GetClientId();
            if (_consumer.State != State.Running)
            {
                Logger.LogInformation($"Stop to receive message because consumer is not running, mq={_mq}, clientId={clientId}");
                return;
            }

            try
            {
                var endpoints = _mq.Broker.Endpoints;
                var batchSize = GetReceptionBatchSize();
                var longPollingTimeout = _consumer.GetPushConsumerSettings().GetLongPollingTimeout();
                var request = _consumer.WrapReceiveMessageRequest(batchSize, _mq, _filterExpression, longPollingTimeout,
                    attemptId);

                Interlocked.Exchange(ref _activityTime, DateTime.UtcNow.Ticks);

                var task = _consumer.ReceiveMessage(request, _mq, longPollingTimeout);
                task.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        string nextAttemptId = null;
                        if (t.Exception is { InnerException: RpcException { StatusCode: StatusCode.DeadlineExceeded } })
                        {
                            nextAttemptId = request.AttemptId;
                        }

                        Logger.LogError(t.Exception, $"Exception raised during message reception, mq={_mq}," +
                                                     $" attemptId={request.AttemptId}, nextAttemptId={nextAttemptId}," +
                                                     $" clientId={clientId}");
                        OnReceiveMessageException(t.Exception, nextAttemptId);
                    }
                    else
                    {
                        try
                        {
                            var result = t.Result;
                            OnReceiveMessageResult(result);
                        }
                        catch (Exception ex)
                        {
                            // Should never reach here.
                            Logger.LogError($"[Bug] Exception raised while handling receive result, mq={_mq}," +
                                            $" endpoints={endpoints}, clientId={clientId}, exception={ex}");
                            OnReceiveMessageException(ex, attemptId);
                        }
                    }
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"Exception raised during message reception, mq={_mq}, clientId={clientId}");
                OnReceiveMessageException(ex, attemptId);
            }
        }

        private void OnReceiveMessageResult(ReceiveMessageResult result)
        {
            var messages = result.Messages;
            if (messages.Count > 0)
            {
                CacheMessages(messages);
                _consumer.GetConsumeService().Consume(this, messages);
            }
            ReceiveMessage();
        }

        private bool IsCacheFull()
        {
            var cacheMessageCountThresholdPerQueue = _consumer.CacheMessageCountThresholdPerQueue();
            var actualMessagesQuantity = CachedMessagesCount();
            var clientId = _consumer.GetClientId();
            if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity)
            {
                Logger.LogWarning($"Process queue total cached messages quantity exceeds the threshold," +
                                  $" threshold={cacheMessageCountThresholdPerQueue}, actual={actualMessagesQuantity}," +
                                  $" mq={_mq}, clientId={clientId}");
                Interlocked.Exchange(ref _cacheFullTime, DateTime.UtcNow.Ticks);
                return true;
            }

            var cacheMessageBytesThresholdPerQueue = _consumer.CacheMessageBytesThresholdPerQueue();
            var actualCachedMessagesBytes = CachedMessageBytes();
            if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes)
            {
                Logger.LogWarning($"Process queue total cached messages memory exceeds the threshold," +
                                  $" threshold={cacheMessageBytesThresholdPerQueue} bytes," +
                                  $" actual={actualCachedMessagesBytes} bytes, mq={_mq}, clientId={clientId}");
                Interlocked.Exchange(ref _cacheFullTime, DateTime.UtcNow.Ticks);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Erase messages(Non-FIFO-consume-mode) which have been consumed properly.
        /// </summary>
        /// <param name="messageView">the message to erase.</param>
        /// <param name="consumeResult">consume result.</param>
        public void EraseMessage(MessageView messageView, ConsumeResult consumeResult)
        {
            var task = ConsumeResult.SUCCESS.Equals(consumeResult) ? AckMessage(messageView) : NackMessage(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private Task AckMessage(MessageView messageView)
        {
            var tcs = new TaskCompletionSource<bool>();
            AckMessage(messageView, 1, tcs);
            return tcs.Task;
        }

        private void AckMessage(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapAckMessageRequest(messageView);
            var task = _consumer.GetClientManager().AckMessage(messageView.MessageQueue.Broker.Endpoints, request,
                _consumer.GetClientConfig().RequestTimeout);

            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    Logger.LogError(responseTask.Exception, $"Exception raised while acknowledging message," +
                                                            $" would retry later, clientId={clientId}," +
                                                            $" consumerGroup={consumerGroup}," +
                                                            $" messageId={messageId}," +
                                                            $" mq={_mq}, endpoints={endpoints}");
                    AckMessageLater(messageView, attempt + 1, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;

                    if (statusCode == Code.InvalidReceiptHandle)
                    {
                        Logger.LogError($"Failed to ack message due to the invalid receipt handle, forgive to retry," +
                                        $" clientId={clientId}, consumerGroup={consumerGroup}, messageId={messageId}," +
                                        $" attempt={attempt}, mq={_mq}, endpoints={endpoints}, requestId={requestId}," +
                                        $" status message={status.Message}");
                        tcs.SetException(new BadRequestException((int)statusCode, requestId, status.Message));
                    }

                    if (statusCode != Code.Ok)
                    {
                        Logger.LogError(
                            $"Failed to change invisible duration, would retry later, clientId={clientId}," +
                            $" consumerGroup={consumerGroup}, messageId={messageId}, attempt={attempt}, mq={_mq}," +
                            $" endpoints={endpoints}, requestId={requestId}, status message={status.Message}");
                        AckMessageLater(messageView, attempt + 1, tcs);
                        return;
                    }

                    tcs.SetResult(true);

                    if (attempt > 1)
                    {
                        Logger.LogInformation($"Successfully acked message finally, clientId={clientId}," +
                                              $" consumerGroup={consumerGroup}, messageId={messageId}," +
                                              $" attempt={attempt}, mq={_mq}, endpoints={endpoints}," +
                                              $" requestId={requestId}");
                    }
                    else
                    {
                        Logger.LogDebug($"Successfully acked message, clientId={clientId}," +
                                        $" consumerGroup={consumerGroup}, messageId={messageId}, mq={_mq}," +
                                        $" endpoints={endpoints}, requestId={requestId}");
                    }
                }
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void AckMessageLater(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(AckMessageFailureBackoffDelay, _ackMsgCts.Token);
                    AckMessage(messageView, attempt + 1, tcs);
                }
                catch (Exception ex)
                {
                    if (_ackMsgCts.IsCancellationRequested)
                    {
                        return;
                    }
                    Logger.LogError(ex, $"[Bug] Failed to schedule message ack request, mq={_mq}," +
                                        $" messageId={messageView.MessageId}, clientId={_consumer.GetClientId()}");
                    AckMessageLater(messageView, attempt + 1, tcs);
                }
            });
        }

        private Task NackMessage(MessageView messageView)
        {
            var deliveryAttempt = messageView.DeliveryAttempt;
            var duration = _consumer.GetRetryPolicy().GetNextAttemptDelay(deliveryAttempt);
            var tcs = new TaskCompletionSource<bool>();
            ChangeInvisibleDuration(messageView, duration, 1, tcs);
            return tcs.Task;
        }

        private void ChangeInvisibleDuration(MessageView messageView, TimeSpan duration, int attempt,
            TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapChangeInvisibleDuration(messageView, duration);
            var task = _consumer.GetClientManager().ChangeInvisibleDuration(endpoints,
                request, _consumer.GetClientConfig().RequestTimeout);
            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    Logger.LogError(responseTask.Exception, $"Exception raised while changing invisible" +
                                                            $" duration, would retry later, clientId={clientId}," +
                                                            $" consumerGroup={consumerGroup}," +
                                                            $" messageId={messageId}, mq={_mq}," +
                                                            $" endpoints={endpoints}");
                    ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;

                    if (statusCode == Code.InvalidReceiptHandle)
                    {
                        Logger.LogError($"Failed to change invisible duration due to the invalid receipt handle," +
                                        $" forgive to retry, clientId={clientId}, consumerGroup={consumerGroup}," +
                                        $" messageId={messageId}, attempt={attempt}, mq={_mq}, endpoints={endpoints}," +
                                        $" requestId={requestId}, status message={status.Message}");
                        tcs.SetException(new BadRequestException((int)statusCode, requestId, status.Message));
                    }

                    if (statusCode != Code.Ok)
                    {
                        Logger.LogError($"Failed to change invisible duration, would retry later," +
                                        $" clientId={clientId}, consumerGroup={consumerGroup}, messageId={messageId}," +
                                        $" attempt={attempt}, mq={_mq}, endpoints={endpoints}, requestId={requestId}," +
                                        $" status message={status.Message}");
                        ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                        return;
                    }

                    tcs.SetResult(true);

                    if (attempt > 1)
                    {
                        Logger.LogInformation($"Finally, changed invisible duration successfully," +
                                              $" clientId={clientId}, consumerGroup={consumerGroup}," +
                                              $" messageId={messageId}, attempt={attempt}, mq={_mq}," +
                                              $" endpoints={endpoints}, requestId={requestId}");
                    }
                    else
                    {
                        Logger.LogDebug($"Changed invisible duration successfully, clientId={clientId}," +
                                        $" consumerGroup={consumerGroup}, messageId={messageId}, mq={_mq}," +
                                        $" endpoints={endpoints}, requestId={requestId}");
                    }
                }
            });
        }

        private void ChangeInvisibleDurationLater(MessageView messageView, TimeSpan duration, int attempt,
            TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(ChangeInvisibleDurationFailureBackoffDelay, _changeInvisibleDurationCts.Token);
                    ChangeInvisibleDuration(messageView, duration, attempt, tcs);
                }
                catch (Exception ex)
                {
                    if (_changeInvisibleDurationCts.IsCancellationRequested)
                    {
                        return;
                    }
                    Logger.LogError(ex, $"[Bug] Failed to schedule message change invisible duration request," +
                                        $" mq={_mq}, messageId={messageView.MessageId}, clientId={_consumer.GetClientId()}");
                    ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                }
            });
        }

        public Task EraseFifoMessage(MessageView messageView, ConsumeResult consumeResult)
        {
            var retryPolicy = _consumer.GetRetryPolicy();
            var maxAttempts = retryPolicy.GetMaxAttempts();
            var attempt = messageView.DeliveryAttempt;
            var messageId = messageView.MessageId;
            var service = _consumer.GetConsumeService();
            var clientId = _consumer.GetClientId();

            if (consumeResult == ConsumeResult.FAILURE && attempt < maxAttempts)
            {
                var nextAttemptDelay = retryPolicy.GetNextAttemptDelay(attempt);
                attempt = messageView.IncrementAndGetDeliveryAttempt();
                Logger.LogDebug($"Prepare to redeliver the fifo message because of the consumption failure," +
                                $" maxAttempt={maxAttempts}, attempt={attempt}, mq={messageView.MessageQueue}," +
                                $" messageId={messageId}, nextAttemptDelay={nextAttemptDelay}, clientId={clientId}");
                var redeliverTask = service.Consume(messageView, nextAttemptDelay);
                _ = redeliverTask.ContinueWith(async t =>
                {
                    var result = await t;
                    await EraseFifoMessage(messageView, result);
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
            else
            {
                var success = consumeResult == ConsumeResult.SUCCESS;
                if (!success)
                {
                    Logger.LogInformation($"Failed to consume fifo message finally, run out of attempt times," +
                                          $" maxAttempts={maxAttempts}, attempt={attempt}, mq={messageView.MessageQueue}," +
                                          $" messageId={messageId}, clientId={clientId}");
                }

                var task = ConsumeResult.SUCCESS.Equals(consumeResult)
                    ? AckMessage(messageView)
                    : ForwardToDeadLetterQueue(messageView);

                _ = task.ContinueWith(_ => { EvictCache(messageView); },
                    TaskContinuationOptions.ExecuteSynchronously);
            }

            return Task.CompletedTask;
        }

        private Task ForwardToDeadLetterQueue(MessageView messageView)
        {
            var tcs = new TaskCompletionSource<bool>();
            ForwardToDeadLetterQueue(messageView, 1, tcs);
            return tcs.Task;
        }

        private void ForwardToDeadLetterQueue(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapForwardMessageToDeadLetterQueueRequest(messageView);
            var task = _consumer.GetClientManager().ForwardMessageToDeadLetterQueue(endpoints, request,
                _consumer.GetClientConfig().RequestTimeout);

            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    // Log failure and retry later.
                    Logger.LogError($"Exception raised while forward message to DLQ, would attempt to re-forward later, " +
                                $"clientId={_consumer.GetClientId()}," +
                                $" consumerGroup={_consumer.GetConsumerGroup()}," +
                                $" messageId={messageView.MessageId}, mq={_mq}", responseTask.Exception);

                    ForwardToDeadLetterQueueLater(messageView, attempt, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;

                    // Log failure and retry later.
                    if (statusCode != Code.Ok)
                    {
                        Logger.LogError($"Failed to forward message to dead letter queue," +
                                        $" would attempt to re-forward later, clientId={clientId}," +
                                        $" consumerGroup={consumerGroup}, messageId={messageId}," +
                                        $" attempt={attempt}, mq={_mq}, endpoints={endpoints}," +
                                        $" requestId={requestId}, code={statusCode}," +
                                        $" status message={status.Message}");
                        ForwardToDeadLetterQueueLater(messageView, attempt, tcs);
                        return;
                    }

                    tcs.SetResult(true);

                    // Log success.
                    if (attempt > 1)
                    {
                        Logger.LogInformation($"Re-forward message to dead letter queue successfully, " +
                                              $"clientId={clientId}, consumerGroup={consumerGroup}," +
                                              $" attempt={attempt}, messageId={messageId}, mq={_mq}," +
                                              $" endpoints={endpoints}, requestId={requestId}");
                    }
                    else
                    {
                        Logger.LogInformation($"Forward message to dead letter queue successfully, " +
                                              $"clientId={clientId}, consumerGroup={consumerGroup}," +
                                              $" messageId={messageId}, mq={_mq}, endpoints={endpoints}," +
                                              $" requestId={requestId}");
                    }
                }
            });
        }

        private void ForwardToDeadLetterQueueLater(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(ForwardMessageToDeadLetterQueueFailureBackoffDelay,
                        _forwardMessageToDeadLetterQueueCts.Token);
                    ForwardToDeadLetterQueue(messageView, attempt, tcs);
                }
                catch (Exception ex)
                {
                    // Should never reach here.
                    Logger.LogError($"[Bug] Failed to schedule DLQ message request, " +
                                    $"mq={_mq}, messageId={messageView.MessageId}, clientId={_consumer.GetClientId()}", ex);

                    ForwardToDeadLetterQueueLater(messageView, attempt + 1, tcs);
                }
            });
        }

        /// <summary>
        /// Discard the message(Non-FIFO-consume-mode) which could not be consumed properly.
        /// </summary>
        /// <param name="messageView">the message to discard.</param>
        public void DiscardMessage(MessageView messageView)
        {
            Logger.LogInformation($"Discard message, mq={_mq}, messageId={messageView.MessageId}," +
                                  $" clientId={_consumer.GetClientId()}");
            var task = NackMessage(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Discard the message(FIFO-consume-mode) which could not consumed properly.
        /// </summary>
        /// <param name="messageView">the FIFO message to discard.</param>
        public void DiscardFifoMessage(MessageView messageView)
        {
            Logger.LogInformation($"Discard fifo message, mq={_mq}, messageId={messageView.MessageId}," +
                                  $" clientId={_consumer.GetClientId()}");
            var task = ForwardToDeadLetterQueue(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void EvictCache(MessageView messageView)
        {
            _cachedMessageLock.EnterWriteLock();
            try
            {
                if (_cachedMessages.Remove(messageView))
                {
                    Interlocked.Add(ref _cachedMessagesBytes, -messageView.Body.Length);
                }
            }
            finally
            {
                _cachedMessageLock.ExitWriteLock();
            }
        }

        public int CachedMessagesCount()
        {
            _cachedMessageLock.EnterReadLock();
            try
            {
                return _cachedMessages.Count;
            }
            finally
            {
                _cachedMessageLock.ExitReadLock();
            }
        }

        public long CachedMessageBytes()
        {
            return Interlocked.Read(ref _cachedMessagesBytes);
        }

        /// <summary>
        /// Get the count of cached messages.
        /// </summary>
        /// <returns>count of pending messages.</returns>
        public long GetCachedMessageCount()
        {
            _cachedMessageLock.EnterReadLock();
            try
            {
                return _cachedMessages.Count;
            }
            finally
            {
                _cachedMessageLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get the bytes of cached message memory footprint.
        /// </summary>
        /// <returns>bytes of cached message memory footprint.</returns>
        public long GetCachedMessageBytes()
        {
            return _cachedMessagesBytes;
        }
    }
}