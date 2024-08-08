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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    public class Transaction : ITransaction
    {
        private const int MaxMessageNum = 1;

        private readonly Producer _producer;
        private readonly HashSet<PublishingMessage> _messages;
        private readonly ReaderWriterLockSlim _messagesLock;
        private readonly ConcurrentDictionary<PublishingMessage, SendReceipt> _messageSendReceiptDict;

        public Transaction(Producer producer)
        {
            _producer = producer;
            _messages = new HashSet<PublishingMessage>();
            _messagesLock = new ReaderWriterLockSlim();
            _messageSendReceiptDict = new ConcurrentDictionary<PublishingMessage, SendReceipt>();
        }

        public PublishingMessage TryAddMessage(Message message)
        {
            _messagesLock.EnterReadLock();
            try
            {
                if (_messages.Count >= MaxMessageNum)
                {
                    throw new ArgumentException($"Message in transaction has exceed the threshold: {MaxMessageNum}");
                }
            }
            finally
            {
                _messagesLock.ExitReadLock();
            }

            _messagesLock.EnterWriteLock();
            try
            {
                if (_messages.Count >= MaxMessageNum)
                {
                    throw new ArgumentException($"Message in transaction has exceed the threshold: {MaxMessageNum}");
                }

                var publishingMessage = new PublishingMessage(message, _producer.PublishingSettings, true);
                _messages.Add(publishingMessage);
                return publishingMessage;
            }
            finally
            {
                _messagesLock.ExitWriteLock();
            }
        }

        public void TryAddReceipt(PublishingMessage publishingMessage, SendReceipt sendReceipt)
        {
            _messagesLock.EnterReadLock();
            try
            {
                if (!_messages.Contains(publishingMessage))
                {
                    throw new ArgumentException("Message is not in the transaction");
                }

                _messageSendReceiptDict[publishingMessage] = sendReceipt;
            }
            finally
            {
                _messagesLock.ExitReadLock();
            }
        }

        public async Task Commit()
        {
            if (State.Running != _producer.State)
            {
                throw new InvalidOperationException("Producer is not running");
            }

            if (_messageSendReceiptDict.IsEmpty)
            {
                throw new ArgumentException("Transactional message has not been sent yet");
            }

            foreach (var (publishingMessage, sendReceipt) in _messageSendReceiptDict)
            {
                await _producer.EndTransaction(sendReceipt.Endpoints, publishingMessage.Topic, sendReceipt.MessageId,
                    sendReceipt.TransactionId, TransactionResolution.Commit);
            }
        }

        public async Task Rollback()
        {
            if (State.Running != _producer.State)
            {
                throw new InvalidOperationException("Producer is not running");
            }

            if (_messageSendReceiptDict.IsEmpty)
            {
                throw new ArgumentException("Transaction message has not been sent yet");
            }

            foreach (var (publishingMessage, sendReceipt) in _messageSendReceiptDict)
            {
                await _producer.EndTransaction(sendReceipt.Endpoints, publishingMessage.Topic, sendReceipt.MessageId,
                    sendReceipt.TransactionId, TransactionResolution.Rollback);
            }
        }
    }
}