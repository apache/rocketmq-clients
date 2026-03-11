/**
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

import { randomUUID } from 'node:crypto';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import { MessageView } from '../message';
import { MessageQueue } from '../route';
import { TooManyRequestsException } from '../exception';
import { ConsumeResult } from './ConsumeResult';
import { FilterExpression } from './FilterExpression';
import type { PushConsumer } from './PushConsumer';

const ACK_MESSAGE_FAILURE_BACKOFF_DELAY = 1000;
const CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY = 1000;
const FORWARD_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY = 1000;

const RECEIVING_FLOW_CONTROL_BACKOFF_DELAY = 20;
const RECEIVING_FAILURE_BACKOFF_DELAY = 1000;
const RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL = 1000;

export class ProcessQueue {
  readonly #consumer: PushConsumer;
  #dropped = false;
  readonly #mq: MessageQueue;
  readonly #filterExpression: FilterExpression;
  readonly #cachedMessages: MessageView[] = [];
  #cachedMessagesBytes = 0;
  #activityTime = Date.now();
  #cacheFullTime = 0;
  #aborted = false;

  constructor(consumer: PushConsumer, mq: MessageQueue, filterExpression: FilterExpression) {
    this.#consumer = consumer;
    this.#mq = mq;
    this.#filterExpression = filterExpression;
  }

  getMessageQueue(): MessageQueue {
    return this.#mq;
  }

  drop(): void {
    this.#dropped = true;
  }

  expired(): boolean {
    const longPollingTimeout = this.#consumer.getPushConsumerSettings().getLongPollingTimeout();
    const requestTimeout = this.#consumer.requestTimeout;
    const maxIdleDuration = (longPollingTimeout + requestTimeout) * 3;
    const idleDuration = Date.now() - this.#activityTime;
    if (idleDuration < maxIdleDuration) {
      return false;
    }
    const afterCacheFullDuration = Date.now() - this.#cacheFullTime;
    if (afterCacheFullDuration < maxIdleDuration) {
      return false;
    }
    return true;
  }

  cacheMessages(messageList: MessageView[]): void {
    for (const messageView of messageList) {
      this.#cachedMessages.push(messageView);
      this.#cachedMessagesBytes += messageView.body.length;
    }
  }

  #getReceptionBatchSize(): number {
    const bufferSize = Math.max(
      this.#consumer.cacheMessageCountThresholdPerQueue() - this.cachedMessagesCount(),
      1,
    );
    return Math.min(bufferSize, this.#consumer.getPushConsumerSettings().getReceiveBatchSize());
  }

  fetchMessageImmediately(): void {
    this.#receiveMessageImmediately();
  }

  onReceiveMessageException(t: Error, attemptId?: string): void {
    const delay = t instanceof TooManyRequestsException
      ? RECEIVING_FLOW_CONTROL_BACKOFF_DELAY
      : RECEIVING_FAILURE_BACKOFF_DELAY;
    this.#receiveMessageLater(delay, attemptId);
  }

  #receiveMessageLater(delay: number, attemptId?: string): void {
    if (this.#aborted) return;
    setTimeout(() => {
      if (this.#aborted) return;
      this.#receiveMessage(attemptId);
    }, delay);
  }

  #generateAttemptId(): string {
    return randomUUID();
  }

  receiveMessage(attemptId?: string): void {
    this.#receiveMessage(attemptId);
  }

  #receiveMessage(attemptId?: string): void {
    if (this.#dropped) {
      return;
    }
    if (this.#isCacheFull()) {
      this.#receiveMessageLater(RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL, attemptId);
      return;
    }
    this.#receiveMessageImmediately(attemptId);
  }

  #receiveMessageImmediately(attemptId?: string): void {
    if (this.#aborted) return;
    attemptId = attemptId ?? this.#generateAttemptId();
    try {
      const batchSize = this.#getReceptionBatchSize();
      const longPollingTimeout = this.#consumer.getPushConsumerSettings().getLongPollingTimeout();
      const request = this.#consumer.wrapPushReceiveMessageRequest(
        batchSize, this.#mq, this.#filterExpression, longPollingTimeout, attemptId,
      );
      this.#activityTime = Date.now();

      this.#consumer.receiveMessage(request, this.#mq, longPollingTimeout)
        .then(messages => {
          this.#onReceiveMessageResult(messages);
        })
        .catch(err => {
          this.onReceiveMessageException(err, attemptId);
        });
    } catch (err) {
      this.onReceiveMessageException(err as Error, attemptId);
    }
  }

  #onReceiveMessageResult(messages: MessageView[]): void {
    if (messages.length > 0) {
      this.cacheMessages(messages);
      this.#consumer.getConsumeService().consume(this, messages);
    }
    this.#receiveMessage();
  }

  #isCacheFull(): boolean {
    const cacheMessageCountThreshold = this.#consumer.cacheMessageCountThresholdPerQueue();
    const actualCount = this.cachedMessagesCount();
    if (cacheMessageCountThreshold <= actualCount) {
      this.#cacheFullTime = Date.now();
      return true;
    }

    const cacheMessageBytesThreshold = this.#consumer.cacheMessageBytesThresholdPerQueue();
    if (cacheMessageBytesThreshold <= this.#cachedMessagesBytes) {
      this.#cacheFullTime = Date.now();
      return true;
    }

    return false;
  }

  eraseMessage(messageView: MessageView, consumeResult: ConsumeResult): void {
    const task = consumeResult === ConsumeResult.SUCCESS
      ? this.#ackMessage(messageView)
      : this.#nackMessage(messageView);
    task.finally(() => {
      this.#evictCache(messageView);
    });
  }

  async #ackMessage(messageView: MessageView, attempt = 1): Promise<void> {
    try {
      const endpoints = messageView.endpoints;
      const response = await this.#consumer.rpcClientManager.ackMessage(
        endpoints,
        this.#consumer.wrapAckMessageRequest(messageView),
        this.#consumer.requestTimeout,
      );
      const status = response.getStatus()?.toObject();
      if (status?.code === Code.INVALID_RECEIPT_HANDLE) {
        return; // Forgive to retry
      }
      if (status?.code !== Code.OK) {
        await this.#ackMessageLater(messageView, attempt + 1);
      }
    } catch {
      await this.#ackMessageLater(messageView, attempt + 1);
    }
  }

  async #ackMessageLater(messageView: MessageView, attempt: number): Promise<void> {
    if (this.#aborted) return;
    await new Promise<void>(resolve => setTimeout(resolve, ACK_MESSAGE_FAILURE_BACKOFF_DELAY));
    if (this.#aborted) return;
    await this.#ackMessage(messageView, attempt);
  }

  async #nackMessage(messageView: MessageView): Promise<void> {
    const retryPolicy = this.#consumer.getRetryPolicy();
    const delay = retryPolicy?.getNextAttemptDelay(messageView.deliveryAttempt ?? 1) ?? 0;
    await this.#changeInvisibleDuration(messageView, delay);
  }

  async #changeInvisibleDuration(messageView: MessageView, duration: number, attempt = 1): Promise<void> {
    try {
      const endpoints = messageView.endpoints;
      const response = await this.#consumer.rpcClientManager.changeInvisibleDuration(
        endpoints,
        this.#consumer.wrapChangeInvisibleDurationRequest(messageView, duration),
        this.#consumer.requestTimeout,
      );
      const status = response.getStatus()?.toObject();
      if (status?.code === Code.INVALID_RECEIPT_HANDLE) {
        return; // Forgive to retry
      }
      if (status?.code !== Code.OK) {
        await this.#changeInvisibleDurationLater(messageView, duration, attempt + 1);
      }
    } catch {
      await this.#changeInvisibleDurationLater(messageView, duration, attempt + 1);
    }
  }

  async #changeInvisibleDurationLater(messageView: MessageView, duration: number, attempt: number): Promise<void> {
    if (this.#aborted) return;
    await new Promise<void>(resolve => setTimeout(resolve, CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY));
    if (this.#aborted) return;
    await this.#changeInvisibleDuration(messageView, duration, attempt);
  }

  async eraseFifoMessage(messageView: MessageView, consumeResult: ConsumeResult): Promise<void> {
    const retryPolicy = this.#consumer.getRetryPolicy();
    const maxAttempts = retryPolicy?.getMaxAttempts() ?? 1;
    let attempt = messageView.deliveryAttempt ?? 1;

    if (consumeResult === ConsumeResult.FAILURE && attempt < maxAttempts) {
      const nextAttemptDelay = retryPolicy?.getNextAttemptDelay(attempt) ?? 0;
      // Redeliver the fifo message
      const result = await this.#consumer.getConsumeService().consumeMessage(messageView, nextAttemptDelay);
      await this.eraseFifoMessage(messageView, result);
    } else {
      const task = consumeResult === ConsumeResult.SUCCESS
        ? this.#ackMessage(messageView)
        : this.#forwardToDeadLetterQueue(messageView);
      await task;
      this.#evictCache(messageView);
    }
  }

  async #forwardToDeadLetterQueue(messageView: MessageView, attempt = 1): Promise<void> {
    try {
      const endpoints = messageView.endpoints;
      const response = await this.#consumer.rpcClientManager.forwardMessageToDeadLetterQueue(
        endpoints,
        this.#consumer.wrapForwardMessageToDeadLetterQueueRequest(messageView),
        this.#consumer.requestTimeout,
      );
      const status = response.getStatus()?.toObject();
      if (status?.code !== Code.OK) {
        await this.#forwardToDeadLetterQueueLater(messageView, attempt + 1);
      }
    } catch {
      await this.#forwardToDeadLetterQueueLater(messageView, attempt + 1);
    }
  }

  async #forwardToDeadLetterQueueLater(messageView: MessageView, attempt: number): Promise<void> {
    if (this.#aborted) return;
    await new Promise<void>(resolve => setTimeout(resolve, FORWARD_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY));
    if (this.#aborted) return;
    await this.#forwardToDeadLetterQueue(messageView, attempt);
  }

  discardMessage(messageView: MessageView): void {
    this.#nackMessage(messageView).finally(() => {
      this.#evictCache(messageView);
    });
  }

  discardFifoMessage(messageView: MessageView): void {
    this.#forwardToDeadLetterQueue(messageView).finally(() => {
      this.#evictCache(messageView);
    });
  }

  #evictCache(messageView: MessageView): void {
    const index = this.#cachedMessages.indexOf(messageView);
    if (index !== -1) {
      this.#cachedMessages.splice(index, 1);
      this.#cachedMessagesBytes -= messageView.body.length;
    }
  }

  cachedMessagesCount(): number {
    return this.#cachedMessages.length;
  }

  cachedMessageBytes(): number {
    return this.#cachedMessagesBytes;
  }

  abort(): void {
    this.#aborted = true;
  }
}
