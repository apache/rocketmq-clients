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

import * as sinon from 'sinon';
import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageResponse,
  ChangeInvisibleDurationResponse,
  ForwardMessageToDeadLetterQueueResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ConsumeService } from '../../src/consumer/ConsumeService';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { ProcessQueue } from '../../src/consumer/ProcessQueue';
import type { PushConsumer } from '../../src/consumer/PushConsumer';
import { MessageView } from '../../src/message';
import type { RetryPolicy } from '../../src/retry';
import { createSilentLogger } from '../support/fakes';
import { buildMessagePB, buildRoute, buildStatus } from '../support/protobuf';

export const ackResponse = (code = Code.OK) => new AckMessageResponse().setStatus(buildStatus(code));
export const changeInvisibleDurationResponse = (code = Code.OK) =>
  new ChangeInvisibleDurationResponse().setStatus(buildStatus(code));
export const forwardToDLQResponse = (code = Code.OK) =>
  new ForwardMessageToDeadLetterQueueResponse().setStatus(buildStatus(code));

export function buildMessageView(options: Parameters<typeof buildMessagePB>[0] = {}) {
  return new MessageView(buildMessagePB(options), buildRoute().messageQueues[0]);
}

export interface HarnessOptions {
  consumeConcurrentlyMax?: number;
  cacheMessageCount?: number;
  cacheMessageBytes?: number;
  receiveBatchSize?: number;
  maxAttempts?: number;
  nextAttemptDelay?: number;
  liteConsumer?: boolean;
}

/**
 * A ProcessQueue backed by a stubbed PushConsumer: every RPC and settlement
 * call is a sinon stub, so tests drive and observe the queue without a broker.
 */
export function createProcessQueueHarness(options: HarnessOptions = {}) {
  const logger = createSilentLogger();
  const retryPolicy = {
    getMaxAttempts: sinon.stub().returns(options.maxAttempts ?? 3),
    getNextAttemptDelay: sinon.stub().returns(options.nextAttemptDelay ?? 0),
  };
  let consumeService: ConsumeService | undefined;
  const consumer = {
    requestTimeoutValue: 3000,
    getPushConsumerSettings: () => ({
      getConsumeConcurrentlyMax: () => options.consumeConcurrentlyMax ?? 32,
      getLongPollingTimeout: () => 30000,
      getReceiveBatchSize: () => options.receiveBatchSize ?? 32,
    }),
    cacheMessageCountThresholdPerQueue: () => options.cacheMessageCount ?? 1024,
    cacheMessageBytesThresholdPerQueue: () => options.cacheMessageBytes ?? 64 * 1024 * 1024,
    getConsumeService: () => consumeService,
    getRetryPolicy: () => retryPolicy as unknown as RetryPolicy,
    getConsumerGroup: () => 'group-a',
    wrapPushReceiveMessageRequest: sinon.stub().callsFake(
      (batchSize: number, _mq: unknown, _filter: unknown, _timeout: number, attemptId?: string) =>
        ({ batchSize, attemptId })),
    receiveMessage: sinon.stub(),
    wrapAckMessageRequest: sinon.stub().returns({}),
    ackMessageViaRpc: sinon.stub().resolves(ackResponse()),
    wrapChangeInvisibleDurationRequest: sinon.stub().callsFake(
      (_messageView: MessageView, duration: number) => ({ duration })),
    changeInvisibleDurationViaRpc: sinon.stub().resolves(changeInvisibleDurationResponse()),
    wrapForwardMessageToDeadLetterQueueRequest: sinon.stub().returns({}),
    forwardMessageToDeadLetterQueueViaRpc: sinon.stub().resolves(forwardToDLQResponse()),
    incrementConsumptionOkQuantity: sinon.stub(),
    incrementConsumptionErrorQuantity: sinon.stub(),
  };
  const pq = new ProcessQueue(consumer as unknown as PushConsumer, buildRoute().messageQueues[0],
    new FilterExpression('*'), { logger, clientId: 'client-a', liteConsumer: options.liteConsumer ?? false });
  return {
    pq,
    consumer,
    logger,
    retryPolicy,
    setConsumeService(service: ConsumeService) {
      consumeService = service;
    },
  };
}

// Fakes timers and Date only; setImmediate stays real so flushPromises() can drain promise chains.
export function useFakeTimers() {
  return sinon.useFakeTimers({ toFake: [ 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date' ] });
}

// Drains every pending promise callback that does not wait on a timer.
export function flushPromises() {
  return new Promise<void>(resolve => setImmediate(resolve));
}
