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

/**
 * Tests for the message interceptor / hook framework and the
 * NOTIFY_UNSUBSCRIBE_LITE_COMMAND telemetry dispatch:
 *  - Telemetry command dispatch vs Java (J-1)
 *  - RECEIVE hook timing and context reuse (J-2)
 *  - Message interceptor chain (J-3)
 *  - Transaction hook points (K-2)
 *  - FORWARD_TO_DLQ hook point (K-3)
 *  - PushConsumer settlement hooks (K-4)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Endpoints } from '../src/route';
import { Producer } from '../src/producer';
import {
  ConsumeResult,
  ConsumeResultSuspend,
  Consumer,
  FilterExpression,
  ProcessQueue,
} from '../src/consumer';
import { Message } from '../src/message';
import { Settings } from '../src/client';
import {
  Attribute,
  AttributeKey,
  CompositedMessageInterceptor,
  InflightRequestCountInterceptor,
  MessageHookPoints,
  MessageHookPointsStatus,
  MessageInterceptor,
  MessageInterceptorContextImpl,
} from '../src/hook';
import { Code, TransactionResolution } from '../proto/apache/rocketmq/v2/definition_pb';
import { Status } from '../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatRequest,
  NotifyClientTerminationRequest,
  ReceiveMessageRequest,
  ReceiveMessageResponse,
} from '../proto/apache/rocketmq/v2/service_pb';

function statusOf(code: Code): Status.AsObject {
  return { code, message: 'mock message', requestId: '' } as unknown as Status.AsObject;
}

/**
 * Minimal response shape accepted by both the hook wrapper (which reads
 * `getStatus().getCode()`) and ProcessQueue (which reads `getStatus().toObject()`).
 */
function responseOf(code: Code) {
  return {
    getStatus: () => ({
      getCode: () => code,
      toObject: () => statusOf(code),
    }),
  };
}

/**
 * Concrete {@link Consumer} used to exercise the hook points without a broker.
 */
class HookTestConsumer extends Consumer {
  protected getSettings(): Settings {
    return {} as Settings;
  }
  protected wrapHeartbeatRequest(): HeartbeatRequest {
    return new HeartbeatRequest();
  }
  protected wrapNotifyClientTerminationRequest(): NotifyClientTerminationRequest {
    return new NotifyClientTerminationRequest();
  }

  /** Expose the protected receive entry point used by ProcessQueue. */
  receiveFrom(mq: any, awaitDuration = 0) {
    return this.receiveMessage(new ReceiveMessageRequest(), mq, awaitDuration);
  }
}

/** Records the hook points as `before:POINT` / `after:POINT:STATUS` strings. */
function eventRecorder(events: string[], key?: AttributeKey<string>, value?: string): MessageInterceptor {
  return {
    doBefore: context => {
      events.push(`before:${MessageHookPoints[context.getMessageHookPoints()]}`);
      if (key && value !== undefined) {
        context.putAttribute(key, Attribute.create(value));
      }
    },
    doAfter: context => {
      const status = context.getStatus();
      events.push(`after:${MessageHookPoints[context.getMessageHookPoints()]}:`
        + `${status === undefined ? 'UNSET' : MessageHookPointsStatus[status]}`);
      if (key) {
        events.push(`attr:${context.getAttribute(key)?.get()}`);
      }
    },
  };
}

describe('Telemetry command dispatch vs Java (J-1)', () => {
  it('should dispatch NOTIFY_UNSUBSCRIBE_LITE_COMMAND to the client', () => {
    const { EventEmitter } = require('node:events');
    const { TelemetryCommand, NotifyUnsubscribeLiteCommand } = require('../proto/apache/rocketmq/v2/service_pb');
    const { TelemetrySession } = require('../src/client/TelemetrySession');

    const received: Array<{ liteTopic: string; facade: string }> = [];
    const stream = new EventEmitter();
    stream.write = () => true;
    stream.end = () => undefined;
    stream.removeAllListeners = () => stream;

    const fakeClient = {
      clientId: 'fake-client-id',
      createTelemetryStream: () => stream,
      onNotifyUnsubscribeLiteCommand(endpoints: any, command: any) {
        received.push({ liteTopic: command.getLiteTopic(), facade: endpoints.facade });
      },
    };
    new TelemetrySession(fakeClient, new Endpoints('127.0.0.1:8081'),
      { info() {}, warn() {}, error() {}, debug() {} });

    const telemetryCommand = new TelemetryCommand();
    telemetryCommand.setNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic('lite-topic-1'));
    stream.emit('data', telemetryCommand);

    assert.strictEqual(received.length, 1);
    assert.strictEqual(received[0].liteTopic, 'lite-topic-1');
    assert.strictEqual(received[0].facade, '127.0.0.1:8081');
  });
});

describe('RECEIVE hook timing vs Java (J-2)', () => {
  const mq = { broker: { endpoints: new Endpoints('127.0.0.1:8081') } } as any;

  function createConsumer(messageInterceptor: MessageInterceptor | MessageInterceptor[]) {
    return new HookTestConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'TestGroup',
      messageInterceptor,
    } as any);
  }

  function okReceiveResponse() {
    return [ new ReceiveMessageResponse().setStatus(new Status().setCode(Code.OK)) ];
  }

  it('should run RECEIVE doBefore before the RPC so in-flight requests are observable', async () => {
    const events: string[] = [];
    const inflight = new InflightRequestCountInterceptor();
    const consumer = createConsumer([ eventRecorder(events), inflight ]);
    let inflightDuringRpc = -1;
    (consumer as any).rpcClientManager = {
      receiveMessage: async () => {
        inflightDuringRpc = inflight.getInflightReceiveRequestCount();
        events.push('rpc');
        return okReceiveResponse();
      },
    };
    assert.deepStrictEqual(await consumer.receiveFrom(mq), []);
    // Before the fix doBefore ran after the RPC, so the counter was always 0 here.
    assert.strictEqual(inflightDuringRpc, 1);
    assert.strictEqual(inflight.getInflightReceiveRequestCount(), 0);
    assert.deepStrictEqual(events, [ 'before:RECEIVE', 'rpc', 'after:RECEIVE:OK' ]);
  });

  it('should carry the doBefore context into doAfter when messages are received', async () => {
    const events: string[] = [];
    const key = AttributeKey.create<string>('receive-attr');
    const consumer = createConsumer(eventRecorder(events, key, 'set-in-before'));
    (consumer as any).rpcClientManager = {
      receiveMessage: async () => okReceiveResponse(),
    };
    await consumer.receiveFrom(mq);
    assert.deepStrictEqual(events, [
      'before:RECEIVE', 'after:RECEIVE:OK', 'attr:set-in-before',
    ]);
  });

  it('should reuse the same context and fire once when the RPC fails', async () => {
    const events: string[] = [];
    const key = AttributeKey.create<string>('receive-attr');
    const inflight = new InflightRequestCountInterceptor();
    const consumer = createConsumer([ eventRecorder(events, key, 'set-in-before'), inflight ]);
    (consumer as any).rpcClientManager = {
      receiveMessage: async () => {
        throw new Error('receive failed');
      },
    };
    await assert.rejects(async () => consumer.receiveFrom(mq));
    assert.strictEqual(inflight.getInflightReceiveRequestCount(), 0);
    assert.deepStrictEqual(events, [
      'before:RECEIVE', 'after:RECEIVE:ERROR', 'attr:set-in-before',
    ]);
  });
});

describe('Transaction hook points vs Java (K-2)', () => {
  function createProducer(events: string[]) {
    const producer = new Producer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      topics: [ 'TopicTestForTransaction' ],
      messageInterceptor: {
        doBefore: context => {
          events.push(`before:${MessageHookPoints[context.getMessageHookPoints()]}`);
        },
        doAfter: context => {
          events.push(`after:${MessageHookPoints[context.getMessageHookPoints()]}:${MessageHookPointsStatus[context.getStatus()]}`);
        },
      },
    } as any);
    return producer;
  }

  function stubEndTransaction(producer: Producer, status: Status.AsObject) {
    (producer as any).rpcClientManager = {
      endTransaction: async () => ({
        getStatus: () => ({
          getCode: () => status.code,
          toObject: () => status,
        }),
      }),
    };
  }

  it('should trigger COMMIT_TRANSACTION on commit and ROLLBACK_TRANSACTION on rollback', async () => {
    const commitEvents: string[] = [];
    const producer = createProducer(commitEvents);
    stubEndTransaction(producer, statusOf(Code.OK));
    await producer.endTransaction(new Endpoints('127.0.0.1:8081'),
      new Message({ topic: 'TopicTestForTransaction', body: Buffer.from('body') }),
      'message-id', 'transaction-id', TransactionResolution.COMMIT);
    assert.deepStrictEqual(commitEvents, [
      'before:COMMIT_TRANSACTION', 'after:COMMIT_TRANSACTION:OK',
    ]);

    const rollbackEvents: string[] = [];
    const rollbackProducer = createProducer(rollbackEvents);
    stubEndTransaction(rollbackProducer, statusOf(Code.OK));
    await rollbackProducer.endTransaction(new Endpoints('127.0.0.1:8081'),
      new Message({ topic: 'TopicTestForTransaction', body: Buffer.from('body') }),
      'message-id', 'transaction-id', TransactionResolution.ROLLBACK);
    assert.deepStrictEqual(rollbackEvents, [
      'before:ROLLBACK_TRANSACTION', 'after:ROLLBACK_TRANSACTION:OK',
    ]);
  });

  it('should mark the transaction hook as ERROR when the RPC fails', async () => {
    const events: string[] = [];
    const producer = createProducer(events);
    (producer as any).rpcClientManager = {
      endTransaction: async () => {
        throw new Error('end transaction failed');
      },
    };
    await assert.rejects(async () => {
      await producer.endTransaction(new Endpoints('127.0.0.1:8081'),
        new Message({ topic: 'TopicTestForTransaction', body: Buffer.from('body') }),
        'message-id', 'transaction-id', TransactionResolution.COMMIT);
    });
    assert.deepStrictEqual(events, [
      'before:COMMIT_TRANSACTION', 'after:COMMIT_TRANSACTION:ERROR',
    ]);
  });
});

describe('FORWARD_TO_DLQ hook point vs Java (K-3)', () => {
  function createConsumer(events: string[]) {
    return new HookTestConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'TestGroup',
      messageInterceptor: {
        doBefore: context => {
          events.push(`before:${MessageHookPoints[context.getMessageHookPoints()]}`);
        },
        doAfter: context => {
          events.push(`after:${MessageHookPoints[context.getMessageHookPoints()]}:${MessageHookPointsStatus[context.getStatus()]}`);
        },
      },
    } as any);
  }

  it('should trigger FORWARD_TO_DLQ with the status carried out of the response', async () => {
    const okEvents: string[] = [];
    const okConsumer = createConsumer(okEvents);
    (okConsumer as any).rpcClientManager = {
      forwardMessageToDeadLetterQueue: async () => ({ getStatus: () => ({ getCode: () => Code.OK }) }),
    };
    await okConsumer.forwardMessageToDeadLetterQueueViaRpc(new Endpoints('127.0.0.1:8081'), {}, 3000,
      { topic: 'TopicTest' } as any);
    assert.deepStrictEqual(okEvents, [ 'before:FORWARD_TO_DLQ', 'after:FORWARD_TO_DLQ:OK' ]);

    const errEvents: string[] = [];
    const errConsumer = createConsumer(errEvents);
    (errConsumer as any).rpcClientManager = {
      forwardMessageToDeadLetterQueue: async () => ({ getStatus: () => ({ getCode: () => Code.INTERNAL_ERROR }) }),
    };
    await errConsumer.forwardMessageToDeadLetterQueueViaRpc(new Endpoints('127.0.0.1:8081'), {}, 3000,
      { topic: 'TopicTest' } as any);
    assert.deepStrictEqual(errEvents, [ 'before:FORWARD_TO_DLQ', 'after:FORWARD_TO_DLQ:ERROR' ]);
  });

  it('should mark FORWARD_TO_DLQ as ERROR when the RPC throws', async () => {
    const events: string[] = [];
    const consumer = createConsumer(events);
    (consumer as any).rpcClientManager = {
      forwardMessageToDeadLetterQueue: async () => {
        throw new Error('forward failed');
      },
    };
    await assert.rejects(async () => {
      await consumer.forwardMessageToDeadLetterQueueViaRpc(new Endpoints('127.0.0.1:8081'), {}, 3000,
        { topic: 'TopicTest' } as any);
    });
    assert.deepStrictEqual(events, [ 'before:FORWARD_TO_DLQ', 'after:FORWARD_TO_DLQ:ERROR' ]);
  });
});

describe('PushConsumer settlement hooks vs Java (K-4)', () => {
  const mq = {
    topic: { name: 'TopicTest' },
    broker: { name: 'broker-a', endpoints: new Endpoints('127.0.0.1:8081') },
    queueId: 0,
  } as any;

  /** PushConsumer-shaped surface that {@link ProcessQueue} relies on for settlement. */
  class FakePushConsumer extends HookTestConsumer {
    get requestTimeoutValue(): number {
      return 3000;
    }
    wrapAckMessageRequest(): any {
      return {};
    }
    wrapChangeInvisibleDurationRequest(): any {
      return {};
    }
    getRetryPolicy() {
      return { getMaxAttempts: () => 1, getNextAttemptDelay: () => 0 };
    }
    incrementConsumptionOkQuantity(): void { /* not collected in tests */ }
    incrementConsumptionErrorQuantity(): void { /* not collected in tests */ }
    protected isLiteConsumer(): boolean {
      // Only lite consumers keep a ConsumeResultSuspend, which drives the
      // CHANGE_INVISIBLE_DURATION settlement path.
      return true;
    }
  }

  function createConsumer(events: string[]) {
    return new FakePushConsumer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      consumerGroup: 'TestGroup',
      messageInterceptor: eventRecorder(events),
    } as any);
  }

  function messageView() {
    return {
      endpoints: new Endpoints('127.0.0.1:8081'),
      topic: 'TopicTest',
      messageId: '010000000000000001',
      deliveryAttempt: 1,
      body: Buffer.from('body'),
      incrementAndGetDeliveryAttempt: () => 2,
    } as any;
  }

  it('should fire the ACK hook from ProcessQueue once per attempt, including retries', async () => {
    const events: string[] = [];
    const consumer = createConsumer(events);
    let attempts = 0;
    (consumer as any).rpcClientManager = {
      ackMessage: async () => {
        attempts++;
        events.push('rpc');
        // The first attempt asks for a retry, which sleeps for the ack backoff.
        return responseOf(attempts === 1 ? Code.INTERNAL_ERROR : Code.OK);
      },
    };
    const processQueue = new ProcessQueue(consumer as any, mq, new FilterExpression('*'));
    await processQueue.eraseFifoMessage(messageView(), ConsumeResult.SUCCESS);
    assert.strictEqual(attempts, 2);
    assert.deepStrictEqual(events, [
      'before:ACK', 'rpc', 'after:ACK:ERROR',
      'before:ACK', 'rpc', 'after:ACK:OK',
    ]);
  });

  it('should fire the CHANGE_INVISIBLE_DURATION hook when consumption is suspended', async () => {
    const events: string[] = [];
    const consumer = createConsumer(events);
    (consumer as any).rpcClientManager = {
      changeInvisibleDuration: async () => {
        events.push('rpc');
        return responseOf(Code.OK);
      },
    };
    const processQueue = new ProcessQueue(consumer as any, mq, new FilterExpression('*'));
    await processQueue.eraseFifoMessage(messageView(), ConsumeResultSuspend.of(50));
    assert.deepStrictEqual(events, [
      'before:CHANGE_INVISIBLE_DURATION', 'rpc', 'after:CHANGE_INVISIBLE_DURATION:OK',
    ]);
  });

  it('should mark the settlement hook as ERROR when the RPC throws', async () => {
    const events: string[] = [];
    const consumer = createConsumer(events);
    (consumer as any).rpcClientManager = {
      ackMessage: async () => {
        throw new Error('ack failed');
      },
    };
    await assert.rejects(async () => {
      await consumer.ackMessageViaRpc(new Endpoints('127.0.0.1:8081'), {} as any, 3000, messageView());
    });
    assert.deepStrictEqual(events, [ 'before:ACK', 'after:ACK:ERROR' ]);
  });
});

describe('Message interceptor chain (J-3)', () => {
  it('should run doBefore in order and doAfter in reverse order', () => {
    const calls: string[] = [];
    const mk = (name: string): MessageInterceptor => ({
      doBefore: () => { calls.push('before-' + name); },
      doAfter: () => { calls.push('after-' + name); },
    });
    const composited = new CompositedMessageInterceptor([ mk('a'), mk('b'), mk('c') ]);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    composited.doBefore(context, []);
    composited.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), []);
    assert.deepStrictEqual(calls, [
      'before-a', 'before-b', 'before-c', 'after-c', 'after-b', 'after-a',
    ]);
  });

  it('should not break the pipeline when an interceptor throws', () => {
    const calls: string[] = [];
    const composited = new CompositedMessageInterceptor([
      { doBefore: () => { calls.push('before-throwing'); }, doAfter: () => { calls.push('after-throwing'); } },
      { doBefore: () => { throw new Error('boom'); }, doAfter: () => { throw new Error('boom'); } },
      { doBefore: () => { calls.push('before-ok'); }, doAfter: () => { calls.push('after-ok'); } },
    ]);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
    composited.doBefore(context, []);
    composited.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), []);
    assert.deepStrictEqual(calls, [ 'before-throwing', 'before-ok', 'after-ok', 'after-throwing' ]);
  });

  it('should carry attributes between doBefore and doAfter', () => {
    const key = AttributeKey.create<string>('k');
    let seen: string | undefined;
    const composited = new CompositedMessageInterceptor([
      {
        doBefore: context => { context.putAttribute(key, Attribute.create('v')); },
        doAfter: context => { seen = context.getAttribute(key)?.get(); },
      },
    ]);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
    composited.doBefore(context, []);
    composited.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), []);
    assert.strictEqual(seen, 'v');
  });

  it('should accumulate attributes across interceptors in doBefore', () => {
    const first = AttributeKey.create<string>('first');
    const second = AttributeKey.create<string>('second');
    const seen: Array<Array<string | undefined>> = [];
    const afterSeen: Array<Array<string | undefined>> = [];
    const mk = (put?: AttributeKey<string>, value?: string): MessageInterceptor => ({
      doBefore: context => {
        seen.push([ context.getAttribute(first)?.get(), context.getAttribute(second)?.get() ]);
        if (put && value !== undefined) {
          context.putAttribute(put, Attribute.create(value));
        }
      },
      doAfter: context => {
        afterSeen.push([ context.getAttribute(first)?.get(), context.getAttribute(second)?.get() ]);
      },
    });
    const composited = new CompositedMessageInterceptor([
      mk(first, 'v1'),
      mk(second, 'v2'),
      mk(),
    ]);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    composited.doBefore(context, []);
    // Each interceptor must observe everything the previous ones recorded.
    assert.deepStrictEqual(seen, [[ undefined, undefined ], [ 'v1', undefined ], [ 'v1', 'v2' ]]);
    composited.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), []);
    // doAfter runs in reverse order and keeps the accumulated attributes.
    assert.deepStrictEqual(afterSeen, [[ 'v1', 'v2' ], [ 'v1', 'v2' ], [ 'v1', 'v2' ]]);
  });

  it('should count inflight receive requests via InflightRequestCountInterceptor', () => {
    const interceptor = new InflightRequestCountInterceptor();
    const composited = new CompositedMessageInterceptor([ interceptor ]);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
    composited.doBefore(context, []);
    composited.doBefore(context, []);
    assert.strictEqual(interceptor.getInflightReceiveRequestCount(), 2);
    composited.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), []);
    assert.strictEqual(interceptor.getInflightReceiveRequestCount(), 1);
  });
});
