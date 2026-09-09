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
 * Regression tests for defect fixes aligned with the Java client:
 *  - Retry backoff unit contract (milliseconds, sub-second precision kept)
 *  - CustomizedBackoffRetryPolicy implementation
 *  - Endpoints string parsing (IPv6 / http(s) prefix)
 *  - StatusChecker mappings (ILLEGAL_LITE_TOPIC / MESSAGE_BODY_EMPTY /
 *    LITE_SUBSCRIPTION_QUOTA_EXCEEDED)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import {
  ExponentialBackoffRetryPolicy,
  CustomizedBackoffRetryPolicy,
} from '../src/retry';
import { Endpoints, TopicRouteData } from '../src/route';
import { Producer, PublishingLoadBalancer } from '../src/producer';
import { Consumer } from '../src/consumer';
import { Message } from '../src/message';
import { Settings } from '../src/client';
import { calculateStringSipHash24 } from '../src/util';
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
import {
  StatusChecker,
  BadRequestException,
  PayloadEmptyException,
  LiteSubscriptionQuotaExceededException,
} from '../src/exception';
import { Code, RetryPolicy as RetryPolicyPB, ExponentialBackoff, CustomizedBackoff, TransactionResolution,
} from '../proto/apache/rocketmq/v2/definition_pb';
import { Status } from '../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatRequest,
  NotifyClientTerminationRequest,
} from '../proto/apache/rocketmq/v2/service_pb';
import {
  MessageQueue as MessageQueuePB,
  Broker as BrokerPB,
  Resource as ResourcePB,
  Endpoints as EndpointsPB,
  Permission,
  AddressScheme,
} from '../proto/apache/rocketmq/v2/definition_pb';

function statusOf(code: Code): Status.AsObject {
  return { code, message: 'mock message', requestId: '' } as unknown as Status.AsObject;
}

describe('ExponentialBackoffRetryPolicy (ms contract)', () => {
  it('should return delays in milliseconds', () => {
    const policy = new ExponentialBackoffRetryPolicy(3, 1000, 10000, 2);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 2000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 4000);
    assert.strictEqual(policy.getNextAttemptDelay(4), 8000);
    // capped by maxBackoff
    assert.strictEqual(policy.getNextAttemptDelay(5), 10000);
  });

  it('should keep sub-second precision when inheriting backoff', () => {
    const retryPolicy = new RetryPolicyPB().setExponentialBackoff(
      new ExponentialBackoff()
        .setInitial(new Duration().setSeconds(0).setNanos(500000000)) // 0.5s
        .setMax(new Duration().setSeconds(3).setNanos(250000000)) // 3.25s
        .setMultiplier(2));
    const policy = new ExponentialBackoffRetryPolicy(3).inheritBackoff(retryPolicy) as ExponentialBackoffRetryPolicy;
    assert.strictEqual(policy.getNextAttemptDelay(1), 500);
    assert.strictEqual(policy.getNextAttemptDelay(2), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 2000);
    // capped by max backoff (3.25s, sub-second precision kept)
    assert.strictEqual(policy.getNextAttemptDelay(4), 3250);
  });

  it('should serialize back to protobuf without losing precision', () => {
    const policy = new ExponentialBackoffRetryPolicy(3, 500, 3250, 2);
    const pb = policy.toProtobuf();
    const exponential = pb.getExponentialBackoff()!;
    assert.strictEqual(exponential.getInitial()!.getSeconds(), 0);
    assert.strictEqual(exponential.getInitial()!.getNanos(), 500000000);
    assert.strictEqual(exponential.getMax()!.getSeconds(), 3);
    assert.strictEqual(exponential.getMax()!.getNanos(), 250000000);
  });
});

describe('CustomizedBackoffRetryPolicy', () => {
  it('should return the Nth duration and clamp to the last one', () => {
    const policy = new CustomizedBackoffRetryPolicy([1000, 5000, 10000], 5);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 5000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 10000);
    assert.strictEqual(policy.getNextAttemptDelay(4), 10000);
    assert.strictEqual(policy.getMaxAttempts(), 5);
  });

  it('should inherit customized backoff from protobuf', () => {
    const customizedBackoff = new CustomizedBackoff();
    customizedBackoff.addNext(new Duration().setSeconds(1));
    customizedBackoff.addNext(new Duration().setNanos(500000000));
    const retryPolicy = new RetryPolicyPB()
      .setMaxAttempts(4)
      .setCustomizedBackoff(customizedBackoff);
    const policy = new CustomizedBackoffRetryPolicy([100], 3).inheritBackoff(retryPolicy);
    assert.strictEqual(policy.getMaxAttempts(), 3);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 500);
  });
});

describe('Endpoints parsing', () => {
  it('should parse IPv4 endpoints with port', () => {
    const endpoints = new Endpoints('127.0.0.1:10911');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: '127.0.0.1', port: 10911 }]);
    assert.strictEqual(endpoints.scheme, 1); // IPV4
  });

  it('should parse domain endpoints', () => {
    const endpoints = new Endpoints('example.com:443');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: 'example.com', port: 443 }]);
    assert.strictEqual(endpoints.scheme, 3); // DOMAIN_NAME
  });

  it('should parse bracketed IPv6 endpoints with port', () => {
    const endpoints = new Endpoints('[::1]:10911');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: '::1', port: 10911 }]);
    assert.strictEqual(endpoints.scheme, 2); // IPV6
  });

  it('should parse bare IPv6 endpoints without port', () => {
    const endpoints = new Endpoints('1050:0000:0000:0000:0005:0600:300c:326b');
    assert.strictEqual(endpoints.addressesList[0].host, '1050:0000:0000:0000:0005:0600:300c:326b');
    assert.strictEqual(endpoints.addressesList[0].port, 80);
    assert.strictEqual(endpoints.scheme, 2); // IPV6
  });

  it('should strip http(s) prefixes and parse multiple endpoints', () => {
    const endpoints = new Endpoints('http://127.0.0.1:10911;https://example.com:8080');
    assert.strictEqual(endpoints.addressesList.length, 2);
    assert.deepStrictEqual(endpoints.addressesList[0], { host: '127.0.0.1', port: 10911 });
    assert.deepStrictEqual(endpoints.addressesList[1], { host: 'example.com', port: 8080 });
  });
});

describe('StatusChecker mappings vs Java', () => {
  it('should map ILLEGAL_LITE_TOPIC to BadRequestException', () => {
    assert.throws(() => StatusChecker.check(statusOf(Code.ILLEGAL_LITE_TOPIC)), BadRequestException);
  });

  it('should map MESSAGE_BODY_EMPTY to PayloadEmptyException', () => {
    assert.throws(() => StatusChecker.check(statusOf(Code.MESSAGE_BODY_EMPTY)), PayloadEmptyException);
  });

  it('should map LITE_SUBSCRIPTION_QUOTA_EXCEEDED to LiteSubscriptionQuotaExceededException', () => {
    assert.throws(
      () => StatusChecker.check(statusOf(Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED)),
      LiteSubscriptionQuotaExceededException);
  });
});

describe('Endpoints.getGrpcTarget with resolver scheme (B-1)', () => {
  it('should prefix ipv4: scheme for IPv4 addresses', () => {
    assert.strictEqual(new Endpoints('127.0.0.1:10911').getGrpcTarget(), 'ipv4:127.0.0.1:10911');
  });

  it('should prefix ipv4: scheme for multiple IPv4 addresses', () => {
    const target = new Endpoints('127.0.0.1:8081;127.0.0.2:8082').getGrpcTarget();
    assert.strictEqual(target, 'ipv4:127.0.0.1:8081,127.0.0.2:8082');
  });

  it('should prefix ipv6: scheme with brackets for IPv6 addresses', () => {
    assert.strictEqual(new Endpoints('[::1]:10911').getGrpcTarget(), 'ipv6:[::1]:10911');
  });

  it('should prefix dns: scheme for domain names', () => {
    assert.strictEqual(new Endpoints('example.com:8080').getGrpcTarget(), 'dns:example.com:8080');
  });
});

describe('takeMessageQueueByMessageGroup floorMod semantics (C-1)', () => {
  // One queue (queueId=0, the writable master queue) per broker across queueCount brokers,
  // since PublishingLoadBalancer only keeps queues with queueId === MASTER_BROKER_ID (0).
  // The returned queue is identified by its broker endpoints port (10911 + expected index).
  function buildLoadBalancer(queueCount: number): PublishingLoadBalancer {
    const pbs: MessageQueuePB[] = [];
    for (let i = 0; i < queueCount; i++) {
      const endpointsPb = new EndpointsPB();
      endpointsPb.setScheme(AddressScheme.IPV4);
      endpointsPb.addAddresses().setHost('127.0.0.1').setPort(10911 + i);
      const brokerPb = new BrokerPB();
      brokerPb.setName('broker-' + i);
      brokerPb.setId(0);
      brokerPb.setEndpoints(endpointsPb);
      const mqPb = new MessageQueuePB();
      mqPb.setId(0);
      mqPb.setTopic(new ResourcePB().setName('topic'));
      mqPb.setBroker(brokerPb);
      mqPb.setPermission(Permission.READ_WRITE);
      pbs.push(mqPb);
    }
    return new PublishingLoadBalancer(new TopicRouteData(pbs));
  }

  it('should always yield a non-negative index matching Java LongMath.mod', () => {
    const loadBalancer = buildLoadBalancer(8);
    const groups = ['group-a', 'fifo-group', 'group-中文', 'x', 'order-12345', ''];
    for (const group of groups) {
      const mq = loadBalancer.takeMessageQueueByMessageGroup(group);
      assert.ok(mq, 'should resolve a message queue for group=' + group);
      // Expected index under floorMod semantics: ((signed hash % size) + size) % size
      const signedHash = BigInt.asIntN(64, calculateStringSipHash24(group));
      const expectedIndex = Number(((signedHash % 8n) + 8n) % 8n);
      assert.strictEqual(mq.broker.endpoints.facade, '127.0.0.1:' + (10911 + expectedIndex));
    }
  });

  it('should match Java floorMod for hashes that are negative when interpreted signed', () => {
    // Find a message group whose SipHash-2-4 has its high bit set (negative as int64)
    let negativeGroup: string | undefined;
    for (let i = 0; i < 10000; i++) {
      const candidate = 'probe-' + i;
      if (BigInt.asIntN(64, calculateStringSipHash24(candidate)) < 0n) {
        negativeGroup = candidate;
        break;
      }
    }
    assert.ok(negativeGroup, 'expected to find a negative hash among probes');
    const loadBalancer = buildLoadBalancer(4);
    const mq = loadBalancer.takeMessageQueueByMessageGroup(negativeGroup!);
    const signedHash = BigInt.asIntN(64, calculateStringSipHash24(negativeGroup!));
    const expectedIndex = Number(((signedHash % 4n) + 4n) % 4n);
    assert.strictEqual(mq.broker.endpoints.facade, '127.0.0.1:' + (10911 + expectedIndex));
  });
});

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
  }

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
