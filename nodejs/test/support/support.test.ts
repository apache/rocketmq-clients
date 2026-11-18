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

import { strict as assert } from 'node:assert';
import * as sinon from 'sinon';
import { Metadata } from '@grpc/grpc-js';
import { ClientType, Code, DigestType, Permission, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { QueryRouteRequest, QueryRouteResponse, TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Producer } from '../../src/producer';
import { MessageView } from '../../src/message';
import { Endpoints } from '../../src/route';
import { createDuration } from '../../src/util';
import { createFakeRpcClientManager, createSilentLogger, deferred, FakeTelemetryStream, installFakeRpcClientManager } from './fakes';
import { buildMessagePB, buildRoute, buildSettings, buildStatus } from './protobuf';

describe('test/support', () => {
  afterEach(() => sinon.restore());

  it('deferred allows explicit resolve and reject', async () => {
    const success = deferred<number>();
    success.resolve(42);
    assert.strictEqual(await success.promise, 42);
    const failure = deferred<void>();
    const error = new Error('controlled rejection');
    const assertion = assert.rejects(failure.promise, reason => reason === error);
    failure.reject(error);
    await assertion;
  });

  it('silent logger records arguments without writing logs', () => {
    const logger = createSilentLogger();
    logger.info('topic=%s', 'topic-a');
    logger.warn('retry');
    logger.error(new Error('failure'));
    logger.debug('debug');
    logger.close();
    sinon.assert.calledWithExactly(logger.info, 'topic=%s', 'topic-a');
    for (const level of [ 'warn', 'error', 'debug', 'close' ] as const) {
      sinon.assert.calledOnce(logger[level]);
    }
  });

  it('RPC fake supports responses, rejections, deferred calls and call records', async () => {
    const fake = createFakeRpcClientManager();
    const endpoints = new Endpoints('127.0.0.1:8081');
    const request = new QueryRouteRequest();
    const response = new QueryRouteResponse().setStatus(buildStatus());
    const held = deferred<QueryRouteResponse>();
    fake.queryRoute.onFirstCall().resolves(response);
    fake.queryRoute.onSecondCall().rejects(new Error('route failed'));
    fake.queryRoute.onThirdCall().callsFake(() => held.promise);
    assert.strictEqual(await fake.queryRoute(endpoints, request, 3000), response);
    await assert.rejects(() => fake.queryRoute(endpoints, request, 3000), /route failed/);
    const waiting = fake.queryRoute(endpoints, request, 3000);
    held.resolve(response);
    assert.strictEqual(await waiting, response);
    sinon.assert.calledThrice(fake.queryRoute);
    assert.deepStrictEqual(fake.queryRoute.firstCall.args, [ endpoints, request, 3000 ]);
    await assert.rejects(() => fake.sendMessage(endpoints, null as never, 3000), /未配置/);
    assert.throws(() => fake.telemetry(endpoints, new Metadata()), /未配置/);
  });

  it('RPC injection closes the real manager and is used by the client', async () => {
    const client = new Producer({ endpoints: '127.0.0.1:8081', namespace: '', logger: createSilentLogger() });
    const original = client.getRpcClientManager();
    const closed = sinon.spy(original, 'close');
    try {
      const fake = installFakeRpcClientManager(client);
      sinon.assert.calledOnce(closed);
      assert.strictEqual(client.getRpcClientManager(), fake);
      await client.shutdown();
      sinon.assert.calledOnce(fake.close);
    } finally {
      original.close();
      closed.restore();
    }
  });

  it('telemetry fake records writes and exposes data, error and server end events', () => {
    const stream = new FakeTelemetryStream();
    const command = new TelemetryCommand().setSettings(buildSettings());
    const data = sinon.spy();
    const error = sinon.spy();
    const end = sinon.spy();
    stream.on('data', data).on('error', error).on('end', end);
    stream.write(command);
    stream.receive(command);
    const failure = new Error('stream failed');
    stream.fail(failure);
    stream.finishFromServer();
    assert.deepStrictEqual(stream.writes, [ command ]);
    sinon.assert.calledWithExactly(data, command);
    sinon.assert.calledWithExactly(error, failure);
    sinon.assert.calledOnce(end);
    assert.strictEqual(stream.asGrpcStream(), stream);
    stream.end();
    assert.ok(stream.writableEnded);
    stream.cancel();
    assert.ok(stream.destroyed);
  });

  it('route builder keeps broker ids, queue ids and permissions independently configurable', () => {
    const route = buildRoute({
      topic: 'topic-a', namespace: 'ns', queuesPerBroker: 2, permission: Permission.READ,
      brokers: [
        { name: 'master', id: 0, endpoints: '127.0.0.1:8081' },
        { name: 'slave', id: 1, endpoints: '127.0.0.1:8082' },
      ],
    });
    assert.deepStrictEqual(route.messageQueues.map(mq => [ mq.broker.id, mq.queueId ]), [[ 0, 0 ], [ 0, 1 ], [ 1, 0 ], [ 1, 1 ]]);
    assert.ok(route.messageQueues.every(mq => mq.permission === Permission.READ && mq.topic.name === 'topic-a'));
    assert.strictEqual(route.getTotalEndpoints().length, 2);
    assert.ok(buildRoute({ brokers: [] }).isEmpty());
  });

  for (const digestType of [ DigestType.CRC32, DigestType.MD5, DigestType.SHA1 ]) {
    it(`message builder creates a decodable compressed message with digest ${digestType}`, () => {
      const body = Buffer.from('body');
      const message = new MessageView(buildMessagePB({ body, gzip: true, digestType, priority: 0,
        liteTopic: 'lite-a', keys: [ 'key-a' ], properties: new Map([[ 'k', 'v' ]]) }), buildRoute().messageQueues[0]);
      assert.deepStrictEqual(message.body, body);
      assert.ok(!message.corrupted);
      assert.strictEqual(message.liteTopic, 'lite-a');
      assert.strictEqual(message.priority, 0);
      assert.strictEqual(message.properties.get('k'), 'v');
      assert.deepStrictEqual(message.keys, [ 'key-a' ]);
    });
  }

  it('message builder supports explicit corruption without omitting required fields', () => {
    const message = new MessageView(buildMessagePB({ checksum: 'invalid' }), buildRoute().messageQueues[0]);
    assert.strictEqual(message.corrupted, true);
    assert.strictEqual(buildStatus(Code.BAD_REQUEST, 'bad input').getMessage(), 'bad input');
  });

  it('settings builder supplies usable publishing and subscription settings', () => {
    const settings = buildSettings();
    assert.strictEqual(settings.getPublishing()?.getMaxBodySize(), 4 * 1024 * 1024);
    assert.strictEqual(settings.getBackoffPolicy()?.getMaxAttempts(), 3);
    const subscription = new Subscription().setReceiveBatchSize(10).setLongPollingTimeout(createDuration(5000));
    assert.strictEqual(buildSettings({ clientType: ClientType.SIMPLE_CONSUMER, subscription }).getSubscription(), subscription);
  });

  it('fake timers control delayed work and can be restored', async () => {
    const clock = sinon.useFakeTimers();
    const pending = deferred<void>();
    setTimeout(() => pending.resolve(), 30000);
    assert.strictEqual(clock.countTimers(), 1);
    await clock.tickAsync(30000);
    await pending.promise;
    assert.strictEqual(clock.countTimers(), 0);
  });
});
