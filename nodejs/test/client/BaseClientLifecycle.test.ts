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
import { hostname } from 'node:os';
import * as sinon from 'sinon';
import { ClientType, Code, Settings as SettingsPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatRequest,
  HeartbeatResponse,
  NotifyClientTerminationRequest,
  NotifyClientTerminationResponse,
  PrintThreadStackTraceCommand,
  QueryRouteResponse,
  RecoverOrphanedTransactionCommand,
  TelemetryCommand,
  VerifyMessageCommand,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { BaseClient, BaseClientOptions } from '../../src/client/BaseClient';
import { ClientId } from '../../src/client/ClientId';
import { Settings } from '../../src/client/Settings';
import { MessageHookPoints, MessageInterceptor, MessageInterceptorContextImpl } from '../../src/hook';
import { Endpoints, TopicRouteData } from '../../src/route';
import { createFakeRpcClientManager, createSilentLogger, deferred, FakeTelemetryStream, installFakeRpcClientManager } from '../support/fakes';
import { buildRoute, buildSettings, buildStatus } from '../support/protobuf';

const BROKER_A = '127.0.0.1:8081';
const BROKER_B = '127.0.0.1:8082';

class StubSettings extends Settings {
  constructor() {
    super('', 'test-client-id', ClientType.PRODUCER, new Endpoints(BROKER_A), 3000);
  }

  toProtobuf(): SettingsPB {
    return new SettingsPB();
  }

  sync(): void {
    // the lifecycle tests do not depend on settings content
  }
}

class TestClient extends BaseClient {
  readonly routeUpdates: Array<[ string, TopicRouteData ]> = [];

  protected getClientType(): ClientType {
    return ClientType.PRODUCER;
  }

  protected getSettings(): Settings {
    return new StubSettings();
  }

  protected wrapHeartbeatRequest(): HeartbeatRequest {
    return new HeartbeatRequest();
  }

  protected wrapNotifyClientTerminationRequest(): NotifyClientTerminationRequest {
    return new NotifyClientTerminationRequest();
  }

  protected onTopicRouteDataUpdate(topic: string, topicRouteData: TopicRouteData) {
    this.routeUpdates.push([ topic, topicRouteData ]);
  }

  route(topic: string) {
    return this.getRouteData(topic);
  }

  heartbeat(): Promise<void> {
    return (this as any).doHeartbeat();
  }

  get isolatedEndpoints() {
    return this.isolated;
  }

  get routeCache() {
    return this.topicRouteCache;
  }
}

function routeResponse(...brokers: string[]) {
  const route = buildRoute({ brokers: brokers.map((endpoints, i) => ({ name: `broker-${i}`, endpoints })) });
  return new QueryRouteResponse().setStatus(buildStatus())
    .setMessageQueuesList(route.messageQueues.map(mq => mq.toProtobuf()));
}

function heartbeatResponse(code = Code.OK) {
  return new HeartbeatResponse().setStatus(buildStatus(code, code === Code.OK ? '' : 'heartbeat rejected'));
}

function createHarness(options: Partial<BaseClientOptions> = {}) {
  const logger = createSilentLogger();
  const client = new TestClient({ endpoints: BROKER_A, namespace: '', logger, ...options });
  const rpc = installFakeRpcClientManager(client, createFakeRpcClientManager());
  const streams: FakeTelemetryStream[] = [];
  rpc.telemetry.callsFake(() => {
    const stream = new FakeTelemetryStream();
    streams.push(stream);
    return stream.asGrpcStream();
  });
  rpc.notifyClientTermination.resolves(new NotifyClientTerminationResponse().setStatus(buildStatus()));
  const meterShutdown = sinon.spy((client as any).clientMeterManager, 'shutdown');
  return { client, rpc, logger, streams, meterShutdown };
}

async function startClient(harness: ReturnType<typeof createHarness>, clock: sinon.SinonFakeTimers) {
  const startup = harness.client.startup();
  await clock.tickAsync(0);
  harness.client.onSettingsCommand(harness.client.getEndpoints(), buildSettings());
  await startup;
}

describe('BaseClient lifecycle', () => {
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    // Start from the real wall clock: the transport recovery cooldown compares against epoch-based timestamps.
    clock = sinon.useFakeTimers({ now: Date.now() });
  });

  afterEach(() => {
    clock.restore();
    sinon.restore();
  });

  describe('shutdown', () => {
    it('keeps releasing resources when notifying termination to one endpoint fails', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A, BROKER_B));
      await startClient(harness, clock);
      assert.strictEqual(harness.streams.length, 2);
      harness.rpc.notifyClientTermination.callsFake(async endpoints => {
        if (endpoints.facade === BROKER_A) {
          throw new Error('broker unreachable');
        }
        return new NotifyClientTerminationResponse().setStatus(buildStatus());
      });

      await harness.client.shutdown();

      sinon.assert.calledTwice(harness.rpc.notifyClientTermination);
      assert.ok(harness.streams.every(stream => stream.writableEnded), 'every telemetry stream must be released');
      sinon.assert.calledOnce(harness.rpc.close);
      sinon.assert.calledOnce(harness.meterShutdown);
      assert.strictEqual(harness.client.routeCache.size, 0);
      assert.strictEqual(harness.client.isRunning(), false);
      assert.strictEqual(clock.countTimers(), 0, 'no timer may survive shutdown');
      sinon.assert.calledWithMatch(harness.logger.warn, /Failed to notify remote/, sinon.match.any, BROKER_A);
    });

    it('can be called repeatedly', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));
      await startClient(harness, clock);

      await harness.client.shutdown();
      await harness.client.shutdown();

      sinon.assert.calledOnce(harness.rpc.notifyClientTermination);
      assert.strictEqual(harness.client.isRunning(), false);
      assert.strictEqual(clock.countTimers(), 0);
    });

    it('works on a client that never started', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      await harness.client.shutdown();
      sinon.assert.notCalled(harness.rpc.notifyClientTermination);
      sinon.assert.calledOnce(harness.rpc.close);
      assert.strictEqual(clock.countTimers(), 0);
    });
  });

  describe('startup failure', () => {
    it('leaves no timer or telemetry session after the route query keeps failing', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.rejects(new Error('route unavailable'));

      const assertion = assert.rejects(harness.client.startup(), /Startup the rocketmq client failed/);
      await clock.tickAsync(3000);
      await assertion;

      sinon.assert.calledThrice(harness.rpc.queryRoute);
      assert.strictEqual(harness.streams.length, 0);
      sinon.assert.calledOnce(harness.rpc.close);
      assert.strictEqual(clock.countTimers(), 0);
      assert.strictEqual(harness.client.isRunning(), false);
    });

    it('fails when one of several topics keeps failing even if the others are routable', async () => {
      const harness = createHarness({ topics: [ 'topic-a', 'topic-b' ] });
      harness.rpc.queryRoute.callsFake(async (_endpoints, request) => {
        if (request.getTopic()?.getName() === 'topic-b') {
          throw new Error('route unavailable for topic-b');
        }
        return routeResponse(BROKER_A);
      });

      const assertion = assert.rejects(harness.client.startup(), /Failed to fetch topic routes after 3 attempts/);
      await clock.tickAsync(3000);
      await assertion;

      assert.strictEqual(harness.client.isRunning(), false);
      assert.ok(harness.streams.every(stream => stream.writableEnded), 'telemetry of the routable topic must be released');
      assert.strictEqual(clock.countTimers(), 0);
    });

    it('succeeds when a failed route query recovers on retry', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.onFirstCall().rejects(new Error('route unavailable'));
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));

      const startup = harness.client.startup();
      await clock.tickAsync(1000);
      harness.client.onSettingsCommand(harness.client.getEndpoints(), buildSettings());
      await startup;

      sinon.assert.calledTwice(harness.rpc.queryRoute);
      assert.strictEqual(harness.client.isRunning(), true);
      await harness.client.shutdown();
    });

    it('releases timers and telemetry sessions when the first settings never arrive', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));
      harness.rpc.heartbeat.resolves(heartbeatResponse());

      const assertion = assert.rejects(harness.client.startup(), /Timed out waiting for the first settings/);
      await clock.tickAsync(30000);
      await assertion;

      assert.strictEqual(harness.streams.length, 1);
      assert.ok(harness.streams[0].writableEnded);
      assert.strictEqual(clock.countTimers(), 0);
    });

    it('rejects startup when the remote answers with an error status on telemetry', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));

      const assertion = assert.rejects(harness.client.startup(), /permission denied/);
      await clock.tickAsync(0);
      harness.streams[0].receive(new TelemetryCommand().setStatus(buildStatus(Code.FORBIDDEN, 'permission denied')));
      await assertion;
      assert.strictEqual(clock.countTimers(), 0);
    });
  });

  describe('heartbeat', () => {
    async function startedHarness() {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));
      await startClient(harness, clock);
      return harness;
    }

    it('sends nothing when no route endpoints are known', async () => {
      const harness = createHarness();
      await harness.client.heartbeat();
      sinon.assert.notCalled(harness.rpc.heartbeat);
    });

    it('resets the failure count after a successful heartbeat', async () => {
      const harness = await startedHarness();
      harness.rpc.heartbeat.onCall(0).resolves(heartbeatResponse(Code.INTERNAL_SERVER_ERROR));
      harness.rpc.heartbeat.onCall(1).resolves(heartbeatResponse());
      harness.rpc.heartbeat.onCall(2).resolves(heartbeatResponse(Code.INTERNAL_SERVER_ERROR));
      for (let i = 0; i < 3; i++) {
        await harness.client.heartbeat();
      }
      sinon.assert.notCalled(harness.rpc.evict);
      await harness.client.shutdown();
    });

    it('rebuilds the transport at the threshold and throttles recovery by the cooldown', async () => {
      const harness = await startedHarness();
      harness.rpc.heartbeat.rejects(new Error('deadline exceeded'));

      await harness.client.heartbeat();
      sinon.assert.notCalled(harness.rpc.evict);
      await harness.client.heartbeat();
      sinon.assert.calledOnce(harness.rpc.evict);
      assert.strictEqual(harness.streams.length, 2, 'telemetry session must be rebuilt');
      assert.ok(harness.streams[0].writableEnded, 'the stale telemetry stream must be released');
      assert.strictEqual(harness.streams[1].writes.length, 1, 'settings must be re-synced');

      await harness.client.heartbeat();
      sinon.assert.calledOnce(harness.rpc.evict);
      // Move the wall clock past the cooldown without firing the periodic tasks.
      clock.setSystemTime(Date.now() + 30000);
      await harness.client.heartbeat();
      sinon.assert.calledTwice(harness.rpc.evict);
      await harness.client.shutdown();
    });

    it('rejoins isolated endpoints after a successful heartbeat', async () => {
      const harness = await startedHarness();
      harness.client.isolatedEndpoints.set(BROKER_A, new Endpoints(BROKER_A));
      harness.rpc.heartbeat.resolves(heartbeatResponse());
      await harness.client.heartbeat();
      assert.strictEqual(harness.client.isolatedEndpoints.size, 0);
      await harness.client.shutdown();
    });
  });

  describe('route cache', () => {
    it('shares one in-flight query among concurrent callers of the same topic', async () => {
      const harness = createHarness();
      const pending = deferred<QueryRouteResponse>();
      harness.rpc.queryRoute.callsFake(() => pending.promise);

      const first = harness.client.route('topic-a');
      const second = harness.client.route('topic-a');
      pending.resolve(routeResponse(BROKER_A));

      assert.strictEqual(await first, await second);
      sinon.assert.calledOnce(harness.rpc.queryRoute);
      assert.strictEqual(await harness.client.route('topic-a'), await first, 'cached route must be reused');
      sinon.assert.calledOnce(harness.rpc.queryRoute);
    });

    it('does not cache a failed query', async () => {
      const harness = createHarness();
      harness.rpc.queryRoute.onFirstCall().rejects(new Error('route unavailable'));
      harness.rpc.queryRoute.onSecondCall().resolves(routeResponse(BROKER_A));

      await assert.rejects(harness.client.route('topic-a'), /route unavailable/);
      assert.strictEqual(harness.client.routeCache.size, 0);
      await harness.client.route('topic-a');
      sinon.assert.calledTwice(harness.rpc.queryRoute);
      assert.strictEqual(harness.client.routeCache.size, 1);
    });

    it('rejects an error status without caching it', async () => {
      const harness = createHarness();
      harness.rpc.queryRoute.resolves(new QueryRouteResponse().setStatus(buildStatus(Code.TOPIC_NOT_FOUND, 'no topic')));
      await assert.rejects(harness.client.route('topic-a'), /no topic/);
      assert.strictEqual(harness.client.routeCache.size, 0);
      assert.strictEqual(harness.client.routeUpdates.length, 0);
    });

    it('opens telemetry only for new endpoints and reports every route update', async () => {
      const harness = createHarness();
      harness.rpc.queryRoute.onFirstCall().resolves(routeResponse(BROKER_A));
      harness.rpc.queryRoute.onSecondCall().resolves(routeResponse(BROKER_A, BROKER_B));

      await harness.client.route('topic-a');
      await harness.client.route('topic-b');

      assert.deepStrictEqual(harness.client.routeUpdates.map(([ topic ]) => topic), [ 'topic-a', 'topic-b' ]);
      assert.strictEqual(harness.client.routeUpdates[1][1].getTotalEndpoints().length, 2);
      assert.strictEqual(harness.streams.length, 2, 'only the new endpoints get a telemetry session');
      assert.ok(harness.streams.every(stream => stream.writes.length === 1), 'settings must be synced once per session');
    });
  });

  describe('telemetry command dispatch', () => {
    async function sessionHarness() {
      const harness = createHarness();
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));
      await harness.client.route('topic-a');
      const stream = harness.streams[0];
      stream.writes.length = 0;
      return { ...harness, stream };
    }

    it('applies a settings command', async () => {
      const { client, stream } = await sessionHarness();
      const onSettings = sinon.spy(client, 'onSettingsCommand');
      const settings = buildSettings();
      stream.receive(new TelemetryCommand().setSettings(settings));
      sinon.assert.calledOnce(onSettings);
      assert.strictEqual(onSettings.firstCall.args[1].toObject().clientType, settings.getClientType());
    });

    it('ignores an orphaned transaction recovery command on the base client', async () => {
      const { stream, logger } = await sessionHarness();
      stream.receive(new TelemetryCommand().setRecoverOrphanedTransactionCommand(new RecoverOrphanedTransactionCommand()));
      sinon.assert.calledWithMatch(logger.warn, /Ignore orphaned transaction recovery command/);
      assert.deepStrictEqual(stream.writes, []);
    });

    it('answers a verify message command with NOT_IMPLEMENTED and the same nonce', async () => {
      const { stream } = await sessionHarness();
      stream.receive(new TelemetryCommand().setVerifyMessageCommand(new VerifyMessageCommand().setNonce('nonce-1')));
      assert.strictEqual(stream.writes.length, 1);
      assert.strictEqual(stream.writes[0].getStatus()?.getCode(), Code.NOT_IMPLEMENTED);
      assert.strictEqual(stream.writes[0].getVerifyMessageResult()?.getNonce(), 'nonce-1');
    });

    it('answers a print thread stack trace command with process diagnostics', async () => {
      const { stream } = await sessionHarness();
      stream.receive(new TelemetryCommand()
        .setPrintThreadStackTraceCommand(new PrintThreadStackTraceCommand().setNonce('nonce-2')));
      assert.strictEqual(stream.writes.length, 1);
      assert.strictEqual(stream.writes[0].getStatus()?.getCode(), Code.OK);
      assert.strictEqual(stream.writes[0].getThreadStackTrace()?.getNonce(), 'nonce-2');
      assert.match(stream.writes[0].getThreadStackTrace()?.getThreadStackTrace() ?? '', /^Process: pid=/);
    });

    it('only logs an unknown command carrying an OK status', async () => {
      const { client, stream, logger } = await sessionHarness();
      const onUnknown = sinon.spy(client, 'onUnknownCommand');
      stream.receive(new TelemetryCommand().setStatus(buildStatus()));
      sinon.assert.calledOnce(onUnknown);
      sinon.assert.calledWithMatch(logger.warn, /unrecognized command/);
      sinon.assert.neverCalledWithMatch(logger.error, /error status/);
    });
  });

  describe('message interceptor', () => {
    it('warns and ignores an interceptor added while running', async () => {
      const harness = createHarness({ topics: [ 'topic-a' ] });
      harness.rpc.queryRoute.resolves(routeResponse(BROKER_A));
      await startClient(harness, clock);
      const interceptor: MessageInterceptor = { doBefore: sinon.spy(), doAfter: sinon.spy() };

      harness.client.addMessageInterceptor(interceptor);
      harness.client.doBefore(new MessageInterceptorContextImpl(MessageHookPoints.SEND), []);

      sinon.assert.notCalled(interceptor.doBefore as sinon.SinonSpy);
      sinon.assert.calledWithMatch(harness.logger.warn, /Message interceptor can only be added before/);
      await harness.client.shutdown();
    });

    it('accepts an interceptor added before startup', () => {
      const harness = createHarness();
      const interceptor: MessageInterceptor = { doBefore: sinon.spy(), doAfter: sinon.spy() };
      harness.client.addMessageInterceptor(interceptor);
      harness.client.doBefore(new MessageInterceptorContextImpl(MessageHookPoints.SEND), []);
      sinon.assert.calledOnce(interceptor.doBefore as sinon.SinonSpy);
    });
  });
});

describe('ClientId', () => {
  it('follows the Java layout and increases within the process', () => {
    const first = ClientId.create();
    const second = ClientId.create();
    const pattern = /^(.+)@(\d+)@(\d+)@([0-9a-z]+)$/;
    const current = pattern.exec(first)!;
    const next = pattern.exec(second)!;
    assert.deepStrictEqual(current.slice(1, 3), [ hostname(), String(process.pid) ]);
    assert.strictEqual(Number(next[3]), Number(current[3]) + 1);
    assert.notStrictEqual(first, second);
  });
});
