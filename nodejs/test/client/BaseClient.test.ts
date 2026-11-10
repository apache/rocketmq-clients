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
 * Regression tests for the startup first-settings promise:
 *  - a server-pushed Settings arriving while route updates are still pending
 *    must complete startup (no 5-minute hang),
 *  - a status-only telemetry error arriving in the same window must reject
 *    startup() without being flagged as an unhandled rejection (which would
 *    terminate the process under Node.js's default behavior), and
 *  - the wait for the first Settings is bounded: a silent remote must make
 *    startup() reject with a clear error and release every resource acquired
 *    by the failed startup (mirrors Java PushConsumerImpl#startUp which calls
 *    shutDown() before rethrowing), instead of hanging forever and leaking
 *    the route/settings/heartbeat timers.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import {
  ClientType,
  Code,
  Settings as SettingsPB,
  Status,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatRequest,
  NotifyClientTerminationRequest,
  QueryRouteResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { BaseClient } from '../../src/client/BaseClient';
import { Settings } from '../../src/client/Settings';
import { Endpoints } from '../../src/route';

const ACCESS_POINT = new Endpoints('127.0.0.1:8081');

class StubSettings extends Settings {
  constructor() {
    super('test-namespace', 'test-client-id', ClientType.PRODUCER, ACCESS_POINT, 3000);
  }

  toProtobuf(): SettingsPB {
    return new SettingsPB();
  }

  sync(settings: SettingsPB): void {
    void settings; // no-op: the startup race only depends on the callback firing
  }
}

class TestClient extends BaseClient {
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

  /** Test hook: swap the route query for a delayed OK response. */
  mockQueryRoute(delayMs: number) {
    const self = this as unknown as { rpcClientManager: Record<string, unknown> };
    self.rpcClientManager.queryRoute = async () => {
      await sleep(delayMs);
      const response = new QueryRouteResponse();
      response.setStatus(new Status().setCode(Code.OK));
      return response;
    };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('BaseClient startup first-settings promise', () => {
  it('completes startup when the first settings command arrives before route updates finish', async () => {
    const client = new TestClient({
      endpoints: '127.0.0.1:8081',
      namespace: 'test-namespace',
      topics: [ 'topic-A', 'topic-B' ],
    });
    client.mockQueryRoute(80);

    const startupPromise = client.startup();
    // Route updates for the two topics are pending now; the server-pushed
    // settings callback fires inside this window. Pre-fix, the resolvers were
    // registered only after updateRoutes() finished, so this callback was
    // silently dropped and startup hung until the 5-minute periodic sync.
    await sleep(20);
    client.onSettingsCommand(client.getEndpoints(), new SettingsPB());
    await startupPromise;
    assert.strictEqual(client.isRunning(), true);
  });

  it('propagates a telemetry error received before route updates complete without an unhandled rejection', async () => {
    const client = new TestClient({
      endpoints: '127.0.0.1:8081',
      namespace: 'test-namespace',
      topics: [ 'topic-A', 'topic-B' ],
    });
    client.mockQueryRoute(80);

    // Record unhandled rejections instead of letting them kill the test
    // process. On stock Node.js (no fix) this scenario rejects the
    // first-settings promise with no handler attached, which terminates the
    // process by default; the listener both keeps the test alive and captures
    // the evidence for the assertion below.
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      const startupPromise = client.startup();
      // A status-only telemetry command arrives while the first route query
      // is still pending: onUnknownCommand rejects the first-settings promise
      // before the code in #startup() attaches a rejection handler to it.
      await sleep(20);
      client.onUnknownCommand(client.getEndpoints(), {
        code: Code.BAD_REQUEST,
        message: 'mock telemetry failure',
        requestId: '',
      } as unknown as Status.AsObject);

      // startup() must surface the telemetry error to the caller...
      await assert.rejects(startupPromise, /mock telemetry failure/);
      // ...and the rejection must never have been flagged as unhandled.
      await sleep(20);
      assert.deepStrictEqual(unhandled, []);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });
});

describe('BaseClient startup first-settings timeout', () => {
  function makeFakeRpcManager(state: { closeCount: number }) {
    return {
      queryRoute: async () => {
        throw new Error('replaced by mockQueryRoute');
      },
      notifyClientTermination: async () => ({}),
      telemetry: () => {
        const stream: any = new EventEmitter();
        stream.write = () => true;
        stream.end = () => undefined;
        return stream;
      },
      close: () => { state.closeCount++; },
    };
  }

  it('rejects startup and releases resources when the first settings never arrive', async () => {
    const state = { closeCount: 0 };
    const client = new TestClient({
      endpoints: '127.0.0.1:8081',
      namespace: 'test-namespace',
      topics: [ 'topic-A' ],
    });
    // Internal-only knob (not part of the public options): shorten the
    // first-settings wait so the test stays fast.
    (client as any).firstSettingsTimeoutMs = 200;
    // Stop the real RPC manager (idle-check timer) and swap in the fake.
    (client as any).rpcClientManager.close();
    (client as any).rpcClientManager = makeFakeRpcManager(state);
    client.mockQueryRoute(10);

    // The remote never pushes Settings: startup must reject within the
    // timeout instead of hanging forever, and the failure path must run the
    // full shutdown cleanup (closeCount proves it).
    await assert.rejects(client.startup(), /Timed out waiting for the first settings/);
    assert.strictEqual(client.isRunning(), false);
    assert.ok(state.closeCount >= 1,
      'failed startup must release the resources it acquired (rpc manager closed)');
  });

  it('completes startup when the first settings arrive within the timeout', async () => {
    const state = { closeCount: 0 };
    const client = new TestClient({
      endpoints: '127.0.0.1:8081',
      namespace: 'test-namespace',
      topics: [ 'topic-A' ],
    });
    (client as any).firstSettingsTimeoutMs = 5000;
    (client as any).rpcClientManager.close();
    (client as any).rpcClientManager = makeFakeRpcManager(state);
    client.mockQueryRoute(10);

    const startupPromise = client.startup();
    await sleep(30);
    client.onSettingsCommand(client.getEndpoints(), new SettingsPB());
    await startupPromise;
    assert.strictEqual(client.isRunning(), true);
    assert.strictEqual(state.closeCount, 0, 'successful startup must not trigger the cleanup path');
    await client.shutdown();
  });
});
