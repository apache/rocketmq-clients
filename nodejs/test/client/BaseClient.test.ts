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
 *    must complete startup (no 5-minute hang), and
 *  - a status-only telemetry error arriving in the same window must reject
 *    startup() without being flagged as an unhandled rejection (which would
 *    terminate the process under Node.js's default behavior).
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { format } from 'node:util';
import {
  AddressScheme,
  Broker as BrokerPB,
  ClientType,
  Code,
  Endpoints as EndpointsPB,
  MessageQueue as MessageQueuePB,
  Permission,
  Resource,
  Settings as SettingsPB,
  Status,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  HeartbeatRequest,
  HeartbeatResponse,
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

  /** Test hook: route query returning one real message queue, so the route
   * cache yields a heartbeat target endpoint. */
  mockQueryRouteWithQueue(delayMs: number) {
    const self = this as unknown as { rpcClientManager: Record<string, unknown> };
    self.rpcClientManager.queryRoute = async () => {
      await sleep(delayMs);
      const endpointsPb = new EndpointsPB();
      endpointsPb.setScheme(AddressScheme.IPV4);
      endpointsPb.addAddresses().setHost('127.0.0.1').setPort(8081);
      const brokerPb = new BrokerPB();
      brokerPb.setName('broker-a');
      brokerPb.setId(0);
      brokerPb.setEndpoints(endpointsPb);
      const mqPb = new MessageQueuePB();
      mqPb.setId(0);
      mqPb.setTopic(new Resource().setName('topic-A'));
      mqPb.setBroker(brokerPb);
      mqPb.setPermission(Permission.READ_WRITE);
      const response = new QueryRouteResponse();
      response.setStatus(new Status().setCode(Code.OK));
      response.addMessageQueues(mqPb);
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

describe('BaseClient heartbeat response status check', () => {
  function makeFakeStream() {
    const stream: any = new EventEmitter();
    stream.writes = [] as unknown[];
    stream.write = (command: unknown) => {
      stream.writes.push(command);
      return true;
    };
    stream.end = () => undefined;
    return stream;
  }

  it('treats an error status in the heartbeat response as a failure and rebuilds transport', async () => {
    const warnings: string[] = [];
    const logger: any = {
      info: () => undefined,
      warn: (...args: unknown[]) => { warnings.push(format(...args as [ string ])); },
      error: () => undefined,
      debug: () => undefined,
    };
    const client = new TestClient({
      endpoints: '127.0.0.1:8081',
      namespace: 'test-namespace',
      topics: [ 'topic-A' ],
      logger,
    });

    // Replace the real RPC manager (closing it first to stop its idle-check
    // timer) with a fake whose heartbeat returns UNRECOGNIZED_CLIENT_TYPE —
    // the signal a proxy sends when it no longer holds the client's settings.
    (client as any).rpcClientManager.close();
    const streams: any[] = [];
    let evictCount = 0;
    (client as any).rpcClientManager = {
      queryRoute: async () => {
        throw new Error('use mockQueryRouteWithQueue');
      },
      heartbeat: async () => new HeartbeatResponse()
        .setStatus(new Status().setCode(Code.UNRECOGNIZED_CLIENT_TYPE).setMessage('client type unrecognized')),
      telemetry: () => {
        const stream = makeFakeStream();
        streams.push(stream);
        return stream;
      },
      evict: () => { evictCount++; },
      notifyClientTermination: async () => ({}),
      close: () => undefined,
    };
    client.mockQueryRouteWithQueue(10);

    const startupPromise = client.startup();
    await sleep(20);
    client.onSettingsCommand(client.getEndpoints(), new SettingsPB());
    await startupPromise;
    // The route fetch opened one telemetry session and synced settings to it.
    assert.strictEqual(streams.length, 1);

    // Heartbeat round 1: the error status must be treated as a failure
    // (pre-fix the response was discarded and this counted as a success).
    await (client as any).doHeartbeat();
    assert.ok(warnings.some(w => w.includes('Heartbeat failed') && w.includes('client type unrecognized')),
      'an error status in the heartbeat response must be logged as a failure');
    assert.strictEqual(evictCount, 0, 'first failure is below the recovery threshold');

    // Heartbeat round 2: consecutive failures reach the threshold and trigger
    // transport recovery — evict the channel and rebuild telemetry so the
    // settings get re-registered.
    await (client as any).doHeartbeat();
    assert.strictEqual(evictCount, 1, 'transport recovery must evict the stale rpc client');
    assert.strictEqual(streams.length, 2, 'telemetry session must be rebuilt');
    assert.ok(streams[1].writes.length >= 1, 'settings must be re-synced on the rebuilt stream');

    await client.shutdown();
  });
});
