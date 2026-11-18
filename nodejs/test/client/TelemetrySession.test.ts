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
 * Regression tests for the telemetry reconnect loop.
 *
 * The reconnect timer callback used to call #renewStream(false) unguarded;
 * since #renewStream rethrows stream-creation failures, a single transient
 * failure escaped the setTimeout callback as an uncaughtException and
 * permanently ended the reconnect loop — the session could never re-register
 * settings afterwards. The callback now catches failures and reschedules with
 * exponential backoff until release().
 */

import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import * as sinon from 'sinon';
import { Metadata, Server, ServerCredentials, ServerDuplexStream, status as GrpcStatus } from '@grpc/grpc-js';
import { TelemetrySession } from '../../src/client/TelemetrySession';
import { RpcClient } from '../../src/client/RpcClient';
import { MessagingServiceService } from '../../proto/apache/rocketmq/v2/service_grpc_pb';
import { TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../../src/route';
import { ILogger } from '../../src/client/Logger';
import { createSilentLogger, FakeTelemetryStream } from '../support/fakes';
import { buildSettings } from '../support/protobuf';

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

function makeFakeLogger(): ILogger & { errors: string[] } {
  const errors: string[] = [];
  return {
    errors,
    info: () => undefined,
    warn: () => undefined,
    error: (msg: string) => { errors.push(msg); },
    debug: () => undefined,
  } as never;
}

interface HarnessOptions {
  // Stream creation fails this many times before succeeding
  failures: number;
}

function makeHarness(options: HarnessOptions) {
  const streams: any[] = [];
  let remainingFailures = options.failures;
  let calls = 0;
  const settingsCommand = new TelemetryCommand();
  const baseClient: any = {
    clientId: 'test-client',
    sslEnabled: false,
    settingsCommand: () => settingsCommand,
    createTelemetryStream: () => {
      calls++;
      // The very first call (constructor) always succeeds; failures are
      // injected into subsequent reconnect attempts only.
      if (calls > 1 && remainingFailures > 0) {
        remainingFailures--;
        throw new Error('transient stream creation failure');
      }
      const stream = makeFakeStream();
      streams.push(stream);
      return stream;
    },
  };
  return { baseClient, streams, settingsCommand };
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

describe('TelemetrySession reconnect', () => {
  it('keeps retrying with backoff after transient stream-creation failures and re-syncs settings', async () => {
    const harness = makeHarness({ failures: 1 });
    const session = new TelemetrySession(harness.baseClient, new Endpoints('127.0.0.1:8081'), makeFakeLogger());
    assert.strictEqual(harness.streams.length, 1, 'initial stream should be created');

    // Stream error triggers the reconnect loop; the next creation attempt
    // fails transiently and must be retried instead of ending the loop.
    harness.streams[0].emit('error', new Error('stream broken'));

    // Backoff: first retry after ~1s fails, second retry after ~2s succeeds.
    await sleep(3600);

    assert.strictEqual(harness.streams.length, 2,
      'reconnect loop must survive the transient failure and establish a new stream');
    assert.ok(harness.streams[1].writes.includes(harness.settingsCommand),
      'settings must be re-synced on the new stream');
    session.release();
  });

  it('release() cancels a pending reconnect', async () => {
    const harness = makeHarness({ failures: 0 });
    const session = new TelemetrySession(harness.baseClient, new Endpoints('127.0.0.1:8081'), makeFakeLogger());
    harness.streams[0].emit('error', new Error('stream broken'));
    session.release();

    await sleep(1300);
    assert.strictEqual(harness.streams.length, 1, 'no reconnect may happen after release');
  });

  it('refresh() schedules a retry when the renewal fails', async () => {
    const harness = makeHarness({ failures: 1 });
    const session = new TelemetrySession(harness.baseClient, new Endpoints('127.0.0.1:8081'), makeFakeLogger());

    // refresh closes the current stream and renews; the renewal fails once
    // and must be retried in the background instead of leaving the session dead.
    session.refresh();
    await sleep(1300);

    assert.strictEqual(harness.streams.length, 2,
      'refresh failure must be followed by a background retry');
    assert.ok(harness.streams[1].writes.includes(harness.settingsCommand),
      'settings must be synced on the refreshed stream');
    session.release();
  });

  it('keeps the reconnect backoff at 1s while the stream keeps delivering data before failing', async () => {
    const clock = sinon.useFakeTimers();
    try {
      const streams: FakeTelemetryStream[] = [];
      const baseClient: any = {
        clientId: 'test-client',
        settingsCommand: () => new TelemetryCommand(),
        onSettingsCommand: () => undefined,
        createTelemetryStream: () => {
          const stream = new FakeTelemetryStream();
          streams.push(stream);
          return stream.asGrpcStream();
        },
      };
      const session = new TelemetrySession(baseClient, new Endpoints('127.0.0.1:8081'), createSilentLogger());
      for (let round = 1; round <= 3; round++) {
        streams[round - 1].receive(new TelemetryCommand().setSettings(buildSettings()));
        streams[round - 1].fail(new Error('stream broken'));
        await clock.tickAsync(999);
        assert.strictEqual(streams.length, round, 'reconnect must not happen before the 1s backoff');
        await clock.tickAsync(1);
        assert.strictEqual(streams.length, round + 1, 'reconnect must happen after the 1s backoff');
      }
      session.release();
    } finally {
      clock.restore();
    }
  });
});

describe('TelemetrySession released stream', () => {
  function makeClient(streams: FakeTelemetryStream[], overrides: Record<string, unknown> = {}) {
    return {
      clientId: 'test-client',
      settingsCommand: () => new TelemetryCommand(),
      createTelemetryStream: () => {
        const stream = new FakeTelemetryStream();
        streams.push(stream);
        return stream.asGrpcStream();
      },
      ...overrides,
    } as any;
  }

  it('drops writes after release instead of raising an unhandled stream error', async () => {
    const streams: FakeTelemetryStream[] = [];
    const logger = createSilentLogger();
    const session = new TelemetrySession(makeClient(streams), new Endpoints('127.0.0.1:8081'), logger);
    session.release();
    session.syncSettings();
    session.write(new TelemetryCommand());
    await sleep(20);
    assert.strictEqual(streams.length, 1, 'a released session must not reopen a stream');
    assert.deepStrictEqual(streams[0].writes, [], 'nothing may reach the ended stream');
    sinon.assert.called(logger.warn);
  });

  it('drops writes during the reconnect backoff instead of writing to the ended stream', async () => {
    const clock = sinon.useFakeTimers();
    try {
      const streams: FakeTelemetryStream[] = [];
      const session = new TelemetrySession(makeClient(streams), new Endpoints('127.0.0.1:8081'), createSilentLogger());
      streams[0].fail(new Error('stream broken'));
      session.write(new TelemetryCommand());
      await clock.tickAsync(0);
      assert.deepStrictEqual(streams[0].writes, [], 'the ended stream must not receive writes');
      await clock.tickAsync(1000);
      assert.strictEqual(streams.length, 2, 'reconnect must still happen after the dropped write');
      assert.strictEqual(streams[1].writes.length, 1, 'settings must be synced on the renewed stream');
      session.release();
    } finally {
      clock.restore();
    }
  });

  it('logs instead of throwing when a command handler fails', () => {
    const streams: FakeTelemetryStream[] = [];
    const logger = createSilentLogger();
    const client = makeClient(streams, {
      onSettingsCommand: () => {
        throw new Error('handler failure');
      },
    });
    const session = new TelemetrySession(client, new Endpoints('127.0.0.1:8081'), logger);
    assert.doesNotThrow(() => streams[0].receive(new TelemetryCommand().setSettings(buildSettings())));
    sinon.assert.calledWithMatch(logger.error, /\[Bug\]/);
    session.release();
  });

  describe('with a real gRPC stream', () => {
    let server: Server;
    let port: number;

    before(async () => {
      server = new Server();
      server.addService(MessagingServiceService, {
        telemetry(call: ServerDuplexStream<TelemetryCommand, TelemetryCommand>) {
          call.on('data', () => undefined);
          // Answer the client half-close with a non-OK status, as a remote
          // that is going away would.
          call.on('end', () => call.emit('error', { code: GrpcStatus.UNAVAILABLE, details: 'remote is going away' }));
        },
      } as any);
      port = await new Promise<number>((resolve, reject) => {
        server.bindAsync('127.0.0.1:0', ServerCredentials.createInsecure(), (err, boundPort) => {
          if (err) return reject(err);
          resolve(boundPort);
        });
      });
    });

    after(() => {
      server.forceShutdown();
    });

    it('swallows the remote error status that arrives after release', async () => {
      const endpoints = new Endpoints(`127.0.0.1:${port}`);
      const rpcClient = new RpcClient(endpoints, false);
      const uncaught: unknown[] = [];
      const listeners = process.listeners('uncaughtException');
      process.removeAllListeners('uncaughtException');
      process.on('uncaughtException', err => uncaught.push(err));
      try {
        const client = makeClient([], { createTelemetryStream: () => rpcClient.telemetry(new Metadata()) });
        const session = new TelemetrySession(client, endpoints, createSilentLogger());
        session.syncSettings();
        await sleep(100);
        session.release();
        await sleep(200);
      } finally {
        process.removeAllListeners('uncaughtException');
        for (const listener of listeners) {
          process.on('uncaughtException', listener);
        }
        rpcClient.close();
      }
      assert.deepStrictEqual(uncaught, [], 'a status error after release must not become an uncaught exception');
    });
  });
});
