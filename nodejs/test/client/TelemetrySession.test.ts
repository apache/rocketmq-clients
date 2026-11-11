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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { TelemetrySession } from '../../src/client/TelemetrySession';
import { TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../../src/route';
import { ILogger } from '../../src/client/Logger';

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
});
