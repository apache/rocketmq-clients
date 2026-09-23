import * as assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { TelemetrySession } from '../src/client/TelemetrySession';

class FakeStream extends EventEmitter {
  ended = false;
  end() { this.ended = true; }
  write() {}
}

function makeSession() {
  const streams: FakeStream[] = [];
  const baseClient: any = {
    clientId: 'test-client',
    settingsCommand: () => ({} as any),
    createTelemetryStream: () => {
      const s = new FakeStream();
      streams.push(s);
      return s;
    },
  };
  const logger: any = { info() {}, warn() {}, error() {}, debug() {} };
  const session = new TelemetrySession(baseClient, { toString: () => 'fake:8081' } as any, logger);
  return { session, streams };
}

const wait = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('telemetry session refresh', () => {
  it('keeps auto-reconnect alive after a refresh', async () => {
    const { session, streams } = makeSession();
    assert.equal(streams.length, 1);

    session.refresh();
    assert.equal(streams.length, 2, 'refresh should open a new stream');
    assert.equal(streams[0].ended, true, 'refresh should close the old stream');

    streams[1].emit('error', new Error('boom'));
    await wait(1300);
    assert.equal(streams.length, 3, 'auto-reconnect must still work after refresh');
    session.release();
  });
});
