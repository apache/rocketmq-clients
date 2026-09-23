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
import {
  Server,
  ServerCredentials,
  ServerDuplexStream,
  ServerUnaryCall,
  sendUnaryData,
  Metadata,
} from '@grpc/grpc-js';
import { MessagingServiceService } from '../../proto/apache/rocketmq/v2/service_grpc_pb';
import { TelemetryCommand, HeartbeatRequest, HeartbeatResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { Code, Status } from '../../proto/apache/rocketmq/v2/definition_pb';
import { RpcClient } from '../../src/client/RpcClient';
import { Endpoints } from '../../src/route';

async function waitForCount(getter: () => number, target: number, timeout = 5000) {
  const start = Date.now();
  while (getter() < target) {
    if (Date.now() - start > timeout) {
      throw new Error(`timeout waiting for count ${target}, current ${getter()}`);
    }
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

describe('test/client/RpcClient.test.ts', () => {
  let server: Server;
  let port: number;
  const telemetryPeers: string[] = [];
  const heartbeatPeers: string[] = [];
  const openStreams: ServerDuplexStream<TelemetryCommand, TelemetryCommand>[] = [];

  before(async () => {
    server = new Server();
    server.addService(MessagingServiceService, {
      telemetry(call: ServerDuplexStream<TelemetryCommand, TelemetryCommand>) {
        telemetryPeers.push(call.getPeer());
        openStreams.push(call);
        // Keep the stream alive; just drain inbound data.
        call.on('data', () => { /* noop */ });
        call.on('error', () => { /* noop */ });
        call.on('end', () => {
          try {
            call.end();
          } catch {
            // ignore
          }
        });
      },
      heartbeat(call: ServerUnaryCall<HeartbeatRequest, HeartbeatResponse>,
        callback: sendUnaryData<HeartbeatResponse>) {
        heartbeatPeers.push(call.getPeer());
        const res = new HeartbeatResponse();
        const status = new Status();
        status.setCode(Code.OK);
        res.setStatus(status);
        callback(null, res);
      },
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);
    port = await new Promise<number>((resolve, reject) => {
      server.bindAsync('127.0.0.1:0', ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) return reject(err);
        resolve(boundPort);
      });
    });
  });

  after(async () => {
    for (const s of openStreams) {
      try {
        s.end();
      } catch {
        // ignore
      }
    }
    await new Promise<void>(resolve => server.tryShutdown(() => resolve()));
  });

  it('should isolate gRPC connections between client instances (issue #1382)', async () => {
    const base = telemetryPeers.length;
    const endpoints = new Endpoints(`127.0.0.1:${port}`);
    const client1 = new RpcClient(endpoints, false);
    const client2 = new RpcClient(endpoints, false);

    const stream1 = client1.telemetry(new Metadata());
    stream1.on('error', () => { /* noop */ });
    stream1.write(new TelemetryCommand());
    await waitForCount(() => telemetryPeers.length, base + 1);

    // Keep client1's stream alive while opening client2's stream.
    const stream2 = client2.telemetry(new Metadata());
    stream2.on('error', () => { /* noop */ });
    stream2.write(new TelemetryCommand());
    await waitForCount(() => telemetryPeers.length, base + 2);

    const peer1 = telemetryPeers[base];
    const peer2 = telemetryPeers[base + 1];

    try {
      assert.notEqual(peer1, peer2,
        `two independent clients must not share a connection, but both used ${peer1}`);
    } finally {
      stream1.destroy();
      stream2.destroy();
      client1.close();
      client2.close();
    }
  });

  it('should reuse one connection for telemetry and heartbeat within a single client', async () => {
    const tBase = telemetryPeers.length;
    const hBase = heartbeatPeers.length;
    const endpoints = new Endpoints(`127.0.0.1:${port}`);
    const client = new RpcClient(endpoints, false);

    const stream = client.telemetry(new Metadata());
    stream.on('error', () => { /* noop */ });
    stream.write(new TelemetryCommand());
    await waitForCount(() => telemetryPeers.length, tBase + 1);

    await client.heartbeat(new HeartbeatRequest(), new Metadata(), 5000);
    await waitForCount(() => heartbeatPeers.length, hBase + 1);

    try {
      assert.equal(telemetryPeers[tBase], heartbeatPeers[hBase],
        'telemetry and heartbeat within one client must reuse the same connection');
    } finally {
      stream.destroy();
      client.close();
    }
  });
});
