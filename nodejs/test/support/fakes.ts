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

import { Duplex } from 'node:stream';
import type { ClientDuplexStream } from '@grpc/grpc-js';
import * as sinon from 'sinon';
import type { BaseClient } from '../../src/client/BaseClient';
import type { ILogger } from '../../src/client/Logger';
import { RpcClientManager } from '../../src/client/RpcClientManager';
import { TelemetryCommand } from '../../proto/apache/rocketmq/v2/service_pb';

export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export function createSilentLogger() {
  return {
    info: sinon.stub<unknown[], void>(),
    warn: sinon.stub<unknown[], void>(),
    error: sinon.stub<unknown[], void>(),
    debug: sinon.stub<unknown[], void>(),
    close: sinon.stub<unknown[], void>(),
  } satisfies ILogger;
}

// 不运行构造函数，避免真实 channel 和空闲回收定时器；应答通过 resolves/rejects/callsFake 设置。
export function createFakeRpcClientManager(): sinon.SinonStubbedInstance<RpcClientManager> {
  const fake = sinon.createStubInstance(RpcClientManager);
  const asyncMethods = [
    'queryRoute', 'heartbeat', 'sendMessage', 'queryAssignment', 'receiveMessage',
    'ackMessage', 'forwardMessageToDeadLetterQueue', 'pullMessage', 'updateOffset',
    'getOffset', 'queryOffset', 'endTransaction', 'changeInvisibleDuration',
    'notifyClientTermination', 'recallMessage', 'syncLiteSubscription',
  ] as const;
  for (const method of asyncMethods) {
    fake[method].rejects(new Error(`未配置 FakeRpcClientManager.${method}`));
  }
  fake.telemetry.throws(new Error('未配置 FakeRpcClientManager.telemetry'));
  return fake;
}

// 仅注入尚未 startup 的测试客户端；先关闭构造时创建的 manager，避免遗留真实定时器。
export function installFakeRpcClientManager(client: BaseClient, fake = createFakeRpcClientManager()) {
  if (client.isRunning()) {
    throw new Error('只能向尚未启动的客户端注入 RPC 替身');
  }
  client.getRpcClientManager().close();
  Object.defineProperty(client, 'rpcClientManager', { value: fake, configurable: true, writable: true });
  return fake;
}

export class FakeTelemetryStream extends Duplex {
  readonly writes: TelemetryCommand[] = [];

  constructor() {
    super({ objectMode: true });
  }

  _read(): void {
    // 服务端响应由测试显式发送。
  }

  _write(command: TelemetryCommand, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.writes.push(command);
    callback();
  }

  receive(command: TelemetryCommand): void {
    this.emit('data', command);
  }

  fail(error: Error): void {
    this.emit('error', error);
  }

  finishFromServer(): void {
    this.emit('end');
  }

  cancel(): void {
    this.destroy();
  }

  // 只模拟 telemetry 使用的流接口，不提供真实 gRPC metadata/status。
  asGrpcStream(): ClientDuplexStream<TelemetryCommand, TelemetryCommand> {
    return this as unknown as ClientDuplexStream<TelemetryCommand, TelemetryCommand>;
  }
}
