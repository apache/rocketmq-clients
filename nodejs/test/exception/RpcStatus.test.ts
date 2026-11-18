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
import { Metadata } from '@grpc/grpc-js';
import * as sinon from 'sinon';
import { Code, TransactionResolution } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageResponse, ChangeInvisibleDurationResponse, EndTransactionResponse, HeartbeatResponse,
  QueryRouteResponse, QueryAssignmentResponse, ReceiveMessageRequest, ReceiveMessageResponse,
  SendMessageRequest, SendMessageResponse, TelemetryCommand,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { BaseClient } from '../../src/client/BaseClient';
import { RpcClient } from '../../src/client/RpcClient';
import { TelemetrySession } from '../../src/client/TelemetrySession';
import { SimpleConsumer } from '../../src/consumer/SimpleConsumer';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import { ClientException, InternalErrorException, NotFoundException } from '../../src/exception';
import { Message, MessageView } from '../../src/message';
import { Endpoints, MessageQueue } from '../../src/route';
import { Producer } from '../../src/producer/Producer';
import { SendReceipt } from '../../src/producer/SendReceipt';
import { getRequestId } from '../../src/util/rpcContext';
import { buildMessagePB, buildRoute, buildStatus } from '../support/protobuf';
import { createSilentLogger, deferred, FakeTelemetryStream } from '../support/fakes';

const endpoints = new Endpoints('127.0.0.1:8081');
const queue = buildRoute().messageQueues[0];
const message = new MessageView(buildMessagePB(), queue);

class TestConsumer extends SimpleConsumer {
  receiveForTest(mq: MessageQueue = queue) { return this.receiveMessage(new ReceiveMessageRequest(), mq, 1000); }
  ackForTest() { return this.ackMessage(message); }
  invisibleForTest() { return this.invisibleDuration(message, 10000); }
  routeForTest() { return this.getRouteData('test-topic'); }
  cacheRouteForTest() { this.topicRouteCache.set('test-topic', buildRoute()); }
}

class TestPushConsumer extends PushConsumer {
  cacheRouteForTest() { this.topicRouteCache.set('test-topic', buildRoute()); }
  scanForTest() { this.startAssignmentScanning(); }
}

function matchesRequest(error: unknown, requestId: string, code = Code.UNAUTHORIZED) {
  assert(error instanceof ClientException);
  assert.equal(error.code, code);
  assert.equal(error.message, `[request-id=${requestId}, response-code=${code}] server message`);
  return true;
}

describe('RPC 异常 requestId 与 receive 限定', () => {
  let consumer: TestConsumer;
  let clients: BaseClient[];
  let clock: sinon.SinonFakeTimers;
  let logger: ReturnType<typeof createSilentLogger>;
  beforeEach(() => {
    clock = sinon.useFakeTimers({ toFake: [ 'setInterval', 'clearInterval', 'setTimeout', 'clearTimeout' ] });
    logger = createSilentLogger();
    consumer = new TestConsumer({ endpoints: endpoints.facade, namespace: '', consumerGroup: 'test-group',
      subscriptions: new Map([[ 'test-topic', '*' ]]), logger });
    clients = [ consumer ];
  });
  afterEach(() => {
    // 未启动客户端，meter 保持 disabled；关闭唯一创建的 RPC channel 与空闲定时器。
    for (const client of clients) client.getRpcClientManager().close();
    sinon.restore();
    clock.restore();
  });

  [ 'ack', 'invisible', 'route' ].forEach(operation => {
    it(`${operation} 异常 ID 等于本次真正发出的 metadata`, async () => {
      let requestId = '';
      const status = buildStatus(Code.UNAUTHORIZED, 'server message');
      let invocation: () => Promise<unknown>;
      if (operation === 'ack') {
        sinon.stub(RpcClient.prototype, 'ackMessage').callsFake(async (_req, metadata) => {
          requestId = String(metadata.get('x-mq-request-id')[0]);
          return new AckMessageResponse().setStatus(status);
        });
        invocation = () => consumer.ackForTest();
      } else if (operation === 'invisible') {
        sinon.stub(RpcClient.prototype, 'changeInvisibleDuration').callsFake(async (_req, metadata) => {
          requestId = String(metadata.get('x-mq-request-id')[0]);
          return new ChangeInvisibleDurationResponse().setStatus(status);
        });
        invocation = () => consumer.invisibleForTest();
      } else {
        sinon.stub(RpcClient.prototype, 'queryRoute').callsFake(async (_req, metadata) => {
          requestId = String(metadata.get('x-mq-request-id')[0]);
          return new QueryRouteResponse().setStatus(status);
        });
        invocation = () => consumer.routeForTest();
      }
      await assert.rejects(invocation, error => matchesRequest(error, requestId));
      assert.match(requestId, /^[a-f0-9-]{36}$/);
    });
  });

  [ Code.OK, Code.MESSAGE_NOT_FOUND, Code.UNAUTHORIZED ].forEach(code => {
    it(`receive 状态 ${code} 保留空结果或错误 ID`, async () => {
      let requestId = '';
      sinon.stub(RpcClient.prototype, 'receiveMessage').callsFake(async (_req, metadata) => {
        requestId = String(metadata.get('x-mq-request-id')[0]);
        return [ new ReceiveMessageResponse().setStatus(buildStatus(code, 'server message')) ];
      });
      if (code === Code.UNAUTHORIZED) {
        await assert.rejects(() => consumer.receiveForTest(), error => matchesRequest(error, requestId));
      } else {
        assert.deepEqual(await consumer.receiveForTest(), []);
      }
    });
  });

  it('receive 流缺失 status 不被当作空结果成功', async () => {
    let requestId = '';
    sinon.stub(RpcClient.prototype, 'receiveMessage').callsFake(async (_req, metadata) => {
      requestId = String(metadata.get('x-mq-request-id')[0]);
      return [];
    });
    await assert.rejects(() => consumer.receiveForTest(), error => {
      assert(error instanceof InternalErrorException);
      assert(error.message.includes(`request-id=${requestId}`));
      return true;
    });
  });

  it('ack 的 MESSAGE_NOT_FOUND 抛 NotFoundException', async () => {
    sinon.stub(RpcClient.prototype, 'ackMessage').resolves(new AckMessageResponse().setStatus(buildStatus(Code.MESSAGE_NOT_FOUND)));
    await assert.rejects(() => consumer.ackForTest(), NotFoundException);
  });

  it('并发 RPC 逆序返回，响应身份和 requestId 不串号', async () => {
    const first = deferred<AckMessageResponse>();
    const second = deferred<AckMessageResponse>();
    const ids: string[] = [];
    sinon.stub(RpcClient.prototype, 'ackMessage').callsFake((_req, metadata) => {
      ids.push(String(metadata.get('x-mq-request-id')[0]));
      return ids.length === 1 ? first.promise : second.promise;
    });
    const a = consumer.ackForTest();
    const b = consumer.ackForTest();
    const failedA = assert.rejects(a, error => matchesRequest(error, ids[0]));
    const failedB = assert.rejects(b, error => matchesRequest(error, ids[1]));
    second.resolve(new AckMessageResponse().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message')));
    await failedB;
    first.resolve(new AckMessageResponse().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message')));
    await failedA;
    assert.notEqual(ids[0], ids[1]);
  });

  it('send 顶层和 entry 状态均读取响应所属请求 ID，返回类型不变', async () => {
    for (const entryStatus of [ false, true ]) {
      let requestId = '';
      const response = new SendMessageResponse().setStatus(buildStatus(entryStatus ? Code.OK : Code.UNAUTHORIZED, 'server message'));
      if (entryStatus) response.addEntries().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message'));
      const stub = sinon.stub(RpcClient.prototype, 'sendMessage').callsFake(async (_req, metadata) => {
        requestId = String(metadata.get('x-mq-request-id')[0]);
        return response;
      });
      const result = await consumer.getRpcClientManager().sendMessage(endpoints, new SendMessageRequest(), 3000);
      assert.equal(result, response);
      assert.equal(getRequestId(result), requestId);
      assert.throws(() => SendReceipt.processResponseInvocation(queue, result), error => matchesRequest(error, requestId));
      stub.restore();
    }
  });

  it('endTransaction 的错误 ID 保留到调用方', async () => {
    const producer = new Producer({ endpoints: endpoints.facade, namespace: '', logger });
    clients.push(producer);
    let requestId = '';
    sinon.stub(RpcClient.prototype, 'endTransaction').callsFake(async (_req, metadata) => {
      requestId = String(metadata.get('x-mq-request-id')[0]);
      return new EndTransactionResponse().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message'));
    });
    await assert.rejects(() => producer.endTransaction(endpoints, new Message({ topic: 'test-topic', body: Buffer.from('body') }),
      'message-id', 'transaction-id', TransactionResolution.COMMIT), error => matchesRequest(error, requestId));
  });

  it('heartbeat 记录的错误带本次 ID', async () => {
    consumer.cacheRouteForTest();
    let requestId = '';
    sinon.stub(RpcClient.prototype, 'heartbeat').callsFake(async (_req, metadata) => {
      requestId = String(metadata.get('x-mq-request-id')[0]);
      return new HeartbeatResponse().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message'));
    });
    await (consumer as unknown as { doHeartbeat(): Promise<void> }).doHeartbeat();
    assert(logger.warn.calledOnce);
    assert(logger.warn.firstCall.args.some(arg => typeof arg === 'string' && arg.includes(`request-id=${requestId}`)));
  });

  it('queryAssignment 的错误带本次 ID', async () => {
    const push = new TestPushConsumer({ endpoints: endpoints.facade, namespace: '', logger, consumerGroup: 'test-group',
      subscriptions: new Map([[ 'test-topic', '*' ]]), messageListener: { consume: () => ConsumeResult.SUCCESS } });
    clients.push(push);
    push.cacheRouteForTest();
    let requestId = '';
    sinon.stub(RpcClient.prototype, 'queryAssignment').callsFake(async (_req, metadata) => {
      requestId = String(metadata.get('x-mq-request-id')[0]);
      return new QueryAssignmentResponse().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message'));
    });
    push.scanForTest();
    await clock.tickAsync(1000);
    const error = logger.error.firstCall.args.find(arg => arg instanceof ClientException);
    matchesRequest(error, requestId);
  });

  it('telemetry 未知命令关联当前建流 ID，刷新后 ID 更新', () => {
    const streams: FakeTelemetryStream[] = [];
    const ids: string[] = [];
    sinon.stub(RpcClient.prototype, 'telemetry').callsFake(metadata => {
      ids.push(String(metadata.get('x-mq-request-id')[0]));
      const stream = new FakeTelemetryStream();
      streams.push(stream);
      return stream.asGrpcStream();
    });
    const check = sinon.spy(consumer, 'onUnknownCommand');
    const session = new TelemetrySession(consumer, endpoints, logger);
    try {
      for (let i = 0; i < 2; i++) {
        if (i > 0) session.refresh();
        streams[i].receive(new TelemetryCommand().setStatus(buildStatus(Code.UNAUTHORIZED, 'server message')));
        const status = check.getCall(i).args[1];
        assert.equal(getRequestId(status), ids[i]);
      }
      assert.notEqual(ids[0], ids[1]);
    } finally {
      session.release();
    }
  });

  it('缺省 metadata ID 的辅助读取保持可选签名', () => {
    const metadata = new Metadata();
    assert.deepEqual(metadata.get('x-mq-request-id'), []);
    assert.equal(getRequestId({}), undefined);
    assert.equal(getRequestId(), undefined);
  });
});
