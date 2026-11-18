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
import { ClientType, Code, FilterType } from '../../proto/apache/rocketmq/v2/definition_pb';
import { AckMessageResponse, ChangeInvisibleDurationResponse, ReceiveMessageResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { SimpleConsumer, SimpleConsumerOptions } from '../../src/consumer/SimpleConsumer';
import { BadRequestException, InternalErrorException } from '../../src/exception';
import { MessageView } from '../../src/message';
import { TopicRouteData } from '../../src/route';
import { createSilentLogger, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildStatus } from '../support/protobuf';

class SimpleConsumerHarness extends SimpleConsumer {
  getRouteData(topic: string) {
    return super.getRouteData(topic);
  }

  settingsSnapshot() {
    return this.getSettings().toProtobuf();
  }

  updateRoute(topic: string, route: TopicRouteData) {
    this.onTopicRouteDataUpdate(topic, route);
  }

  heartbeatRequest() {
    return this.wrapHeartbeatRequest();
  }

  terminationRequest() {
    return this.wrapNotifyClientTerminationRequest();
  }
}

describe('SimpleConsumer', () => {
  const clients: SimpleConsumerHarness[] = [];

  function createConsumer(options: Partial<SimpleConsumerOptions> = {}) {
    const logger = createSilentLogger();
    const consumer = new SimpleConsumerHarness({ endpoints: '127.0.0.1:8081', namespace: '', consumerGroup: 'group',
      requestTimeout: 3000, subscriptions: new Map([[ 'topic-a', FilterExpression.SUB_ALL ]]), logger, ...options });
    clients.push(consumer);
    const rpc = installFakeRpcClientManager(consumer);
    rpc.receiveMessage.resolves([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    const running = sinon.stub(consumer, 'isRunning').returns(true);
    const route = sinon.stub(consumer, 'getRouteData').callsFake(async topic => buildRoute({ topic, queuesPerBroker: 3 }));
    return { consumer, rpc, running, route, logger };
  }

  afterEach(async () => {
    for (const consumer of clients.splice(0)) {
      await consumer.shutdown();
    }
    sinon.restore();
  });

  it('现有 string 与 FilterExpression 订阅、默认参数及组查询保持可用', async () => {
    const { consumer, rpc } = createConsumer({ subscriptions: new Map<string, FilterExpression | string>([
      [ 'topic-a', 'tag' ], [ 'topic-b', new FilterExpression('a > 1', FilterType.SQL) ],
    ]) });
    assert.strictEqual(consumer.getConsumerGroup(), 'group');
    await consumer.receive();
    await consumer.receive();
    const requests = rpc.receiveMessage.getCalls().map(call => call.args[1]);
    assert.strictEqual(new Set(requests.map(request => request.getMessageQueue()?.getTopic()?.getName())).size, 2);
    for (const request of requests) {
      assert.strictEqual(request.getBatchSize(), 10);
      assert.strictEqual(request.getInvisibleDuration()?.getSeconds(), 15);
      assert.strictEqual(request.getLongPollingTimeout()?.getSeconds(), 30);
    }
    assert.strictEqual(consumer.heartbeatRequest().getClientType(), ClientType.SIMPLE_CONSUMER);
    assert.strictEqual(consumer.heartbeatRequest().getGroup()?.getName(), 'group');
    assert.strictEqual(consumer.terminationRequest().getGroup()?.getName(), 'group');
  });

  it('多 topic 连续轮询，缓存路由并只发送 master 的各个队列', async () => {
    const { consumer, rpc, route } = createConsumer({ subscriptions: new Map([
      [ 'topic-a', FilterExpression.SUB_ALL ], [ 'topic-b', FilterExpression.SUB_ALL ],
    ]) });
    route.callsFake(async topic => buildRoute({ topic, queuesPerBroker: 3, brokers: [
      { name: 'master', id: 0, endpoints: '127.0.0.1:8081' },
      { name: 'slave', id: 1, endpoints: '127.0.0.1:8082' },
    ] }));
    for (let i = 0; i < 6; i++) await consumer.receive();
    const queues = rpc.receiveMessage.getCalls().map(call => call.args[1].getMessageQueue()!);
    assert.ok(queues.every(queue => queue.getBroker()?.getId() === 0));
    assert.notStrictEqual(queues[0].getTopic()?.getName(), queues[1].getTopic()?.getName());
    assert.strictEqual(queues[0].getTopic()?.getName(), queues[2].getTopic()?.getName());
    for (const topic of [ 'topic-a', 'topic-b' ]) {
      assert.deepStrictEqual(queues.filter(queue => queue.getTopic()?.getName() === topic).map(queue => queue.getId()).sort(), [ 0, 1, 2 ]);
    }
    sinon.assert.calledTwice(route);
  });

  it('路由更新保留轮询位置，后续 receive 使用更新后的端点', async () => {
    const { consumer, rpc } = createConsumer();
    await consumer.receive();
    const firstId = rpc.receiveMessage.firstCall.args[1].getMessageQueue()!.getId();
    consumer.updateRoute('topic-a', buildRoute({ topic: 'topic-a', queuesPerBroker: 3,
      brokers: [{ name: 'updated', endpoints: '127.0.0.1:8083' }] }));
    await consumer.receive();
    assert.strictEqual(rpc.receiveMessage.secondCall.args[1].getMessageQueue()?.getId(), (firstId + 1) % 3);
    assert.strictEqual(rpc.receiveMessage.secondCall.args[0].facade, '127.0.0.1:8083');
  });

  it('subscribe 查询成功后写入订阅，失败时保留原订阅', async () => {
    const { consumer, route, rpc } = createConsumer();
    const filter = new FilterExpression('tag');
    route.withArgs('topic-b').rejects(new Error('route unavailable'));
    await assert.rejects(consumer.subscribe('topic-b', filter), /route unavailable/);
    assert.deepStrictEqual(consumer.settingsSnapshot().getSubscription()?.getSubscriptionsList()
      .map(entry => entry.getTopic()?.getName()), [ 'topic-a' ]);
    route.withArgs('topic-b').resolves(buildRoute({ topic: 'topic-b' }));
    assert.strictEqual(await consumer.subscribe('topic-b', filter), undefined);
    consumer.unsubscribe('topic-a');
    await consumer.receive();
    assert.strictEqual(rpc.receiveMessage.firstCall.args[1].getMessageQueue()?.getTopic()?.getName(), 'topic-b');
    assert.strictEqual(rpc.receiveMessage.firstCall.args[1].getFilterExpression()?.getExpression(), 'tag');
  });

  it('unsubscribe 只更新本地订阅，删除后不再选择该 topic', async () => {
    const { consumer, route, rpc } = createConsumer({ subscriptions: new Map([
      [ 'topic-a', FilterExpression.SUB_ALL ], [ 'topic-b', FilterExpression.SUB_ALL ],
    ]) });
    assert.strictEqual(consumer.unsubscribe('topic-a'), undefined);
    consumer.unsubscribe('unknown-topic');
    for (let i = 0; i < 3; i++) await consumer.receive();
    assert.ok(rpc.receiveMessage.getCalls().every(call => call.args[1].getMessageQueue()?.getTopic()?.getName() === 'topic-b'));
    sinon.assert.calledOnce(route);
    consumer.unsubscribe('topic-b');
    await assert.rejects(consumer.receive(), /No subscriptions available/);
    sinon.assert.calledThrice(rpc.receiveMessage);
  });

  it('保留 startup 前的订阅调整能力及 subscribe/unsubscribe 返回类型', async () => {
    const { consumer, running } = createConsumer();
    running.returns(false);
    assert.strictEqual(await consumer.subscribe('topic-b', FilterExpression.SUB_ALL), undefined);
    assert.strictEqual(consumer.unsubscribe('topic-a'), undefined);
    assert.deepStrictEqual(consumer.settingsSnapshot().getSubscription()?.getSubscriptionsList()
      .map(entry => entry.getTopic()?.getName()), [ 'topic-b' ]);
  });

  it('未运行时 receive、ack、changeInvisibleDuration 均拒绝且不发 RPC', async () => {
    const { consumer, running, rpc } = createConsumer();
    running.returns(false);
    const message = new MessageView(buildMessagePB(), buildRoute().messageQueues[0]);
    await assert.rejects(consumer.receive(), /not running/);
    await assert.rejects(consumer.ack(message), /not running/);
    await assert.rejects(consumer.changeInvisibleDuration(message, 15000), /not running/);
    sinon.assert.notCalled(rpc.receiveMessage);
    sinon.assert.notCalled(rpc.ackMessage);
    sinon.assert.notCalled(rpc.changeInvisibleDuration);
  });

  it('receive 拒绝非正批量大小且不发 RPC', async () => {
    const { consumer, rpc } = createConsumer();
    for (const count of [ 0, -1 ]) await assert.rejects(consumer.receive(count), /maxMessageNum must be greater than 0/);
    sinon.assert.notCalled(rpc.receiveMessage);
  });

  it('成功更新 receiptHandle，后续 ack 使用新句柄并保留 void 返回值', async () => {
    const { consumer, rpc } = createConsumer();
    const message = new MessageView(buildMessagePB(), buildRoute().messageQueues[0]);
    rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse().setStatus(buildStatus()).setReceiptHandle('new'));
    assert.strictEqual(await consumer.changeInvisibleDuration(message, 15000), undefined);
    assert.strictEqual(message.receiptHandle, 'new');
    rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
    assert.strictEqual(await consumer.ack(message), undefined);
    assert.strictEqual(rpc.ackMessage.firstCall.args[1].getEntriesList()[0].getReceiptHandle(), 'new');
  });

  it('续期失败时保留旧 receiptHandle', async () => {
    const { consumer, rpc } = createConsumer();
    const message = new MessageView(buildMessagePB(), buildRoute().messageQueues[0]);
    const old = message.receiptHandle;
    rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse()
      .setStatus(buildStatus(Code.INVALID_RECEIPT_HANDLE)).setReceiptHandle('invalid-new'));
    await assert.rejects(consumer.changeInvisibleDuration(message, 15000), BadRequestException);
    assert.strictEqual(message.receiptHandle, old);
  });

  it('仅 InternalErrorException 重试，复用 request 和队列并按 1000ms 退避', async () => {
    const clock = sinon.useFakeTimers();
    const { consumer, rpc } = createConsumer();
    rpc.receiveMessage.onFirstCall().resolves([ new ReceiveMessageResponse().setStatus(buildStatus(Code.INTERNAL_SERVER_ERROR)) ]);
    rpc.receiveMessage.onSecondCall().resolves([ new ReceiveMessageResponse().setStatus(buildStatus()) ]);
    const pending = consumer.receive();
    await clock.tickAsync(0);
    sinon.assert.calledOnce(rpc.receiveMessage);
    await clock.tickAsync(999);
    sinon.assert.calledOnce(rpc.receiveMessage);
    await clock.tickAsync(1);
    assert.deepStrictEqual(await pending, []);
    sinon.assert.calledTwice(rpc.receiveMessage);
    assert.strictEqual(rpc.receiveMessage.firstCall.args[1], rpc.receiveMessage.secondCall.args[1]);
    assert.strictEqual(rpc.receiveMessage.firstCall.args[0], rpc.receiveMessage.secondCall.args[0]);
  });

  for (const attempts of [ undefined, 1, 2, 4 ]) {
    it(`重试耗尽在 RPC 等待与退避预算内结束（maxRetryAttempts=${attempts ?? '默认3'}）`, async () => {
      const clock = sinon.useFakeTimers();
      const { consumer, rpc } = createConsumer({ maxRetryAttempts: attempts });
      rpc.receiveMessage.callsFake(async (_endpoints, _request, timeout) => {
        await new Promise(resolve => setTimeout(resolve, timeout));
        return [ new ReceiveMessageResponse().setStatus(buildStatus(Code.INTERNAL_SERVER_ERROR)) ];
      });
      let settled = false;
      const pending = assert.rejects(consumer.receive(), InternalErrorException).then(() => { settled = true; });
      const count = attempts ?? 3;
      const budget = count * 33000 + (count - 1) * 1000;
      await clock.tickAsync(budget - 1);
      assert.strictEqual(settled, false);
      await clock.tickAsync(1);
      await pending;
      assert.strictEqual(settled, true);
      assert.strictEqual(rpc.receiveMessage.callCount, count);
      assert.strictEqual(clock.now, budget);
    });
  }

  for (const code of [ Code.BAD_REQUEST, Code.PROXY_TIMEOUT ]) {
    it(`非内部错误 ${code} 不重试`, async () => {
      const { consumer, rpc } = createConsumer();
      rpc.receiveMessage.resolves([ new ReceiveMessageResponse().setStatus(buildStatus(code)) ]);
      await assert.rejects(consumer.receive());
      sinon.assert.calledOnce(rpc.receiveMessage);
    });
  }

  it('传输异常不重试并保留原异常对象', async () => {
    const { consumer, rpc } = createConsumer();
    const error = new Error('transport');
    rpc.receiveMessage.rejects(error);
    await assert.rejects(consumer.receive(), reason => reason === error);
    sinon.assert.calledOnce(rpc.receiveMessage);
  });

  it('构造时拒绝非法 maxRetryAttempts，在创建客户端资源前报 TypeError', () => {
    for (const maxRetryAttempts of [ 0, -1, NaN, Infinity, -Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1 ]) {
      assert.throws(() => new SimpleConsumer({ endpoints: '127.0.0.1:8081', namespace: '', consumerGroup: 'group',
        subscriptions: new Map([[ 'topic-a', FilterExpression.SUB_ALL ]]), maxRetryAttempts }),
      { name: 'TypeError', message: 'maxRetryAttempts must be a positive safe integer' });
    }
  });
});
