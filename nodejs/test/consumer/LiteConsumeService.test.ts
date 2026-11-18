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

import * as assert from 'node:assert';
import * as sinon from 'sinon';
import { Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { AckMessageResponse, ChangeInvisibleDurationResponse, ForwardMessageToDeadLetterQueueResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { ConsumeResult, ConsumeResultSuspend } from '../../src/consumer/ConsumeResult';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { LitePushConsumerImpl } from '../../src/consumer/LitePushConsumerImpl';
import { LiteStandardConsumeService } from '../../src/consumer/LiteStandardConsumeService';
import { LiteFifoConsumeService } from '../../src/consumer/LiteFifoConsumeService';
import { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { MessageView } from '../../src/message';
import { Endpoints } from '../../src/route';
import { createSilentLogger, deferred, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildSettings, buildStatus } from '../support/protobuf';

for (const fifo of [ false, true ]) {
  describe(`Lite ${fifo ? 'FIFO' : 'standard'} service settlement`, () => {
    let client: LitePushConsumerImpl;
    let rpc: ReturnType<typeof installFakeRpcClientManager>;
    let listener: sinon.SinonStub<[MessageView], Promise<ConsumeResult>>;
    let service: LiteStandardConsumeService | LiteFifoConsumeService;
    let pq: ProcessQueue;
    let clock: sinon.SinonFakeTimers;

    beforeEach(() => {
      listener = sinon.stub<[MessageView], Promise<ConsumeResult>>().resolves(ConsumeResult.SUCCESS);
      client = new LitePushConsumerImpl({
        endpoints: '127.0.0.1:8081', namespace: '', consumerGroup: 'group', bindTopic: 'parent',
        subscriptions: new Map(), messageListener: { consume: listener }, logger: createSilentLogger(),
      });
      rpc = installFakeRpcClientManager(client);
      clock = sinon.useFakeTimers();
      rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
      rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse().setStatus(buildStatus()));
      rpc.forwardMessageToDeadLetterQueue.resolves(new ForwardMessageToDeadLetterQueueResponse().setStatus(buildStatus()));
      client.onSettingsCommand(new Endpoints('127.0.0.1:8081'), buildSettings({ subscription: new Subscription().setFifo(fifo) }));
      service = fifo
        ? new LiteFifoConsumeService(client.clientId, { consume: listener }, true, client, 'group')
        : new LiteStandardConsumeService(client.clientId, { consume: listener }, client, 'group');
      sinon.stub(client, 'getConsumeService').returns(service);
      pq = new ProcessQueue(client, buildRoute({ topic: 'parent' }).messageQueues[0], FilterExpression.SUB_ALL);
    });

    afterEach(async () => {
      pq.abort();
      service.abort();
      const shutdown = client.shutdown();
      await clock.tickAsync(2000);
      await shutdown;
      clock.restore();
      sinon.restore();
    });

    function message(id = 'one', liteTopic = 'a', corrupted = false) {
      return new MessageView(buildMessagePB({ topic: 'parent', messageId: id, liteTopic,
        checksum: corrupted ? 'bad-checksum' : undefined }), pq.getMessageQueue());
    }

    function consume(messages: MessageView[]) {
      pq.cacheMessages(messages);
      service.consume(pq, messages);
    }

    it('acks success and returns the cache permit', async () => {
      const view = message();
      consume([ view ]);
      await clock.tickAsync(0);
      sinon.assert.calledOnce(listener);
      sinon.assert.calledOnce(rpc.ackMessage);
      assert.strictEqual(rpc.ackMessage.firstCall.args[1].getEntriesList()[0].getLiteTopic(), 'a');
      assert.strictEqual(pq.cachedMessagesCount(), 0);
      assert.strictEqual(pq.availablePermits(), 32);
      sinon.assert.notCalled(rpc.changeInvisibleDuration);
    });

    it('retries failure locally before ack without changing invisible time', async () => {
      listener.onFirstCall().resolves(ConsumeResult.FAILURE);
      const view = message();
      consume([ view ]);
      await clock.tickAsync(0);
      assert.strictEqual(pq.cachedMessagesCount(), 1);
      sinon.assert.notCalled(rpc.ackMessage);
      await clock.tickAsync(1000);
      sinon.assert.calledTwice(listener);
      assert.strictEqual(view.deliveryAttempt, 2);
      sinon.assert.calledOnce(rpc.ackMessage);
      sinon.assert.notCalled(rpc.changeInvisibleDuration);
      sinon.assert.notCalled(rpc.forwardMessageToDeadLetterQueue);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
    });

    it('forwards exhausted local retries to the dead letter queue', async () => {
      listener.resolves(ConsumeResult.FAILURE);
      consume([ message() ]);
      await clock.tickAsync(4000);
      sinon.assert.calledThrice(listener);
      sinon.assert.calledOnce(rpc.forwardMessageToDeadLetterQueue);
      assert.strictEqual(rpc.forwardMessageToDeadLetterQueue.firstCall.args[1].getLiteTopic(), 'a');
      sinon.assert.notCalled(rpc.ackMessage);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
    });

    it('discards corrupted messages without invoking the listener', async () => {
      const view = message('bad', 'a', true);
      assert.ok(view.corrupted);
      consume([ view ]);
      await clock.tickAsync(0);
      sinon.assert.notCalled(listener);
      sinon.assert.notCalled(rpc.ackMessage);
      if (fifo) sinon.assert.calledOnce(rpc.forwardMessageToDeadLetterQueue);
      else sinon.assert.calledOnce(rpc.changeInvisibleDuration);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
    });

    it('sets liteTopic, suspend and the exact duration when the listener requests SUSPEND', async () => {
      listener.resolves(ConsumeResultSuspend.of(4321));
      consume([ message() ]);
      await clock.tickAsync(0);
      sinon.assert.calledOnce(rpc.changeInvisibleDuration);
      const request = rpc.changeInvisibleDuration.firstCall.args[1];
      assert.strictEqual(request.getLiteTopic(), 'a');
      assert.strictEqual(request.getSuspend(), true);
      assert.deepStrictEqual(request.getInvisibleDuration()!.toObject(), { seconds: 4, nanos: 321000000 });
      sinon.assert.notCalled(rpc.ackMessage);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
    });

    it('abort settles a pending local retry without invoking the listener or settlement RPC', async () => {
      listener.resolves(ConsumeResult.FAILURE);
      consume([ message() ]);
      await clock.tickAsync(0);
      sinon.assert.calledOnce(listener);
      assert.strictEqual(pq.cachedMessagesCount(), 1);
      pq.abort();
      service.abort();
      await clock.tickAsync(30000);
      sinon.assert.calledOnce(listener);
      sinon.assert.notCalled(rpc.ackMessage);
      sinon.assert.notCalled(rpc.changeInvisibleDuration);
      sinon.assert.notCalled(rpc.forwardMessageToDeadLetterQueue);
      assert.strictEqual(pq.cachedMessagesCount(), 0);
      assert.strictEqual(clock.countTimers(), 0);
    });

    if (fifo) {
      it('consumes different liteTopics in parallel and waits for settlement before the next message of one topic', async () => {
        const first = deferred<ConsumeResult>();
        const ack = deferred<AckMessageResponse>();
        listener.callsFake(async view => (view.messageId === 'a1' ? first.promise : ConsumeResult.SUCCESS));
        rpc.ackMessage.callsFake(async (_endpoints, request) => {
          return request.getEntriesList()[0].getMessageId() === 'a1'
            ? ack.promise : new AckMessageResponse().setStatus(buildStatus());
        });
        consume([ message('a1', 'a'), message('a2', 'a'), message('b1', 'b') ]);
        await clock.tickAsync(0);
        assert.deepStrictEqual(listener.getCalls().map(call => call.args[0].messageId), [ 'a1', 'b1' ]);
        first.resolve(ConsumeResult.SUCCESS);
        await clock.tickAsync(0);
        assert.strictEqual(listener.callCount, 2);
        ack.resolve(new AckMessageResponse().setStatus(buildStatus()));
        await clock.tickAsync(0);
        assert.deepStrictEqual(listener.getCalls().map(call => call.args[0].messageId), [ 'a1', 'b1', 'a2' ]);
        assert.strictEqual(pq.cachedMessagesCount(), 0);
      });
    }
  });
}
