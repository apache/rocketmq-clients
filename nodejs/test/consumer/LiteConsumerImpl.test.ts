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
import { Assignment, ClientType, LiteSubscriptionAction, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageResponse, ChangeInvisibleDurationResponse, HeartbeatResponse,
  NotifyUnsubscribeLiteCommand, QueryAssignmentResponse, QueryRouteResponse,
  ReceiveMessageResponse, SyncLiteSubscriptionResponse, TelemetryCommand,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { LitePushConsumerImpl } from '../../src/consumer/LitePushConsumerImpl';
import { LiteSimpleConsumerImpl } from '../../src/consumer/LiteSimpleConsumerImpl';
import { LiteFifoConsumeService } from '../../src/consumer/LiteFifoConsumeService';
import { LiteStandardConsumeService } from '../../src/consumer/LiteStandardConsumeService';
import { LiteSubscriptionManager } from '../../src/consumer/LiteSubscriptionManager';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import { OffsetOption } from '../../src/consumer/OffsetOption';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { SimpleConsumer } from '../../src/consumer/SimpleConsumer';
import { Endpoints } from '../../src/route';
import { createSilentLogger, deferred, FakeTelemetryStream, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildSettings, buildStatus } from '../support/protobuf';

const options = {
  endpoints: '127.0.0.1:8081', namespace: 'ns', consumerGroup: 'group', bindTopic: 'parent',
  requestTimeout: 1234,
};
const route = buildRoute({
  topic: 'parent', namespace: 'ns', queuesPerBroker: 2,
  brokers: [{ name: 'a', endpoints: '127.0.0.1:8081' }, { name: 'b', endpoints: '127.0.0.1:8082' }],
});
const endpoint = new Endpoints(options.endpoints);

for (const kind of [ 'push', 'simple' ] as const) {
  describe(`Lite ${kind} consumer offline`, () => {
    let client: LitePushConsumerImpl | LiteSimpleConsumerImpl;
    let rpc: ReturnType<typeof installFakeRpcClientManager>;
    let clock: sinon.SinonFakeTimers;
    let streams: FakeTelemetryStream[];

    beforeEach(() => {
      client = kind === 'push'
        ? new LitePushConsumerImpl({ ...options, subscriptions: new Map(),
          logger: createSilentLogger(), messageListener: { consume: async () => ConsumeResult.SUCCESS } })
        : new LiteSimpleConsumerImpl({ ...options, logger: createSilentLogger() });
      rpc = installFakeRpcClientManager(client);
      clock = sinon.useFakeTimers();
      streams = [];
      rpc.queryRoute.resolves(new QueryRouteResponse().setStatus(buildStatus())
        .setMessageQueuesList(route.messageQueues.map(mq => mq.toProtobuf())));
      rpc.syncLiteSubscription.resolves(new SyncLiteSubscriptionResponse().setStatus(buildStatus()));
      rpc.notifyClientTermination.resolves({} as never);
      rpc.heartbeat.resolves(new HeartbeatResponse().setStatus(buildStatus()));
      rpc.queryAssignment.resolves(new QueryAssignmentResponse().setStatus(buildStatus()));
      rpc.telemetry.callsFake(() => {
        const stream = new FakeTelemetryStream();
        streams.push(stream);
        // Wait for telemetry listeners to attach before simulating the first server settings.
        Promise.resolve().then(() => stream.receive(new TelemetryCommand().setSettings(buildSettings({
          clientType: kind === 'push' ? ClientType.LITE_PUSH_CONSUMER : ClientType.LITE_SIMPLE_CONSUMER,
          subscription: new Subscription().setLiteSubscriptionQuota(3).setMaxLiteTopicSize(8),
        }))));
        return stream.asGrpcStream();
      });
    });

    afterEach(async () => {
      const shutdown = client.shutdown();
      await clock.tickAsync(2000);
      await shutdown;
      for (const stream of streams) stream.cancel();
      clock.restore();
      sinon.restore();
    });

    it('retains parent class, host accessors, group and settings client type', () => {
      assert.ok(client instanceof (kind === 'push' ? PushConsumer : SimpleConsumer));
      assert.strictEqual(client.getConsumerGroup(), 'group');
      assert.strictEqual(client.getLiteTopicSet().size, 0);
      assert.strictEqual(client.getRpcClientManager(), rpc);
      assert.strictEqual(client.getRequestTimeout(), 1234);
      assert.strictEqual(client.settingsCommand().getSettings()!.getClientType(),
        kind === 'push' ? ClientType.LITE_PUSH_CONSUMER : ClientType.LITE_SIMPLE_CONSUMER);
      assert.ok(client.getLogger());
    });

    it('preserves blank-input validation and rejects legal subscriptions before startup', async () => {
      for (const method of [ 'subscribeLite', 'unsubscribeLite' ] as const) {
        await assert.rejects(client[method](' '), /liteTopic should not be blank/);
        await assert.rejects(client[method]('one'), /Consumer is not running/);
      }
      sinon.assert.notCalled(rpc.syncLiteSubscription);
    });

    it('starts base resources before initial sync, forwards offsets and subscriptions to all endpoints', async () => {
      rpc.syncLiteSubscription.callsFake(async () => {
        assert.ok(client.isRunning());
        return new SyncLiteSubscriptionResponse().setStatus(buildStatus());
      });
      await client.startup();
      await clock.tickAsync(0);
      assert.strictEqual(rpc.syncLiteSubscription.callCount, 2);
      assert.ok(rpc.syncLiteSubscription.getCalls().every(call => call.args[1].getAction() === LiteSubscriptionAction.COMPLETE_ADD));
      assert.deepStrictEqual(client.getSyncEndpoints().map(e => e.facade), route.getTotalEndpoints().map(e => e.facade));
      rpc.syncLiteSubscription.resetHistory();
      await client.subscribeLite('one');
      await client.subscribeLite('two', OffsetOption.MIN_OFFSET);
      assert.strictEqual(rpc.syncLiteSubscription.callCount, 4);
      assert.ok(!rpc.syncLiteSubscription.firstCall.args[1].hasOffsetOption());
      assert.deepStrictEqual(rpc.syncLiteSubscription.lastCall.args[1].getOffsetOption()!.toObject(),
        OffsetOption.MIN_OFFSET.toProtobuf().toObject());
      const snapshot = client.getLiteTopicSet();
      snapshot.clear();
      assert.strictEqual(client.getLiteTopicSet().size, 2);
      client.onNotifyUnsubscribeLiteCommand(endpoint, new NotifyUnsubscribeLiteCommand().setLiteTopic('one'));
      assert.deepStrictEqual([ ...client.getLiteTopicSet() ], [ 'two' ]);
      await client.unsubscribeLite('two');
      assert.strictEqual(client.getLiteTopicSet().size, 0);
    });

    it('stops the manager before parent shutdown and keeps notifications safe while draining', async () => {
      await client.startup();
      await client.subscribeLite('one');
      const stopped = sinon.spy(LiteSubscriptionManager.prototype, 'shutdown');
      const notify = deferred<never>();
      rpc.notifyClientTermination.callsFake(() => {
        assert.ok(stopped.called);
        assert.strictEqual(client.getLiteTopicSet().size, 0);
        return notify.promise;
      });
      const shutdown = client.close();
      await clock.tickAsync(1000);
      assert.ok(rpc.notifyClientTermination.called);
      const syncCount = rpc.syncLiteSubscription.callCount;
      client.onNotifyUnsubscribeLiteCommand(endpoint, new NotifyUnsubscribeLiteCommand().setLiteTopic('one'));
      await clock.tickAsync(60000);
      assert.strictEqual(rpc.syncLiteSubscription.callCount, syncCount);
      notify.resolve({} as never);
      await shutdown;
      assert.strictEqual(clock.countTimers(), 0);
      assert.ok(!client.isRunning());
      await assert.rejects(client.subscribeLite('two'), /Consumer is not running/);
    });

    it('applies settings quota and maximum length to both consumer flavors', async () => {
      await client.startup();
      client.onSettingsCommand(endpoint, buildSettings({ subscription: new Subscription()
        .setLiteSubscriptionQuota(1).setMaxLiteTopicSize(3) }));
      await assert.rejects(client.subscribeLite('long'), /max length 3/);
      await client.subscribeLite('one');
      await assert.rejects(client.subscribeLite('two'), /quota exceeded 1/);
    });

    it('does not start lite synchronization when parent startup fails', async () => {
      const failure = new Error('startup failed');
      const parent = kind === 'push' ? PushConsumer.prototype : SimpleConsumer.prototype;
      sinon.stub(parent, 'startup').rejects(failure);
      await assert.rejects(client.startup(), error => error === failure);
      sinon.assert.notCalled(rpc.syncLiteSubscription);
      assert.strictEqual(clock.countTimers(), 0);
    });

    if (kind === 'simple') {
      it('receives only on the first readable master and propagates liteTopic to ack and invisible duration', async () => {
        await client.startup();
        const simple = client as LiteSimpleConsumerImpl;
        rpc.receiveMessage.resolves([
          new ReceiveMessageResponse().setStatus(buildStatus()),
          new ReceiveMessageResponse().setMessage(buildMessagePB({ topic: 'parent', liteTopic: 'one' })),
        ]);
        const messages = await simple.receive(2, 15000);
        await simple.receive(2, 15000);
        assert.strictEqual(messages[0].liteTopic, 'one');
        for (const call of rpc.receiveMessage.getCalls()) {
          assert.strictEqual(call.args[1].getMessageQueue()!.getBroker()!.getName(), 'a');
          assert.strictEqual(call.args[1].getMessageQueue()!.getId(), 0);
          assert.strictEqual(call.args[1].getAutoRenew(), false);
        }
        rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
        rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse().setStatus(buildStatus()).setReceiptHandle('renewed'));
        await simple.ack(messages[0]);
        await simple.changeInvisibleDuration(messages[0], 5000);
        assert.strictEqual(rpc.ackMessage.firstCall.args[1].getEntriesList()[0].getLiteTopic(), 'one');
        assert.strictEqual(rpc.changeInvisibleDuration.firstCall.args[1].getLiteTopic(), 'one');
        assert.strictEqual(rpc.changeInvisibleDuration.firstCall.args[1].getSuspend(), false);
        assert.strictEqual(messages[0].receiptHandle, 'renewed');
      });
    } else {
      [ true, false ].forEach(fifo => {
        it(`selects the lite ${fifo ? 'FIFO' : 'standard'} consume service`, async () => {
          rpc.telemetry.callsFake(() => {
            const stream = new FakeTelemetryStream();
            streams.push(stream);
            Promise.resolve().then(() => stream.receive(new TelemetryCommand().setSettings(buildSettings({
              subscription: new Subscription().setFifo(fifo),
            }))));
            return stream.asGrpcStream();
          });
          await client.startup();
          assert.ok((client as LitePushConsumerImpl).getConsumeService() instanceof
            (fifo ? LiteFifoConsumeService : LiteStandardConsumeService));
        });
      });

      it('uses assignments like Java, receives and acks through a real ProcessQueue, then releases it', async () => {
        const push = client as LitePushConsumerImpl;
        const pending = deferred<ReceiveMessageResponse[]>();
        rpc.queryAssignment.resolves(new QueryAssignmentResponse().setStatus(buildStatus())
          .setAssignmentsList([ new Assignment().setMessageQueue(route.messageQueues[0].toProtobuf()) ]));
        rpc.receiveMessage.onFirstCall().resolves([
          new ReceiveMessageResponse().setStatus(buildStatus()),
          new ReceiveMessageResponse().setMessage(buildMessagePB({ topic: 'parent', liteTopic: 'one' })),
        ]);
        rpc.receiveMessage.onSecondCall().callsFake(() => pending.promise);
        rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
        await push.startup();
        await push.subscribeLite('one');
        await clock.tickAsync(1000);
        sinon.assert.calledOnce(rpc.queryAssignment);
        assert.strictEqual(push.getQueueSize(), 1);
        assert.strictEqual(rpc.receiveMessage.callCount, 2);
        sinon.assert.calledOnce(rpc.ackMessage);
        assert.strictEqual(rpc.receiveMessage.firstCall.args[1].getAutoRenew(), true);
        assert.strictEqual(rpc.ackMessage.firstCall.args[1].getEntriesList()[0].getLiteTopic(), 'one');
        assert.strictEqual(push.processQueues()[0].cachedMessagesCount(), 0);
        const shutdown = push.shutdown();
        pending.resolve([]);
        await clock.tickAsync(2000);
        await shutdown;
        assert.strictEqual(push.getQueueSize(), 0);
        const receiveCount = rpc.receiveMessage.callCount;
        await clock.tickAsync(30000);
        assert.strictEqual(rpc.receiveMessage.callCount, receiveCount);
        assert.strictEqual(clock.countTimers(), 0);
      });
    }
  });
}
