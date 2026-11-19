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
import type { Resource } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageRequest, AckMessageResponse, ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse,
  QueryAssignmentRequest, QueryAssignmentResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { PushConsumer } from '../../src/consumer/PushConsumer';
import { SimpleConsumer } from '../../src/consumer/SimpleConsumer';
import { MessageView } from '../../src/message';
import { MessageQueue } from '../../src/route';
import { createSilentLogger, installFakeRpcClientManager } from '../support/fakes';
import { buildMessagePB, buildRoute, buildStatus } from '../support/protobuf';

const NAMESPACE = 'ns';
const TOPIC = 'test-topic';
const GROUP = 'test-group';
const queue = buildRoute({ topic: TOPIC }).messageQueues[0];
const message = new MessageView(buildMessagePB({ topic: TOPIC }), queue);

function assertResource(resource: Resource | undefined, name: string) {
  assert.ok(resource);
  assert.equal(resource.getName(), name);
  assert.equal(resource.getResourceNamespace(), NAMESPACE);
}

class TestSimpleConsumer extends SimpleConsumer {
  receiveRequest(mq: MessageQueue) {
    return this.wrapReceiveMessageRequest(1, mq, FilterExpression.SUB_ALL, 15000, 30000);
  }
  heartbeatRequest() { return this.wrapHeartbeatRequest(); }
  terminationRequest() { return this.wrapNotifyClientTerminationRequest(); }
  settingsProto() { return this.getSettings().toProtobuf(); }
  ackRpc() { return this.ackMessage(message); }
  invisibleRpc() { return this.invisibleDuration(message, 15000); }
}

class TestPushConsumer extends PushConsumer {
  heartbeatRequest() { return this.wrapHeartbeatRequest(); }
  terminationRequest() { return this.wrapNotifyClientTerminationRequest(); }
  settingsProto() { return this.getSettings().toProtobuf(); }
  cacheRoute() { this.topicRouteCache.set(TOPIC, buildRoute({ topic: TOPIC })); }
  scan() { this.startAssignmentScanning(); }
}

// Java ConsumerImpl / PushConsumerImpl / *SubscriptionSettings carry the client namespace on every resource.
describe('Consumer resource namespace', () => {
  afterEach(() => {
    sinon.restore();
  });

  describe('SimpleConsumer', () => {
    let consumer: TestSimpleConsumer;
    let rpc: ReturnType<typeof installFakeRpcClientManager>;

    beforeEach(() => {
      consumer = new TestSimpleConsumer({ endpoints: '127.0.0.1:8081', namespace: NAMESPACE, consumerGroup: GROUP,
        subscriptions: new Map([[ TOPIC, FilterExpression.SUB_ALL ]]), logger: createSilentLogger() });
      rpc = installFakeRpcClientManager(consumer);
    });

    it('sets the namespace on the receive, heartbeat and termination groups', () => {
      assertResource(consumer.receiveRequest(queue).getGroup(), GROUP);
      assertResource(consumer.heartbeatRequest().getGroup(), GROUP);
      assertResource(consumer.terminationRequest().getGroup(), GROUP);
    });

    it('sets the namespace on the subscription settings', () => {
      const subscription = consumer.settingsProto().getSubscription()!;
      assertResource(subscription.getGroup(), GROUP);
      assertResource(subscription.getSubscriptionsList()[0].getTopic(), TOPIC);
    });

    it('sets the namespace on the ack and change invisible duration requests', async () => {
      rpc.ackMessage.resolves(new AckMessageResponse().setStatus(buildStatus()));
      rpc.changeInvisibleDuration.resolves(new ChangeInvisibleDurationResponse().setStatus(buildStatus()));

      await consumer.ackRpc();
      await consumer.invisibleRpc();

      const ack = rpc.ackMessage.firstCall.args[1] as AckMessageRequest;
      assertResource(ack.getGroup(), GROUP);
      assertResource(ack.getTopic(), TOPIC);
      const invisible = rpc.changeInvisibleDuration.firstCall.args[1] as ChangeInvisibleDurationRequest;
      assertResource(invisible.getGroup(), GROUP);
      assertResource(invisible.getTopic(), TOPIC);
    });
  });

  describe('PushConsumer', () => {
    let consumer: TestPushConsumer;
    let rpc: ReturnType<typeof installFakeRpcClientManager>;

    beforeEach(() => {
      consumer = new TestPushConsumer({ endpoints: '127.0.0.1:8081', namespace: NAMESPACE, consumerGroup: GROUP,
        subscriptions: new Map([[ TOPIC, FilterExpression.SUB_ALL ]]), logger: createSilentLogger(),
        messageListener: { consume: () => ConsumeResult.SUCCESS } });
      rpc = installFakeRpcClientManager(consumer);
    });

    it('sets the namespace on the receive and settlement requests', () => {
      assertResource(consumer.wrapPushReceiveMessageRequest(1, queue, FilterExpression.SUB_ALL, 30000).getGroup(), GROUP);
      for (const request of [
        consumer.wrapAckMessageRequest(message),
        consumer.wrapChangeInvisibleDurationRequest(message, 15000),
        consumer.wrapForwardMessageToDeadLetterQueueRequest(message),
      ]) {
        assertResource(request.getGroup(), GROUP);
        assertResource(request.getTopic(), TOPIC);
      }
    });

    it('sets the namespace on the heartbeat, termination and settings resources', () => {
      assertResource(consumer.heartbeatRequest().getGroup(), GROUP);
      assertResource(consumer.terminationRequest().getGroup(), GROUP);
      const subscription = consumer.settingsProto().getSubscription()!;
      assertResource(subscription.getGroup(), GROUP);
      assertResource(subscription.getSubscriptionsList()[0].getTopic(), TOPIC);
    });

    it('sets the namespace on the query assignment request', async () => {
      const clock = sinon.useFakeTimers({ toFake: [ 'setInterval', 'clearInterval', 'setTimeout', 'clearTimeout' ] });
      try {
        consumer.cacheRoute();
        rpc.queryAssignment.resolves(new QueryAssignmentResponse().setStatus(buildStatus()));

        consumer.scan();
        await clock.tickAsync(1000);
      } finally {
        clock.restore();
      }

      const request = rpc.queryAssignment.firstCall.args[1] as QueryAssignmentRequest;
      assertResource(request.getGroup(), GROUP);
      assertResource(request.getTopic(), TOPIC);
    });
  });
});
