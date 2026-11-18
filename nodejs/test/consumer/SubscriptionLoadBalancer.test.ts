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
import { Permission } from '../../proto/apache/rocketmq/v2/definition_pb';
import { SubscriptionLoadBalancer } from '../../src/consumer/SubscriptionLoadBalancer';
import { buildRoute } from '../support/protobuf';

const brokers = [
  { name: 'a', id: 0, endpoints: '127.0.0.1:8081' },
  { name: 'a', id: 1, endpoints: '127.0.0.1:8082' },
  { name: 'b', id: 0, endpoints: '127.0.0.1:8083' },
];

describe('SubscriptionLoadBalancer', () => {
  it('轮询所有 master 可读队列，排除 slave 的 0 号队列', () => {
    const route = buildRoute({ brokers, queuesPerBroker: 3 });
    const balancer = new SubscriptionLoadBalancer(route, 0);
    const selected = Array.from({ length: 6 }, () => balancer.takeMessageQueue());
    assert.deepStrictEqual(selected.map(mq => [ mq.broker.name, mq.broker.id, mq.queueId ]), [
      [ 'a', 0, 0 ], [ 'a', 0, 1 ], [ 'a', 0, 2 ],
      [ 'b', 0, 0 ], [ 'b', 0, 1 ], [ 'b', 0, 2 ],
    ]);
    assert.strictEqual(balancer.takeMessageQueue(), selected[0]);
  });

  it('接受 READ 和 READ_WRITE，排除 WRITE 和 NONE', () => {
    const route = buildRoute({ queuesPerBroker: 4 });
    [ Permission.READ, Permission.WRITE, Permission.READ_WRITE, Permission.NONE ].forEach((permission, i) => {
      route.messageQueues[i] = buildRoute({ permission }).messageQueues[0];
    });
    const balancer = new SubscriptionLoadBalancer(route, 0);
    assert.strictEqual(balancer.takeMessageQueue(), route.messageQueues[0]);
    assert.strictEqual(balancer.takeMessageQueue(), route.messageQueues[2]);
    assert.strictEqual(balancer.takeMessageQueue(), route.messageQueues[0]);
  });

  for (const route of [
    buildRoute({ brokers: [] }),
    buildRoute({ brokerId: 1 }),
    buildRoute({ permission: Permission.WRITE }),
    buildRoute({ permission: Permission.NONE }),
  ]) {
    it('无可读 master 队列时抛出明确错误（含默认随机起点）', () => {
      assert.throws(() => new SubscriptionLoadBalancer(route), /No readable message queue found/);
      assert.throws(() => new SubscriptionLoadBalancer(route, 0), /No readable message queue found/);
    });
  }

  it('默认随机起点只产生可读 master 队列', () => {
    const route = buildRoute({ brokers, queuesPerBroker: 3 });
    const balancer = new SubscriptionLoadBalancer(route);
    const selected = Array.from({ length: 6 }, () => balancer.takeMessageQueue());
    assert.strictEqual(new Set(selected).size, 6);
    assert.ok(selected.every(mq => mq.broker.id === 0));
  });

  it('update 保留轮询位置，并使用新路由', () => {
    const balancer = new SubscriptionLoadBalancer(buildRoute({ queuesPerBroker: 3 }), 0);
    assert.strictEqual(balancer.takeMessageQueue().queueId, 0);
    const route = buildRoute({ topic: 'updated', queuesPerBroker: 3 });
    const updated = balancer.update(route);
    assert.strictEqual(updated.takeMessageQueue(), route.messageQueues[1]);
    assert.strictEqual(updated.takeMessageQueue(), route.messageQueues[2]);
    assert.strictEqual(updated.takeMessageQueue(), route.messageQueues[0]);
  });

  it('update 后位置超出缩短的路由时回到起点', () => {
    const balancer = new SubscriptionLoadBalancer(buildRoute({ queuesPerBroker: 3 }), 2);
    const route = buildRoute();
    assert.strictEqual(balancer.update(route).takeMessageQueue(), route.messageQueues[0]);
  });
});
