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
 * Regression tests for the FIFO message-group routing fix (C-1):
 * SipHash-2-4 must be interpreted as a signed int64 and combined with
 * floorMod, matching Java LongMath.mod, otherwise "negative" hashes are
 * routed to different queues than the Java client.
 */

import * as assert from 'node:assert';
import { PublishingLoadBalancer } from '../../src/producer';
import { Endpoints, MessageQueue, TopicRouteData } from '../../src/route';
import { calculateStringSipHash24 } from '../../src/util';
import {
  MessageQueue as MessageQueuePB,
  Broker as BrokerPB,
  Resource as ResourcePB,
  Endpoints as EndpointsPB,
  Permission,
  AddressScheme,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { buildRoute } from '../support/protobuf';

describe('takeMessageQueueByMessageGroup floorMod semantics (C-1)', () => {
  // One writable master queue per broker across queueCount brokers.
  // The returned queue is identified by its broker endpoints port (10911 + expected index).
  function buildLoadBalancer(queueCount: number): PublishingLoadBalancer {
    const pbs: MessageQueuePB[] = [];
    for (let i = 0; i < queueCount; i++) {
      const endpointsPb = new EndpointsPB();
      endpointsPb.setScheme(AddressScheme.IPV4);
      endpointsPb.addAddresses().setHost('127.0.0.1').setPort(10911 + i);
      const brokerPb = new BrokerPB();
      brokerPb.setName('broker-' + i);
      brokerPb.setId(0);
      brokerPb.setEndpoints(endpointsPb);
      const mqPb = new MessageQueuePB();
      mqPb.setId(0);
      mqPb.setTopic(new ResourcePB().setName('topic'));
      mqPb.setBroker(brokerPb);
      mqPb.setPermission(Permission.READ_WRITE);
      pbs.push(mqPb);
    }
    return new PublishingLoadBalancer(new TopicRouteData(pbs));
  }

  it('should always yield a non-negative index matching Java LongMath.mod', () => {
    const loadBalancer = buildLoadBalancer(8);
    const groups = [ 'group-a', 'fifo-group', 'group-中文', 'x', 'order-12345', '' ];
    for (const group of groups) {
      const mq = loadBalancer.takeMessageQueueByMessageGroup(group);
      assert.ok(mq, 'should resolve a message queue for group=' + group);
      // Expected index under floorMod semantics: ((signed hash % size) + size) % size
      const signedHash = BigInt.asIntN(64, calculateStringSipHash24(group));
      const expectedIndex = Number(((signedHash % 8n) + 8n) % 8n);
      assert.strictEqual(mq.broker.endpoints.facade, '127.0.0.1:' + (10911 + expectedIndex));
    }
  });

  it('should match Java floorMod for hashes that are negative when interpreted signed', () => {
    // Find a message group whose SipHash-2-4 has its high bit set (negative as int64)
    let negativeGroup: string | undefined;
    for (let i = 0; i < 10000; i++) {
      const candidate = 'probe-' + i;
      if (BigInt.asIntN(64, calculateStringSipHash24(candidate)) < 0n) {
        negativeGroup = candidate;
        break;
      }
    }
    assert.ok(negativeGroup, 'expected to find a negative hash among probes');
    const loadBalancer = buildLoadBalancer(4);
    const mq = loadBalancer.takeMessageQueueByMessageGroup(negativeGroup!);
    const signedHash = BigInt.asIntN(64, calculateStringSipHash24(negativeGroup!));
    const expectedIndex = Number(((signedHash % 4n) + 4n) % 4n);
    assert.strictEqual(mq.broker.endpoints.facade, '127.0.0.1:' + (10911 + expectedIndex));
  });
});

describe('PublishingLoadBalancer queue selection', () => {
  const TWO_BROKERS = [
    { name: 'broker-a', endpoints: '127.0.0.1:8081' },
    { name: 'broker-b', endpoints: '127.0.0.1:8082' },
  ];

  function queuePB(brokerName: string, brokerId: number, port: number, queueId: number, permission: Permission) {
    return new MessageQueuePB().setId(queueId)
      .setTopic(new ResourcePB().setName('test-topic'))
      .setBroker(new BrokerPB().setName(brokerName).setId(brokerId)
        .setEndpoints(new Endpoints(`127.0.0.1:${port}`).toProtobuf()))
      .setPermission(permission);
  }

  function key(mq: MessageQueue) {
    return `${mq.broker.name}#${mq.broker.id}#${mq.queueId}`;
  }

  function rotate(loadBalancer: PublishingLoadBalancer, times: number) {
    const picked = new Set<string>();
    for (let i = 0; i < times; i++) {
      picked.add(key(loadBalancer.takeMessageQueues(new Map(), 1)[0]));
    }
    return picked;
  }

  it('rotates over every writable queue of every master broker', () => {
    const loadBalancer = new PublishingLoadBalancer(buildRoute({ brokers: TWO_BROKERS, queuesPerBroker: 4 }), 0);
    const picked = rotate(loadBalancer, 8);
    assert.strictEqual(picked.size, 8, `only picked ${JSON.stringify([ ...picked ])}`);
  });

  it('spreads FIFO message groups over every master queue', () => {
    const loadBalancer = new PublishingLoadBalancer(buildRoute({ brokers: TWO_BROKERS, queuesPerBroker: 4 }));
    const picked = new Set<string>();
    for (let i = 0; i < 200; i++) {
      picked.add(key(loadBalancer.takeMessageQueueByMessageGroup(`group-${i}`)));
    }
    assert.strictEqual(picked.size, 8, `only picked ${JSON.stringify([ ...picked ])}`);
  });

  it('excludes queues of slave brokers by broker id', () => {
    const loadBalancer = new PublishingLoadBalancer(buildRoute({
      queuesPerBroker: 2,
      brokers: [
        { name: 'broker-a', id: 0, endpoints: '127.0.0.1:8081' },
        { name: 'broker-a', id: 1, endpoints: '127.0.0.1:8082' },
      ],
    }), 0);
    assert.deepStrictEqual([ ...rotate(loadBalancer, 4) ].sort(), [ 'broker-a#0#0', 'broker-a#0#1' ]);
  });

  it('keeps only writable queues', () => {
    const loadBalancer = new PublishingLoadBalancer(new TopicRouteData([
      queuePB('broker-a', 0, 8081, 0, Permission.READ),
      queuePB('broker-a', 0, 8081, 1, Permission.WRITE),
      queuePB('broker-a', 0, 8081, 2, Permission.READ_WRITE),
      queuePB('broker-a', 0, 8081, 3, Permission.NONE),
    ]), 0);
    assert.deepStrictEqual([ ...rotate(loadBalancer, 4) ].sort(), [ 'broker-a#0#1', 'broker-a#0#2' ]);
  });

  it('throws when no writable master queue exists', () => {
    assert.throws(() => new PublishingLoadBalancer(buildRoute({ permission: Permission.READ })), /No writable message queue found/);
    assert.throws(() => new PublishingLoadBalancer(buildRoute({ brokerId: 1 })), /No writable message queue found/);
    assert.throws(() => new PublishingLoadBalancer(buildRoute({ brokers: [] })), /No writable message queue found/);
  });

  it('takes one queue per broker and skips isolated endpoints', () => {
    const brokers = [ ...TWO_BROKERS, { name: 'broker-c', endpoints: '127.0.0.1:8083' }];
    const loadBalancer = new PublishingLoadBalancer(buildRoute({ brokers, queuesPerBroker: 2 }), 0);
    const candidates = loadBalancer.takeMessageQueues(new Map(), 3);
    assert.deepStrictEqual(candidates.map(mq => mq.broker.name).sort(), [ 'broker-a', 'broker-b', 'broker-c' ]);

    const isolated = new Map([[ '127.0.0.1:8082', new Endpoints('127.0.0.1:8082') ]]);
    const healthy = loadBalancer.takeMessageQueues(isolated, 3);
    assert.deepStrictEqual(healthy.map(mq => mq.broker.name).sort(), [ 'broker-a', 'broker-c' ]);
  });

  it('falls back to isolated endpoints when every endpoint is isolated', () => {
    const loadBalancer = new PublishingLoadBalancer(buildRoute({ brokers: TWO_BROKERS }), 0);
    const isolated = new Map(TWO_BROKERS.map(b => [ b.endpoints, new Endpoints(b.endpoints) ]));
    const candidates = loadBalancer.takeMessageQueues(isolated, 2);
    assert.deepStrictEqual(candidates.map(mq => mq.broker.name).sort(), [ 'broker-a', 'broker-b' ]);
  });

  it('keeps the rotation position across route updates', () => {
    const route = buildRoute({ brokers: TWO_BROKERS, queuesPerBroker: 2 });
    const loadBalancer = new PublishingLoadBalancer(route, 0);
    loadBalancer.takeMessageQueues(new Map(), 1);
    const updated = loadBalancer.update(route);
    const expected = new PublishingLoadBalancer(route, 1).takeMessageQueues(new Map(), 1)[0];
    assert.ok(updated.takeMessageQueues(new Map(), 1)[0].equals(expected));
    assert.ok(updated.equals(loadBalancer));
  });

  it('routes the same message group to the same queue', () => {
    const route = buildRoute({ brokers: TWO_BROKERS, queuesPerBroker: 4 });
    const loadBalancer = new PublishingLoadBalancer(route);
    const first = loadBalancer.takeMessageQueueByMessageGroup('order-1');
    for (let i = 0; i < 10; i++) {
      assert.ok(loadBalancer.takeMessageQueueByMessageGroup('order-1').equals(first));
    }
    assert.ok(loadBalancer.update(route).takeMessageQueueByMessageGroup('order-1').equals(first));
  });
});
