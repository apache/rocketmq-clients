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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { PublishingLoadBalancer } from '../../src/producer';
import { TopicRouteData } from '../../src/route';
import { calculateStringSipHash24 } from '../../src/util';
import {
  MessageQueue as MessageQueuePB,
  Broker as BrokerPB,
  Resource as ResourcePB,
  Endpoints as EndpointsPB,
  Permission,
  AddressScheme,
} from '../../proto/apache/rocketmq/v2/definition_pb';

describe('takeMessageQueueByMessageGroup floorMod semantics (C-1)', () => {
  // One queue (queueId=0, the writable master queue) per broker across queueCount brokers,
  // since PublishingLoadBalancer only keeps queues with queueId === MASTER_BROKER_ID (0).
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
    const groups = ['group-a', 'fifo-group', 'group-中文', 'x', 'order-12345', ''];
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
