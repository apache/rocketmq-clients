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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { SubscriptionLoadBalancer } from '../../src/consumer';
import { TopicRouteData } from '../../src/route';
import {
  MessageQueue as MessageQueuePB,
  Broker as BrokerPB,
  Resource as ResourcePB,
  Endpoints as EndpointsPB,
  Permission,
  AddressScheme,
} from '../../proto/apache/rocketmq/v2/definition_pb';

describe('SubscriptionLoadBalancer keeps the readable queues of master brokers', () => {
  // The proxy lists the queues of every broker (master and slaves) and numbers them
  // read-only first, then write-only, then read-write.
  function routeOf(...queues: [ brokerId: number, queueId: number, permission: Permission ][]) {
    return new TopicRouteData(queues.map(([ brokerId, queueId, permission ]) => {
      const endpointsPb = new EndpointsPB();
      endpointsPb.setScheme(AddressScheme.IPV4);
      endpointsPb.addAddresses().setHost('127.0.0.1').setPort(8081 + brokerId);
      const brokerPb = new BrokerPB();
      brokerPb.setName('broker-a');
      brokerPb.setId(brokerId);
      brokerPb.setEndpoints(endpointsPb);
      const mqPb = new MessageQueuePB();
      mqPb.setId(queueId);
      mqPb.setTopic(new ResourcePB().setName('topic'));
      mqPb.setBroker(brokerPb);
      mqPb.setPermission(permission);
      return mqPb;
    }));
  }

  function pickedQueues(loadBalancer: SubscriptionLoadBalancer, times: number) {
    const picked = new Set<string>();
    for (let i = 0; i < times; i++) {
      const mq = loadBalancer.takeMessageQueue();
      picked.add(mq.broker.id + '/' + mq.queueId);
    }
    return Array.from(picked).sort();
  }

  it('should select readable queues whatever their queue id', () => {
    // writeQueueNums=4, readQueueNums=2: queues 0 and 1 are write-only.
    const loadBalancer = new SubscriptionLoadBalancer(routeOf(
      [ 0, 0, Permission.WRITE ], [ 0, 1, Permission.WRITE ],
      [ 0, 2, Permission.READ_WRITE ], [ 0, 3, Permission.READ_WRITE ],
    ));
    assert.deepStrictEqual(pickedQueues(loadBalancer, 8), [ '0/2', '0/3' ]);
  });

  it('should not select queues of slave brokers', () => {
    const loadBalancer = new SubscriptionLoadBalancer(routeOf(
      [ 0, 0, Permission.READ_WRITE ], [ 0, 1, Permission.READ_WRITE ],
      [ 1, 0, Permission.READ_WRITE ], [ 1, 1, Permission.READ_WRITE ],
    ));
    assert.deepStrictEqual(pickedQueues(loadBalancer, 8), [ '0/0', '0/1' ]);
  });

  it('should reject a route without readable master queue', () => {
    assert.throws(() => new SubscriptionLoadBalancer(routeOf(
      [ 0, 0, Permission.WRITE ], [ 1, 0, Permission.READ_WRITE ],
    )), /No readable message queue found/);
  });
});
