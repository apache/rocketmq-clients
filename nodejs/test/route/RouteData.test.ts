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
import { MessageType, Permission, MessageQueue as MessageQueuePB } from '../../proto/apache/rocketmq/v2/definition_pb';
import { Broker, Endpoints, MessageQueue, Resource, TopicRouteData } from '../../src/route';
import { buildRoute } from '../support/protobuf';
import { NotFoundException } from '../../src/exception';

describe('Route 数据模型与 namespace', () => {
  for (const namespace of [ '', 'tenant-a', '中文租户' ]) {
    it(`Resource(${namespace}) 保留构造参数、namespace、相等与 protobuf 往返`, () => {
      const resource = new Resource(namespace, 'topic');
      const copy = Resource.fromProtobuf(resource.toProtobuf());
      assert.equal(copy.getNamespace(), namespace);
      assert.equal(copy.getName(), 'topic');
      assert(resource.equals(resource));
      assert(resource.equals(copy));
      assert.equal(resource.hashCode(), copy.hashCode());
      assert.equal(resource.equals(new Resource('other', 'topic')), false);
      assert.equal(resource.equals(new Resource(namespace, 'other')), false);
      assert.equal(resource.equals(undefined as unknown as Resource), false);
      assert.equal(resource.toString(), namespace ? `Resource{name='topic', namespace='${namespace}'}` : 'topic');
    });
  }
  it('Resource 单参数构造保留默认 namespace', () => assert(new Resource('topic').equals(new Resource('', 'topic'))));

  for (const namespace of [ '', 'tenant-a' ]) {
    it(`MessageQueue(${namespace}) protobuf 往返保留完整路由`, () => {
      const queue = buildRoute({ namespace, queuesPerBroker: 3 }).messageQueues[2];
      queue.acceptMessageTypesList = [ MessageType.NORMAL, MessageType.FIFO ];
      const copy = new MessageQueue(MessageQueuePB.deserializeBinary(queue.toProtobuf().serializeBinary()));
      assert.deepEqual(copy.topic, queue.topic);
      assert.equal(copy.topic.resourceNamespace, namespace);
      assert.equal(copy.queueId, 2);
      assert.equal(copy.permission, Permission.READ_WRITE);
      assert.deepEqual(copy.acceptMessageTypesList, queue.acceptMessageTypesList);
      assert(queue.equals(queue));
      assert(queue.equals(copy));
      assert.equal(copy.hashCode(), queue.hashCode());
      assert.deepEqual(copy.toProtobuf().toObject(), queue.toProtobuf().toObject());
      assert.equal(queue.equals(undefined as unknown as MessageQueue), false);
    });
  }

  for (const field of [ 'namespace', 'queueId', 'permission', 'brokerId', 'topic' ]) {
    it(`MessageQueue 的 ${field} 不同则不相等`, () => {
      const queue = buildRoute({ namespace: 'tenant-a' }).messageQueues[0];
      const other = new MessageQueue(queue.toProtobuf());
      switch (field) {
        case 'namespace': other.topic.resourceNamespace = 'tenant-b'; break;
        case 'queueId': other.queueId++; break;
        case 'permission': other.permission = Permission.READ; break;
        case 'brokerId': other.broker.id++; break;
        case 'topic': other.topic.name = 'other-topic'; break;
        default: assert.fail(`未知字段 ${field}`);
      }
      assert.equal(queue.equals(other), false);
      assert.notEqual(queue.hashCode(), other.hashCode());
    });
  }

  it('空 namespace 保留基点的 MessageQueue hashCode 数值', () => {
    assert.equal(buildRoute().messageQueues[0].hashCode(), -1085438236);
  });

  it('Broker 以 name、id、endpoints 比较并往返', () => {
    const broker = buildRoute().messageQueues[0].broker;
    const copy = new Broker(broker.toProtobuf().toObject());
    assert(broker.equals(broker));
    assert(broker.equals(copy));
    assert.equal(broker.hashCode(), copy.hashCode());
    assert.equal(broker.equals(undefined as unknown as Broker), false);
    for (const change of [
      (b: Broker) => { b.id++; }, (b: Broker) => { b.name = 'other'; },
      (b: Broker) => { b.endpoints = new Endpoints('127.0.0.2:8081'); },
    ]) {
      const other = new Broker(broker.toProtobuf().toObject());
      change(other);
      assert.equal(broker.equals(other), false);
    }
  });

  it('分配查询轮转回归：跳过非 master 和 NONE 并循环使用合法端点', () => {
    const route = buildRoute({ brokers: [
      { name: 'slave', id: 1, endpoints: '127.0.0.1:80' },
      { name: 'none', endpoints: '127.0.0.2:80' },
      { name: 'a', endpoints: '127.0.0.3:80' },
      { name: 'b', endpoints: '127.0.0.4:80' },
    ] });
    route.messageQueues[1].permission = Permission.NONE;
    const endpoints = Array.from({ length: 8 }, () => route.pickEndpointsToQueryAssignments().facade);
    assert.deepEqual(new Set(endpoints), new Set([ '127.0.0.3:80', '127.0.0.4:80' ]));
    for (let i = 1; i < endpoints.length; i++) assert.notEqual(endpoints[i], endpoints[i - 1]);
  });

  it('分配查询轮转回归：没有合法端点时抛 NotFoundException', () => {
    for (const route of [ new TopicRouteData([]), buildRoute({ brokerId: 1 }), buildRoute({ permission: Permission.NONE }) ]) {
      assert.throws(() => route.pickEndpointsToQueryAssignments(), NotFoundException);
    }
  });

  it('分配查询保持 Java 对 WRITE 权限 master 的兼容性', () => {
    const route = buildRoute({ permission: Permission.WRITE });
    assert.equal(route.pickEndpointsToQueryAssignments(), route.messageQueues[0].broker.endpoints);
  });

  it('TopicRouteData 去重 endpoint、保留队列顺序和 equals/hashCode', () => {
    const route = buildRoute({ queuesPerBroker: 2, brokers: [
      { name: 'a', endpoints: '127.0.0.1:80' }, { name: 'b', endpoints: '127.0.0.2:80' },
      { name: 'c', endpoints: '127.0.0.1:80' },
    ] });
    const copy = new TopicRouteData(route.messageQueues.map(q => q.toProtobuf()));
    assert.equal(route.getQueueCount(), 6);
    assert.equal(route.isEmpty(), false);
    assert.deepEqual(route.getTotalEndpoints().map(e => e.facade), [ '127.0.0.1:80', '127.0.0.2:80' ]);
    assert(route.equals(route));
    assert(route.equals(copy));
    assert.equal(route.hashCode(), copy.hashCode());
    assert.equal(route.equals(undefined as unknown as TopicRouteData), false);
    assert.equal(route.equals(new TopicRouteData([])), false);
    assert.equal(route.equals(new TopicRouteData(route.messageQueues.map(q => q.toProtobuf()).reverse())), false);
    const empty = new TopicRouteData([]);
    assert(empty.isEmpty());
    assert.equal(empty.getQueueCount(), 0);
    assert.deepEqual(empty.getTotalEndpoints(), []);
    assert(empty.equals(new TopicRouteData([])));
  });
});
