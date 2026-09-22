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
 * Offline tests for LiteSimpleConsumer (builder, impl, route filter) and
 * LiteSubscriptionManager. No broker required.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import {
  Broker as BrokerPB,
  ClientType,
  LiteSubscriptionAction,
  MessageQueue as MessageQueuePB,
  Permission,
  Settings as SettingsPB,
  Status,
  Code,
  Subscription as SubscriptionPB,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  NotifyUnsubscribeLiteCommand,
  SyncLiteSubscriptionResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { createResource } from '../../src/util';
import { Endpoints, Resource, TopicRouteData } from '../../src/route';
import { LiteSimpleConsumerBuilder } from '../../src/consumer/LiteSimpleConsumer';
import {
  LiteSimpleConsumerImpl,
  firstReadableMasterQueue,
} from '../../src/consumer/LiteSimpleConsumerImpl';
import {
  LiteSubscriptionManager,
  LiteSubscriptionHost,
} from '../../src/consumer/LiteSubscriptionManager';
import { OffsetOption } from '../../src/consumer/OffsetOption';

const ENDPOINTS = '127.0.0.1:8081';

function buildProtoQueue(id: number, brokerId: number, permission: Permission): MessageQueuePB {
  const broker = new BrokerPB()
    .setName(`broker-${brokerId}`)
    .setId(brokerId)
    .setEndpoints(new Endpoints('127.0.0.1:10911').toProtobuf());
  return new MessageQueuePB()
    .setId(id)
    .setTopic(createResource('lite-parent-topic'))
    .setBroker(broker)
    .setPermission(permission);
}

interface SyncRecord {
  endpoints: Endpoints;
  action: LiteSubscriptionAction;
  liteTopics: string[];
}

class FakeHost implements LiteSubscriptionHost {
  clientId = 'fake-client-id';
  running = true;
  quota: number | null = null;
  maxLiteTopicSize: number | null = null;
  readonly endpointsList = [ new Endpoints('127.0.0.1:8081'), new Endpoints('127.0.0.1:8082') ];
  readonly syncRecords: SyncRecord[] = [];
  readonly logger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    debug: () => undefined,
  };

  isRunning(): boolean {
    return this.running;
  }

  getLogger() {
    return this.logger;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getRpcClientManager(): any {
    return {
      syncLiteSubscription: async (endpoints: Endpoints, request: any) => {
        this.syncRecords.push({
          endpoints,
          action: request.getAction(),
          liteTopics: request.getLiteTopicSetList(),
        });
        return new SyncLiteSubscriptionResponse().setStatus(new Status().setCode(Code.OK));
      },
    };
  }

  getRequestTimeout(): number {
    return 3000;
  }

  getSyncEndpoints(): Endpoints[] {
    return this.endpointsList;
  }

  resetRecords() {
    this.syncRecords.length = 0;
  }
}

describe('test/consumer/LiteSimpleConsumer.test.ts', () => {

  describe('LiteSimpleConsumerBuilder', () => {
    it('should chain bindTopic and validate blank topic', () => {
      const builder = new LiteSimpleConsumerBuilder();
      assert.strictEqual(builder.bindTopic('test-bind-topic'), builder);
      assert.throws(() => builder.bindTopic(''), /bindTopic should not be blank/);
      assert.throws(() => builder.bindTopic('   '), /bindTopic should not be blank/);
    });

    it('should validate consumer group', () => {
      const builder = new LiteSimpleConsumerBuilder();
      assert.throws(() => builder.setConsumerGroup(null as any), /consumerGroup should not be null/);
      assert.throws(() => builder.setConsumerGroup('invalid group!'), /does not match the pattern/);
      assert.strictEqual(builder.setConsumerGroup('valid-group_1'), builder);
    });

    it('should validate await duration', () => {
      const builder = new LiteSimpleConsumerBuilder();
      assert.throws(() => builder.setAwaitDuration(0), /awaitDuration should be positive/);
      assert.throws(() => builder.setAwaitDuration(-1), /awaitDuration should be positive/);
      assert.strictEqual(builder.setAwaitDuration(5000), builder);
    });

    it('should fail to build without required options', async () => {
      await assert.rejects(async () => {
        await new LiteSimpleConsumerBuilder().build();
      }, /clientConfiguration has not been set yet/);

      await assert.rejects(async () => {
        await new LiteSimpleConsumerBuilder().setClientConfiguration({ endpoints: ENDPOINTS, namespace: '' }).build();
      }, /consumerGroup has not been set yet/);

      await assert.rejects(async () => {
        await new LiteSimpleConsumerBuilder()
          .setClientConfiguration({ endpoints: ENDPOINTS, namespace: '' })
          .setConsumerGroup('valid-group')
          .build();
      }, /bindTopic has not been set yet/);
    });
  });

  describe('LiteSimpleConsumerImpl constructor', () => {
    const baseOptions = {
      endpoints: ENDPOINTS,
      namespace: '',
      consumerGroup: 'lite-unittest-group',
      bindTopic: 'lite-parent-topic',
    };

    it('should reject blank bindTopic', () => {
      assert.throws(() => {
        new LiteSimpleConsumerImpl({ ...baseOptions, bindTopic: ' ' });
      }, /bindTopic should not be blank/);
    });

    it('should use LITE_SIMPLE_CONSUMER client type', () => {
      const impl = new LiteSimpleConsumerImpl(baseOptions);
      assert.strictEqual((impl as any).getClientType(), ClientType.LITE_SIMPLE_CONSUMER);
    });

    it('should subscribe the bind topic with SUB_ALL by default', () => {
      const impl = new LiteSimpleConsumerImpl(baseOptions);
      const settings = (impl as any).getSettings();
      const protobuf = settings.toProtobuf();
      const subscription = protobuf.getSubscription()!;
      assert.strictEqual(subscription.getGroup()!.getName(), 'lite-unittest-group');
      const entries = subscription.getSubscriptionsList();
      assert.strictEqual(entries.length, 1);
      assert.strictEqual(entries[0].getTopic()!.getName(), 'lite-parent-topic');
      assert.strictEqual(entries[0].getExpression()!.getExpression(), '*');
      assert.strictEqual(protobuf.getClientType(), ClientType.LITE_SIMPLE_CONSUMER);
    });

    it('should expose group and empty lite topic set before startup', () => {
      const impl = new LiteSimpleConsumerImpl(baseOptions);
      assert.strictEqual(impl.getConsumerGroup(), 'lite-unittest-group');
      assert.strictEqual(impl.getLiteTopicSet().size, 0);
    });
  });

  describe('firstReadableMasterQueue', () => {
    it('should keep only the first readable master queue', () => {
      const routeData = new TopicRouteData([
        buildProtoQueue(0, 1, Permission.READ_WRITE), // slave readable
        buildProtoQueue(1, 0, Permission.WRITE), // master not readable
        buildProtoQueue(2, 0, Permission.READ_WRITE), // first readable master
        buildProtoQueue(3, 0, Permission.READ), // second readable master
      ]);

      const filtered = firstReadableMasterQueue(routeData);
      assert.strictEqual(filtered.messageQueues.length, 1);
      assert.strictEqual(filtered.messageQueues[0].broker.id, 0);
      assert.strictEqual(filtered.messageQueues[0].queueId, 2);
    });

    it('should return empty route when no readable master exists', () => {
      const routeData = new TopicRouteData([
        buildProtoQueue(0, 0, Permission.WRITE),
        buildProtoQueue(1, 1, Permission.READ),
      ]);

      const filtered = firstReadableMasterQueue(routeData);
      assert.strictEqual(filtered.messageQueues.length, 0);
    });
  });

  describe('LiteSubscriptionManager', () => {
    const bindTopic = 'lite-parent-topic';

    function createManager(host = new FakeHost()) {
      const manager = new LiteSubscriptionManager(
        host,
        new Resource('', bindTopic),
        new Resource('', 'lite-unittest-group'),
      );
      return { host, manager };
    }

    it('should sync PARTIAL_ADD to every sync endpoint on subscribeLite', async () => {
      const { host, manager } = createManager();
      await manager.subscribeLite('lite-topic-1', OffsetOption.MIN_OFFSET);

      assert.strictEqual(host.syncRecords.length, 2);
      for (const record of host.syncRecords) {
        assert.strictEqual(record.action, LiteSubscriptionAction.PARTIAL_ADD);
        assert.deepStrictEqual(record.liteTopics, [ 'lite-topic-1' ]);
      }
      assert.ok(host.syncRecords.some(r => r.endpoints.facade === '127.0.0.1:8081'));
      assert.ok(host.syncRecords.some(r => r.endpoints.facade === '127.0.0.1:8082'));
      assert.ok(manager.getLiteTopicSet().has('lite-topic-1'));
      assert.strictEqual(manager.getBindTopicName(), bindTopic);
      assert.strictEqual(manager.getConsumerGroupName(), 'lite-unittest-group');
    });

    it('should reject subscribeLite when not running', async () => {
      const { host, manager } = createManager();
      host.running = false;
      await assert.rejects(async () => {
        await manager.subscribeLite('lite-topic-1');
      }, /Consumer is not running/);
      assert.strictEqual(host.syncRecords.length, 0);
    });

    it('should skip duplicate subscribeLite without extra rpc', async () => {
      const { host, manager } = createManager();
      await manager.subscribeLite('lite-topic-1');
      host.resetRecords();
      await manager.subscribeLite('lite-topic-1');
      assert.strictEqual(host.syncRecords.length, 0);
      assert.strictEqual(manager.getLiteTopicSet().size, 1);
    });

    it('should validate lite topic name and length', async () => {
      const { manager } = createManager();
      await assert.rejects(async () => {
        await manager.subscribeLite('   ');
      }, /liteTopic is blank/);
      await assert.rejects(async () => {
        await manager.subscribeLite('x'.repeat(65));
      }, /liteTopic length exceeded max length 64/);
    });

    it('should throw when quota exceeded', async () => {
      const { manager } = createManager();
      const settings = new SettingsPB()
        .setSubscription(new SubscriptionPB().setLiteSubscriptionQuota(1));
      manager.sync(settings);
      await manager.subscribeLite('lite-topic-1');
      await assert.rejects(async () => {
        await manager.subscribeLite('lite-topic-2');
      }, /Lite subscription quota exceeded 1/);
      assert.strictEqual(manager.getLiteTopicSet().size, 1);
    });

    it('should adopt maxLiteTopicSize from settings', async () => {
      const { manager } = createManager();
      const settings = new SettingsPB()
        .setSubscription(new SubscriptionPB().setMaxLiteTopicSize(8));
      manager.sync(settings);
      await assert.rejects(async () => {
        await manager.subscribeLite('x'.repeat(9));
      }, /liteTopic length exceeded max length 8/);
    });

    it('should sync PARTIAL_REMOVE on unsubscribeLite', async () => {
      const { host, manager } = createManager();
      await manager.subscribeLite('lite-topic-1');
      host.resetRecords();

      await manager.unsubscribeLite('lite-topic-1');
      assert.strictEqual(host.syncRecords.length, 2);
      assert.ok(host.syncRecords.every(r => r.action === LiteSubscriptionAction.PARTIAL_REMOVE));
      assert.strictEqual(manager.getLiteTopicSet().size, 0);
    });

    it('should skip unsubscribeLite for unknown topic', async () => {
      const { host, manager } = createManager();
      await manager.unsubscribeLite('never-subscribed');
      assert.strictEqual(host.syncRecords.length, 0);
    });

    it('should fail subscription when any endpoint rejects the sync', async () => {
      const host = new FakeHost();
      const manager = new LiteSubscriptionManager(
        host,
        new Resource('', bindTopic),
        new Resource('', 'lite-unittest-group'),
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (host as any).getRpcClientManager = () => ({
        syncLiteSubscription: async (endpoints: Endpoints) => {
          if (endpoints.facade === '127.0.0.1:8081') {
            return new SyncLiteSubscriptionResponse().setStatus(
              new Status().setCode(Code.INTERNAL_SERVER_ERROR),
            );
          }
          return new SyncLiteSubscriptionResponse().setStatus(new Status().setCode(Code.OK));
        },
      });

      await assert.rejects(async () => {
        await manager.subscribeLite('lite-topic-1');
      }, /Failed to sync lite subscription/);
      // Failed sync must not pollute the local set
      assert.strictEqual(manager.getLiteTopicSet().size, 0);
    });

    it('should remove lite topic on notify unsubscribe command', async () => {
      const { manager } = createManager();
      await manager.subscribeLite('lite-topic-1');
      const command = new NotifyUnsubscribeLiteCommand().setLiteTopic('lite-topic-1');
      manager.onNotifyUnsubscribeLiteCommand(command);
      assert.strictEqual(manager.getLiteTopicSet().size, 0);
    });

    it('should ignore blank lite topic in notify unsubscribe command', async () => {
      const { manager } = createManager();
      await manager.subscribeLite('lite-topic-1');
      manager.onNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic(''));
      assert.ok(manager.getLiteTopicSet().has('lite-topic-1'));
    });

    it('should shutdown cleanly and clear the topic set', async () => {
      const { manager } = createManager();
      await manager.subscribeLite('lite-topic-1');
      manager.startUp();
      manager.shutdown();
      assert.strictEqual(manager.getLiteTopicSet().size, 0);
      manager.shutdown(); // idempotent
    });
  });
});
