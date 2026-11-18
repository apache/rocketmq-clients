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
import { Code, LiteSubscriptionAction, Settings, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { NotifyUnsubscribeLiteCommand, SyncLiteSubscriptionResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { LiteSubscriptionHost, LiteSubscriptionManager } from '../../src/consumer/LiteSubscriptionManager';
import { OffsetOption } from '../../src/consumer/OffsetOption';
import { ClientException, LiteSubscriptionQuotaExceededException } from '../../src/exception';
import { Endpoints, Resource } from '../../src/route';
import { createFakeRpcClientManager, createSilentLogger, deferred } from '../support/fakes';
import { buildStatus } from '../support/protobuf';

class FakeHost implements LiteSubscriptionHost {
  clientId = 'lite-manager-test';
  running = true;
  readonly logger = createSilentLogger();
  readonly rpc = createFakeRpcClientManager();
  endpoints = [ new Endpoints('127.0.0.1:8081'), new Endpoints('127.0.0.1:8082') ];

  constructor() {
    this.rpc.syncLiteSubscription.resolves(new SyncLiteSubscriptionResponse().setStatus(buildStatus()));
  }

  isRunning() { return this.running; }
  getLogger() { return this.logger; }
  getRpcClientManager() { return this.rpc; }
  getRequestTimeout() { return 1234; }
  getSyncEndpoints() { return this.endpoints; }
}

describe('LiteSubscriptionManager offline', () => {
  let host: FakeHost;
  let manager: LiteSubscriptionManager;
  let clock: sinon.SinonFakeTimers;

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    host = new FakeHost();
    manager = new LiteSubscriptionManager(host, new Resource('ns', 'parent'), new Resource('ns', 'group'));
  });

  afterEach(() => {
    manager.shutdown();
    clock.restore();
    sinon.restore();
  });

  it('syncs immediately and every 30 seconds, then stops and clears on shutdown', async () => {
    await manager.subscribeLite('*');
    host.rpc.syncLiteSubscription.resetHistory();
    manager.startUp();
    await clock.tickAsync(0);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 2);
    const [ endpoints, request, timeout ] = host.rpc.syncLiteSubscription.firstCall.args;
    assert.strictEqual(endpoints, host.endpoints[0]);
    assert.strictEqual(timeout, 1234);
    assert.strictEqual(request.getAction(), LiteSubscriptionAction.COMPLETE_ADD);
    assert.deepStrictEqual(request.getLiteTopicSetList(), [ '*' ]);
    assert.strictEqual(request.getTopic()!.getResourceNamespace(), 'ns');
    assert.strictEqual(request.getGroup()!.getName(), 'group');
    await clock.tickAsync(29999);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 2);
    await clock.tickAsync(1);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 4);
    manager.shutdown();
    await clock.tickAsync(60000);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 4);
    assert.strictEqual(manager.getLiteTopicSet().size, 0);
    assert.strictEqual(clock.countTimers(), 0);
  });

  it('regression: repeated startUp creates only one sync timer and one immediate round', async () => {
    manager.startUp();
    manager.startUp();
    await clock.tickAsync(0);
    assert.strictEqual(clock.countTimers(), 1);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 2);
    await clock.tickAsync(30000);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 4);
    manager.shutdown();
    assert.strictEqual(clock.countTimers(), 0);
    manager.startUp();
    await clock.tickAsync(0);
    assert.strictEqual(clock.countTimers(), 1);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 6);
  });

  ([ 'subscribeLite', 'unsubscribeLite' ] as const).forEach(method => {
    it(`regression: ${method} rejects with a plain Error before running`, async () => {
      host.running = false;
      await assert.rejects(manager[method]('one'), error => {
        assert.strictEqual((error as Error).constructor, Error);
        assert.match((error as Error).message, /Consumer is not running/);
        return true;
      });
      assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 0);
    });
  });

  it('keeps failed subscribe out of the set and preserves failed unsubscribe', async () => {
    const failure = new Error('transport failed');
    host.rpc.syncLiteSubscription.rejects(failure);
    await assert.rejects(manager.subscribeLite('one'), error => error === failure);
    assert.strictEqual(manager.getLiteTopicSet().size, 0);
    host.rpc.syncLiteSubscription.resolves(new SyncLiteSubscriptionResponse().setStatus(buildStatus()));
    await manager.subscribeLite('one');
    host.rpc.syncLiteSubscription.rejects(failure);
    await assert.rejects(manager.unsubscribeLite('one'), error => error === failure);
    assert.deepStrictEqual([ ...manager.getLiteTopicSet() ], [ 'one' ]);
  });

  it('deduplicates completed subscriptions and ignores unknown unsubscribe without RPC', async () => {
    await manager.subscribeLite('one', OffsetOption.MIN_OFFSET);
    const request = host.rpc.syncLiteSubscription.firstCall.args[1];
    assert.deepStrictEqual(request.getOffsetOption()!.toObject(), OffsetOption.MIN_OFFSET.toProtobuf().toObject());
    host.rpc.syncLiteSubscription.resetHistory();
    await manager.subscribeLite('one', OffsetOption.MAX_OFFSET);
    await manager.unsubscribeLite('unknown');
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 0);
    const copy = manager.getLiteTopicSet();
    copy.clear();
    assert.strictEqual(manager.getLiteTopicSet().size, 1);
  });

  it('validates blank and excessive length but accepts arbitrary nonblank names like Java', async () => {
    for (const name of [ '', ' ', '\t' ]) {
      await assert.rejects(manager.subscribeLite(name), /liteTopic is blank/);
    }
    await assert.rejects(manager.subscribeLite('x'.repeat(65)), /max length 64/);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 0);
    for (const name of [ '*', '中文/路径 !', 'x'.repeat(64) ]) await manager.subscribeLite(name);
    assert.strictEqual(manager.getLiteTopicSet().size, 3);
  });

  it('updates quota and length from settings while absent fields retain the last values', async () => {
    manager.sync(new Settings().setSubscription(new Subscription().setLiteSubscriptionQuota(1).setMaxLiteTopicSize(3)));
    manager.sync(new Settings());
    manager.sync(new Settings().setSubscription(new Subscription()));
    await assert.rejects(manager.subscribeLite('long'), /max length 3/);
    await manager.subscribeLite('one');
    await assert.rejects(manager.subscribeLite('two'), /quota exceeded 1/);
    manager.sync(new Settings().setSubscription(new Subscription().setLiteSubscriptionQuota(0).setMaxLiteTopicSize(0)));
    await assert.rejects(manager.subscribeLite('a'), /max length 0/);
  });

  it('regression: local quota failure uses the advertised typed exception and protocol code', async () => {
    manager.sync(new Settings().setSubscription(new Subscription().setLiteSubscriptionQuota(0)));
    await assert.rejects(manager.subscribeLite('one'), error => {
      assert.ok(error instanceof LiteSubscriptionQuotaExceededException);
      assert.ok(error instanceof ClientException);
      assert.strictEqual(error.code, Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED);
      return true;
    });
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 0);
  });

  it('concurrent subscriptions add only the successful topic when the server rejects the other', async () => {
    manager.sync(new Settings().setSubscription(new Subscription().setLiteSubscriptionQuota(1)));
    const accepted = deferred<SyncLiteSubscriptionResponse>();
    const rejected = deferred<SyncLiteSubscriptionResponse>();
    host.rpc.syncLiteSubscription.callsFake(async (_endpoints, request) => {
      return request.getLiteTopicSetList()[0] === 'one' ? accepted.promise : rejected.promise;
    });
    const first = manager.subscribeLite('one');
    const second = assert.rejects(manager.subscribeLite('two'), (error: ClientException) => {
      assert.strictEqual(error.code, Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED);
      return true;
    });
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 4);
    rejected.resolve(new SyncLiteSubscriptionResponse().setStatus(buildStatus(Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED)));
    await second;
    assert.strictEqual(manager.getLiteTopicSet().size, 0);
    accepted.resolve(new SyncLiteSubscriptionResponse().setStatus(buildStatus()));
    await first;
    assert.deepStrictEqual([ ...manager.getLiteTopicSet() ], [ 'one' ]);
  });

  it('logs periodic sync failure and retries at the next interval', async () => {
    host.rpc.syncLiteSubscription.rejects(new Error('offline'));
    manager.startUp();
    await clock.tickAsync(0);
    assert.ok(host.logger.error.called);
    host.rpc.syncLiteSubscription.resolves(new SyncLiteSubscriptionResponse().setStatus(buildStatus()));
    await clock.tickAsync(30000);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 4);
  });

  it('skips full sync after a quota reduction until the notified topic is removed', async () => {
    await manager.subscribeLite('one');
    manager.sync(new Settings().setSubscription(new Subscription().setLiteSubscriptionQuota(0)));
    host.rpc.syncLiteSubscription.resetHistory();
    manager.startUp();
    await clock.tickAsync(0);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 0);
    assert.ok(host.logger.error.called);
    manager.onNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic('one'));
    await clock.tickAsync(30000);
    assert.strictEqual(host.rpc.syncLiteSubscription.callCount, 2);
  });

  it('removes only the notified topic and tolerates notifications during shutdown', async () => {
    await manager.subscribeLite('one');
    await manager.subscribeLite('two');
    manager.onNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic('one'));
    manager.onNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic(' '));
    assert.deepStrictEqual([ ...manager.getLiteTopicSet() ], [ 'two' ]);
    manager.shutdown();
    manager.onNotifyUnsubscribeLiteCommand(new NotifyUnsubscribeLiteCommand().setLiteTopic('two'));
    assert.strictEqual(manager.getLiteTopicSet().size, 0);
  });

  it('fails without route endpoints rather than adding a local subscription', async () => {
    host.endpoints = [];
    await assert.rejects(manager.subscribeLite('one'), /No endpoints available/);
    assert.strictEqual(manager.getLiteTopicSet().size, 0);
  });
});
