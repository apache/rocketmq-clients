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
import {
  ClientType,
  CustomizedBackoff,
  FilterType,
  RetryPolicy as RetryPolicyPB,
  Subscription,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { FilterExpression } from '../../src/consumer/FilterExpression';
import { PushSubscriptionSettings } from '../../src/consumer/PushSubscriptionSettings';
import { CustomizedBackoffRetryPolicy, ExponentialBackoffRetryPolicy } from '../../src/retry';
import { Endpoints } from '../../src/route';
import { createDuration } from '../../src/util';
import { buildSettings } from '../support/protobuf';

function createSettings(consumeConcurrentlyMax?: number) {
  return new PushSubscriptionSettings('ns', 'client-a', ClientType.PUSH_CONSUMER, new Endpoints('127.0.0.1:8081'),
    'group-a', 3000, new Map([[ 'topic-a', new FilterExpression('tag-a') ]]), undefined, consumeConcurrentlyMax);
}

describe('PushSubscriptionSettings', () => {
  it('uses the defaults before the server settings arrive', () => {
    const settings = createSettings();
    assert.strictEqual(settings.isFifo(), false);
    assert.strictEqual(settings.getReceiveBatchSize(), 32);
    assert.strictEqual(settings.getLongPollingTimeout(), 30000);
    assert.strictEqual(settings.getConsumeConcurrentlyMax(), 32);
    assert.strictEqual(settings.getRetryPolicy(), undefined);
  });

  it('syncs fifo, batch size, long polling timeout and the exponential retry policy', () => {
    const settings = createSettings(8);
    const subscription = new Subscription().setFifo(true).setReceiveBatchSize(16)
      .setLongPollingTimeout(createDuration(20500));

    settings.sync(buildSettings({ clientType: ClientType.PUSH_CONSUMER, subscription }));

    assert.strictEqual(settings.isFifo(), true);
    assert.strictEqual(settings.getReceiveBatchSize(), 16);
    assert.strictEqual(settings.getLongPollingTimeout(), 20500);
    // The subscription settings carry no consumeConcurrentlyMax, so the local option is kept.
    assert.strictEqual(settings.getConsumeConcurrentlyMax(), 8);
    const retryPolicy = settings.getRetryPolicy();
    assert.ok(retryPolicy instanceof ExponentialBackoffRetryPolicy);
    assert.strictEqual(retryPolicy.getMaxAttempts(), 3);
    assert.strictEqual(retryPolicy.getNextAttemptDelay(1), 1000);
  });

  it('syncs the customized retry policy', () => {
    const settings = createSettings();
    const retryPolicy = new RetryPolicyPB().setMaxAttempts(4).setCustomizedBackoff(
      new CustomizedBackoff().setNextList([ createDuration(1000), createDuration(5000) ]));

    settings.sync(buildSettings({ clientType: ClientType.PUSH_CONSUMER, subscription: new Subscription(), retryPolicy }));

    const synced = settings.getRetryPolicy();
    assert.ok(synced instanceof CustomizedBackoffRetryPolicy);
    assert.strictEqual(synced.getMaxAttempts(), 4);
    assert.strictEqual(synced.getNextAttemptDelay(2), 5000);
  });

  it('ignores settings without a subscription', () => {
    const settings = createSettings();
    settings.sync(buildSettings());
    assert.strictEqual(settings.getRetryPolicy(), undefined);
    assert.strictEqual(settings.getReceiveBatchSize(), 32);
  });

  it('writes the client type, access point, timeout, group and subscriptions to protobuf', () => {
    const pb = createSettings().toProtobuf();

    assert.strictEqual(pb.getClientType(), ClientType.PUSH_CONSUMER);
    assert.strictEqual(pb.getAccessPoint()?.getAddressesList()[0].getHost(), '127.0.0.1');
    assert.strictEqual(pb.getRequestTimeout()?.getSeconds(), 3);
    assert.ok(pb.getUserAgent());
    const subscription = pb.getSubscription()!;
    assert.strictEqual(subscription.getGroup()?.getName(), 'group-a');
    const entries = subscription.getSubscriptionsList();
    assert.strictEqual(entries.length, 1);
    assert.strictEqual(entries[0].getTopic()?.getName(), 'topic-a');
    assert.strictEqual(entries[0].getExpression()?.getExpression(), 'tag-a');
    assert.strictEqual(entries[0].getExpression()?.getType(), FilterType.TAG);
  });
});
