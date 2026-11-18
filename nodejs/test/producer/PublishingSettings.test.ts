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
import { ClientType, Publishing, Subscription } from '../../proto/apache/rocketmq/v2/definition_pb';
import { PublishingSettings } from '../../src/producer';
import { ExponentialBackoffRetryPolicy } from '../../src/retry';
import { Endpoints } from '../../src/route';
import { buildSettings } from '../support/protobuf';

describe('PublishingSettings', () => {
  const accessPoint = new Endpoints('127.0.0.1:8080');

  function create(topics = new Set([ 'topic-a', 'topic-b' ])) {
    return new PublishingSettings('ns', 'client-id', accessPoint,
      ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3), 3000, topics);
  }

  it('starts with the default publishing settings', () => {
    const settings = create();
    assert.equal(settings.maxBodySizeBytes, 4 * 1024 * 1024);
    assert.equal(settings.isValidateMessageType(), true);
    assert.equal(settings.getRetryPolicy()!.getNextAttemptDelay(2), 0);
  });

  it('syncs backoff, message type validation and max body size from the remote', () => {
    const settings = create();
    settings.sync(buildSettings({
      publishing: new Publishing().setMaxBodySize(1024).setValidateMessageType(false),
    }));

    assert.equal(settings.maxBodySizeBytes, 1024);
    assert.equal(settings.isValidateMessageType(), false);
    const retryPolicy = settings.getRetryPolicy()!;
    // Max attempts stay local; the backoff comes from the remote (initial=1s, max=10s, multiplier=2).
    assert.equal(retryPolicy.getMaxAttempts(), 3);
    assert.equal(retryPolicy.getNextAttemptDelay(1), 1000);
    assert.equal(retryPolicy.getNextAttemptDelay(2), 2000);
    assert.equal(retryPolicy.getNextAttemptDelay(10), 10000);
  });

  it('ignores settings issued for another client type', () => {
    const settings = create();
    settings.sync(buildSettings({ clientType: ClientType.SIMPLE_CONSUMER, subscription: new Subscription() }));

    assert.equal(settings.maxBodySizeBytes, 4 * 1024 * 1024);
    assert.equal(settings.isValidateMessageType(), true);
    assert.equal(settings.getRetryPolicy()!.getNextAttemptDelay(2), 0);
  });

  it('converts to protobuf with every publishing field', () => {
    const pb = create().toProtobuf();

    assert.equal(pb.getClientType(), ClientType.PRODUCER);
    assert.deepEqual(pb.getAccessPoint()!.toObject(), accessPoint.toProtobuf().toObject());
    assert.equal(pb.getRequestTimeout()!.getSeconds(), 3);
    assert.ok(pb.getUserAgent());
    const publishing = pb.getPublishing()!;
    assert.equal(publishing.getValidateMessageType(), true);
    assert.deepEqual(publishing.getTopicsList().map(t => [ t.getName(), t.getResourceNamespace() ]),
      [[ 'topic-a', 'ns' ], [ 'topic-b', 'ns' ]]);
  });

  it('includes topics added after construction', () => {
    const topics = new Set<string>();
    const settings = create(topics);
    topics.add('topic-c');
    assert.deepEqual(settings.toProtobuf().getPublishing()!.getTopicsList().map(t => t.getName()), [ 'topic-c' ]);
  });
});
