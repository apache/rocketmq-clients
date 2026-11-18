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
import { Encoding, MessageType, Publishing } from '../../proto/apache/rocketmq/v2/definition_pb';
import { MessageOptions, PublishingMessage } from '../../src/message';
import { PublishingSettings } from '../../src/producer';
import { ExponentialBackoffRetryPolicy } from '../../src/retry';
import { Endpoints } from '../../src/route';
import { buildRoute, buildSettings } from '../support/protobuf';

describe('PublishingMessage', () => {
  const settings = new PublishingSettings('', 'client-id', new Endpoints('127.0.0.1:8080'),
    ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3), 3000, new Set());
  const base = { topic: 'test-topic', body: Buffer.from('body') };

  const cases: Array<[ string, Partial<MessageOptions>, boolean, MessageType ]> = [
    [ 'NORMAL', {}, false, MessageType.NORMAL ],
    [ 'FIFO', { messageGroup: 'group-a' }, false, MessageType.FIFO ],
    [ 'DELAY', { delay: 1000 }, false, MessageType.DELAY ],
    [ 'DELAY by timestamp', { deliveryTimestamp: new Date(Date.now() + 1000) }, false, MessageType.DELAY ],
    [ 'TRANSACTION', {}, true, MessageType.TRANSACTION ],
    [ 'PRIORITY', { priority: 0 }, false, MessageType.PRIORITY ],
    [ 'LITE', { liteTopic: 'lite-a' }, false, MessageType.LITE ],
  ];
  for (const [ name, options, txEnabled, messageType ] of cases) {
    it(`detects the ${name} message type`, () => {
      const message = new PublishingMessage({ ...base, ...options }, settings, txEnabled);
      assert.equal(message.messageType, messageType);
      assert.ok(message.messageId);
    });
  }

  for (const extra of [{ messageGroup: 'g' }, { delay: 1000 }, { liteTopic: 'lite' }, { priority: 1 }]) {
    it(`rejects a transactional message with ${Object.keys(extra)[0]}`, () => {
      assert.throws(() => new PublishingMessage({ ...base, ...extra }, settings, true),
        /Transactional message should not set messageGroup, liteTopic, deliveryTimestamp or priority/);
    });
  }

  const conflicts: Array<[ Partial<MessageOptions>, RegExp ]> = [
    [{ priority: 1, messageGroup: 'g' }, /priority is mutually exclusive/ ],
    [{ priority: 1, delay: 1000 }, /priority is mutually exclusive/ ],
    [{ priority: 1, liteTopic: 'lite' }, /priority is mutually exclusive/ ],
    [{ liteTopic: 'lite', messageGroup: 'g' }, /liteTopic is mutually exclusive/ ],
    [{ liteTopic: 'lite', delay: 1000 }, /liteTopic is mutually exclusive/ ],
    [{ messageGroup: 'g', delay: 1000 }, /delay\/deliveryTimestamp is mutually exclusive with messageGroup/ ],
    [{ priority: -1 }, /priority must be greater than or equal to 0/ ],
  ];
  for (const [ options, error ] of conflicts) {
    it(`rejects conflicting fields ${JSON.stringify(options)}`, () => {
      assert.throws(() => new PublishingMessage({ ...base, ...options }, settings, false), error);
    });
  }

  it('rejects a body larger than the max body size', () => {
    const small = new PublishingSettings('', 'client-id', new Endpoints('127.0.0.1:8080'),
      ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3), 3000, new Set());
    small.sync(buildSettings({ publishing: new Publishing().setMaxBodySize(4).setValidateMessageType(true) }));

    assert.ok(new PublishingMessage({ ...base, body: Buffer.alloc(4) }, small, false));
    assert.throws(() => new PublishingMessage({ ...base, body: Buffer.alloc(5) }, small, false),
      /Message body size exceeds the threshold, max size=4 bytes/);
  });

  it('converts to protobuf with every field', () => {
    const deliveryTimestamp = new Date(Date.now() + 1000);
    const message = new PublishingMessage({
      ...base, tag: 'tag-a', keys: [ 'key-a' ], deliveryTimestamp, properties: new Map([[ 'k', 'v' ]]),
    }, settings, false);
    const mq = buildRoute({ queuesPerBroker: 3 }).messageQueues[2];

    const pb = message.toProtobuf('ns', mq);

    assert.equal(pb.getTopic()!.getName(), 'test-topic');
    assert.equal(pb.getTopic()!.getResourceNamespace(), 'ns');
    assert.deepEqual(Buffer.from(pb.getBody_asU8()), base.body);
    assert.equal(pb.getUserPropertiesMap().get('k'), 'v');
    const properties = pb.getSystemProperties()!;
    assert.equal(properties.getMessageId(), message.messageId);
    assert.equal(properties.getMessageType(), MessageType.DELAY);
    assert.equal(properties.getTag(), 'tag-a');
    assert.deepEqual(properties.getKeysList(), [ 'key-a' ]);
    assert.equal(properties.getDeliveryTimestamp()!.toDate().getTime(), deliveryTimestamp.getTime());
    assert.equal(properties.getQueueId(), 2);
    assert.equal(properties.getBodyEncoding(), Encoding.IDENTITY);
    assert.ok(properties.getBornTimestamp());
    assert.ok(properties.getBornHost());
    assert.equal(properties.hasMessageGroup(), false);
    assert.equal(properties.hasLiteTopic(), false);
    assert.equal(properties.hasPriority(), false);
  });

  it('converts FIFO, lite and priority fields to protobuf', () => {
    const mq = buildRoute().messageQueues[0];
    const fifo = new PublishingMessage({ ...base, messageGroup: 'group-a' }, settings, false).toProtobuf('', mq);
    assert.equal(fifo.getSystemProperties()!.getMessageGroup(), 'group-a');
    const lite = new PublishingMessage({ ...base, liteTopic: 'lite-a' }, settings, false).toProtobuf('', mq);
    assert.equal(lite.getSystemProperties()!.getLiteTopic(), 'lite-a');
    const priority = new PublishingMessage({ ...base, priority: 0 }, settings, false).toProtobuf('', mq);
    assert.equal(priority.getSystemProperties()!.hasPriority(), true);
    assert.equal(priority.getSystemProperties()!.getPriority(), 0);
  });
});
