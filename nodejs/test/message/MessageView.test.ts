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
import { gzipSync } from 'node:zlib';
import * as sinon from 'sinon';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import { DigestType, Encoding } from '../../proto/apache/rocketmq/v2/definition_pb';
import { MessageView } from '../../src/message';
import { crc32CheckSum } from '../../src/util';
import { buildMessagePB, buildRoute } from '../support/protobuf';

describe('MessageView', () => {
  const mq = buildRoute({ brokers: [{ name: 'broker-a', endpoints: '127.0.0.1:8081' }] }).messageQueues[0];

  it('记录每个实例的本地解码时间，缺省队列时同样有效', () => {
    const clock = sinon.useFakeTimers({ now: 1000, toFake: [ 'Date' ] });
    try {
      const first = new MessageView(buildMessagePB(), mq);
      assert.ok(first.decodeTimestamp instanceof Date);
      assert.equal(first.decodeTimestamp.getTime(), 1000);
      clock.tick(7);
      const second = new MessageView(buildMessagePB());
      assert.equal(second.decodeTimestamp.getTime(), 1007);
      assert.equal(first.decodeTimestamp.getTime(), 1000);
    } finally {
      clock.restore();
    }
  });

  for (const [ name, digestType ] of [[ 'CRC32', DigestType.CRC32 ], [ 'MD5', DigestType.MD5 ], [ 'SHA1', DigestType.SHA1 ]] as const) {
    it(`marks the message corrupted when the ${name} checksum mismatches`, () => {
      const view = new MessageView(buildMessagePB({ digestType, checksum: 'mismatch' }), mq);
      assert.equal(view.corrupted, true);
      assert.deepEqual(view.body, Buffer.from('test-body'));
    });

    it(`accepts a matching ${name} checksum`, () => {
      const view = new MessageView(buildMessagePB({ digestType }), mq);
      assert.equal(view.corrupted, false);
    });
  }

  it('decompresses a gzip body', () => {
    const body = Buffer.from('compressed-body');
    const view = new MessageView(buildMessagePB({ body, gzip: true }), mq);
    assert.equal(view.corrupted, false);
    assert.deepEqual(view.body, body);
  });

  it('marks the message corrupted and keeps the raw body when gzip decoding fails', () => {
    const raw = Buffer.from('not gzip data');
    const message = buildMessagePB({ body: raw });
    message.getSystemProperties()!.setBodyEncoding(Encoding.GZIP)
      .getBodyDigest()!.setChecksum(crc32CheckSum(raw));
    const view = new MessageView(message, mq);
    assert.equal(view.corrupted, true);
    assert.deepEqual(view.body, raw);
    assert.equal(view.messageId, 'test-message-id');
  });

  it('decodes system properties, user properties and endpoints', () => {
    const deliveryTimestamp = new Date('2026-01-01T00:00:00Z');
    const bornTimestamp = new Date('2025-12-31T23:59:59Z');
    const message = buildMessagePB({
      tag: 'tag-a', keys: [ 'key-a', 'key-b' ], messageGroup: 'group-a', liteTopic: 'lite-a', priority: 3,
      deliveryAttempt: 2, receiptHandle: 'handle-a', properties: new Map([[ 'k', 'v' ]]),
    });
    message.getSystemProperties()!
      .setDeliveryTimestamp(Timestamp.fromDate(deliveryTimestamp))
      .setBornTimestamp(Timestamp.fromDate(bornTimestamp))
      .setBornHost('host-a')
      .setQueueOffset(42);
    const transportDeliveryTimestamp = new Date();
    const view = new MessageView(message, mq, transportDeliveryTimestamp);

    assert.equal(view.getMessageId(), 'test-message-id');
    assert.equal(view.getTopic(), 'test-topic');
    assert.equal(view.getTag(), 'tag-a');
    assert.deepEqual(view.getKeys(), [ 'key-a', 'key-b' ]);
    assert.equal(view.getMessageGroup(), 'group-a');
    assert.equal(view.getLiteTopic(), 'lite-a');
    assert.equal(view.getPriority(), 3);
    assert.deepEqual(view.getDeliveryTimestamp(), deliveryTimestamp);
    assert.deepEqual(view.getBornTimestamp(), bornTimestamp);
    assert.equal(view.getBornHost(), 'host-a');
    assert.equal(view.getDeliveryAttempt(), 2);
    assert.equal(view.offset, 42);
    assert.equal(view.receiptHandle, 'handle-a');
    assert.equal(view.transportDeliveryTimestamp, transportDeliveryTimestamp);
    assert.equal(view.getProperties().get('k'), 'v');
    assert.equal(view.endpoints, mq.broker.endpoints);
    assert.equal(view.incrementAndGetDeliveryAttempt(), 3);
  });

  it('returns copies of body, keys and properties', () => {
    const view = new MessageView(buildMessagePB({ keys: [ 'key-a' ], properties: new Map([[ 'k', 'v' ]]) }), mq);
    view.getBody().fill(0);
    view.getKeys().push('key-b');
    view.getProperties().set('k', 'changed');
    assert.deepEqual(view.body, Buffer.from('test-body'));
    assert.deepEqual(view.keys, [ 'key-a' ]);
    assert.equal(view.properties.get('k'), 'v');
  });

  it('skips the checksum for an unsupported digest type', () => {
    const message = buildMessagePB({ checksum: 'ignored' });
    message.getSystemProperties()!.getBodyDigest()!.setType(DigestType.DIGEST_TYPE_UNSPECIFIED);
    assert.equal(new MessageView(message, mq).corrupted, false);
  });

  it('keeps a gzip body whose checksum covers the compressed bytes', () => {
    const body = Buffer.from('payload');
    const compressed = gzipSync(body);
    const message = buildMessagePB({ body, gzip: true });
    assert.equal(message.getSystemProperties()!.getBodyDigest()!.getChecksum(), crc32CheckSum(compressed));
    assert.deepEqual(new MessageView(message, mq).body, body);
  });
});
