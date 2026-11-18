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
 * Test for Priority Message
 */

import * as assert from 'node:assert';
import { Message } from '../../src/message/Message';
import { PublishingMessage } from '../../src/message/PublishingMessage';
import { MessageType } from '../../proto/apache/rocketmq/v2/definition_pb';

describe('Priority Message', () => {
  const fakeTopic = 'test-topic';
  const fakeBody = Buffer.from('test body');

  it('should create message with priority', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      priority: 1,
    });

    assert.strictEqual(message.priority, 1);
  });

  it('should create message without priority (undefined)', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
    });

    assert.strictEqual(message.priority, undefined);
  });

  it('should throw error when priority is negative', () => {
    assert.throws(() => {
      new Message({
        topic: fakeTopic,
        body: fakeBody,
        priority: -1,
      });
    }, /priority must be greater than or equal to 0/);
  });

  it('should throw error when priority and deliveryTimestamp are set together', () => {
    assert.throws(() => {
      new Message({
        topic: fakeTopic,
        body: fakeBody,
        priority: 1,
        deliveryTimestamp: new Date(Date.now() + 60000),
      });
    }, /priority is mutually exclusive with/);
  });

  it('should throw error when priority and messageGroup are set together', () => {
    assert.throws(() => {
      new Message({
        topic: fakeTopic,
        body: fakeBody,
        priority: 1,
        messageGroup: 'test-group',
      });
    }, /priority is mutually exclusive with/);
  });

  it('should accept priority = 0', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      priority: 0,
    });

    assert.strictEqual(message.priority, 0);
  });

  it('should accept high priority values', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      priority: 100,
    });

    assert.strictEqual(message.priority, 100);
  });

  it('should classify priority = 0 as a priority message', () => {
    const settings = { maxBodySizeBytes: 1024 } as any;
    const options = { topic: fakeTopic, body: fakeBody };
    for (const priority of [ 0, 5 ]) {
      assert.strictEqual(new PublishingMessage({ ...options, priority }, settings, false).messageType,
        MessageType.PRIORITY);
    }
    assert.strictEqual(new PublishingMessage(options, settings, false).messageType, MessageType.NORMAL);
    assert.throws(() => new PublishingMessage({ ...options, priority: 0 }, settings, true),
      /Transactional message should not set/);
  });
});
