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
 * Test for Lite Message
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Message } from '../../src/message/Message';

describe('Lite Message', () => {
  const fakeTopic = 'test-topic';
  const fakeBody = Buffer.from('test body');

  it('should create message with liteTopic', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      liteTopic: 'lite-topic-name',
    });

    assert.strictEqual(message.liteTopic, 'lite-topic-name');
  });

  it('should create message without liteTopic (undefined)', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
    });

    assert.strictEqual(message.liteTopic, undefined);
  });

  it('should throw error when liteTopic and priority are set together', () => {
    assert.throws(() => {
      new Message({
        topic: fakeTopic,
        body: fakeBody,
        liteTopic: 'lite-topic-name',
        priority: 5,
      });
    }, /priority and liteTopic should not be set at same time/);
  });

  it('should throw error when liteTopic and messageGroup are set together', () => {
    // Note: liteTopic and messageGroup mutual exclusivity is checked in PublishingMessage
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      liteTopic: 'lite-topic-name',
      messageGroup: 'test-group',
    });

    assert.strictEqual(message.liteTopic, 'lite-topic-name');
    assert.strictEqual(message.messageGroup, 'test-group');
  });

  it('should throw error when liteTopic and deliveryTimestamp are set together', () => {
    // Note: liteTopic and deliveryTimestamp mutual exclusivity is checked in PublishingMessage
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      liteTopic: 'lite-topic-name',
      deliveryTimestamp: new Date(Date.now() + 60000),
    });

    assert.strictEqual(message.liteTopic, 'lite-topic-name');
    assert.ok(message.deliveryTimestamp);
  });

  it('should accept empty string liteTopic', () => {
    const message = new Message({
      topic: fakeTopic,
      body: fakeBody,
      liteTopic: '',
    });

    assert.strictEqual(message.liteTopic, '');
  });
});
