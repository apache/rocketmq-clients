/**
 * Test for Priority Message
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Message } from '../../src/message/Message';

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
    }, /priority and deliveryTimestamp should not be set at same time/);
  });

  it('should throw error when priority and messageGroup are set together', () => {
    assert.throws(() => {
      new Message({
        topic: fakeTopic,
        body: fakeBody,
        priority: 1,
        messageGroup: 'test-group',
      });
    }, /priority and messageGroup should not be set at same time/);
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
});
