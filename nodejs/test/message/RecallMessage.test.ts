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
import { Message } from '../../src/message/Message';
import { SendReceipt } from '../../src/producer/SendReceipt';
import { RecallReceipt } from '../../src/producer/RecallReceipt';

describe('Recall Message', () => {
  describe('SendReceipt', () => {
    it('should contain recallHandle for delay messages', () => {
      const messageId = 'test-message-id-12345';
      const transactionId = 'test-transaction-id';
      const recallHandle = 'recall-handle-abc-xyz';
      const mockMessageQueue = {
        broker: {
          endpoints: 'localhost:8080',
        },
      } as any;
      const offset = 100;

      const receipt = new SendReceipt(messageId, transactionId, recallHandle, mockMessageQueue, offset);

      assert.strictEqual(receipt.messageId, messageId);
      assert.strictEqual(receipt.transactionId, transactionId);
      assert.strictEqual(receipt.recallHandle, recallHandle);
      assert.strictEqual(receipt.offset, offset);
    });

    it('should have valid recallHandle after sending delay message', () => {
      // This test verifies that when sending a delay message,
      // the SendReceipt contains a valid recallHandle
      const mockMessageQueue = {
        broker: {
          endpoints: 'localhost:8080',
        },
      } as any;

      const receipt = new SendReceipt(
        'message-id-123',
        '',
        'recall-handle-xyz-789',
        mockMessageQueue,
        50,
      );

      assert.ok(receipt.recallHandle, 'recallHandle should exist');
      assert.strictEqual(typeof receipt.recallHandle, 'string');
      assert.ok(receipt.recallHandle.length > 0, 'recallHandle should not be empty');
    });
  });

  describe('RecallReceipt', () => {
    it('should create recall receipt with messageId', () => {
      const messageId = 'recalled-message-id-999';
      const receipt = new RecallReceipt(messageId);

      assert.strictEqual(receipt.messageId, messageId);
    });

    it('should return valid toString representation', () => {
      const messageId = 'test-message-id';
      const receipt = new RecallReceipt(messageId);
      const str = receipt.toString();

      assert.ok(str.includes(messageId));
    });
  });

  describe('Message with delay for recall', () => {
    it('should create delay message successfully', () => {
      const delayMs = 60000; // 1 minute
      const message = new Message({
        topic: 'test-delay-topic',
        body: Buffer.from('Delay message for recall test'),
        delay: delayMs,
      });

      assert.strictEqual(message.topic, 'test-delay-topic');
      assert.ok(message.deliveryTimestamp, 'deliveryTimestamp should be set');

      // Verify deliveryTimestamp is approximately now + delay
      const expectedTime = Date.now() + delayMs;
      const actualTime = message.deliveryTimestamp.getTime();
      assert.ok(Math.abs(actualTime - expectedTime) < 1000, 'deliveryTimestamp should be now + delay');
    });

    it('should create timed message successfully', () => {
      const futureTime = new Date(Date.now() + 120000); // 2 minutes later
      const message = new Message({
        topic: 'test-timed-topic',
        body: Buffer.from('Timed message for recall test'),
        deliveryTimestamp: futureTime,
      });

      assert.strictEqual(message.topic, 'test-timed-topic');
      assert.strictEqual(message.deliveryTimestamp?.getTime(), futureTime.getTime());
    });
  });
});
