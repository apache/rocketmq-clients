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
 * Test for Lite FIFO Consume Service
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { LiteFifoConsumeService } from '../../src/consumer/LiteFifoConsumeService';
import { MessageListener } from '../../src/consumer/MessageListener';
import { ConsumeResult, ConsumeResultSuspend } from '../../src/consumer/ConsumeResult';
import type { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { MessageView } from '../../src/message';

describe('LiteFifoConsumeService', () => {
  const createMessageListener = (result: ConsumeResult = ConsumeResult.SUCCESS): MessageListener => {
    return {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      async consume(_messageView: MessageView): Promise<ConsumeResult> {
        return result;
      },
    };
  };

  const createMockProcessQueue = (): ProcessQueue => {
    const erased: Array<{ messageView: MessageView; result: ConsumeResult }> = [];
    return {
      eraseFifoMessage: async (messageView: MessageView, result: ConsumeResult) => {
        erased.push({ messageView, result });
      },
      discardFifoMessage: () => { /* noop */ },
      getErased: () => erased,
    } as unknown as ProcessQueue;
  };

  const createMessageView = (id: string, liteTopic: string): MessageView => {
    return {
      messageId: id,
      liteTopic,
      corrupted: false,
    } as unknown as MessageView;
  };

  it('should support enableFifoConsumeAccelerator option', () => {
    const messageListener = createMessageListener();

    // Create with accelerator enabled
    const serviceWithAccelerator = new LiteFifoConsumeService('test-client', messageListener, true);
    assert.ok(serviceWithAccelerator, 'Should create service with accelerator enabled');

    // Create with accelerator disabled (default)
    const serviceWithoutAccelerator = new LiteFifoConsumeService('test-client', messageListener, false);
    assert.ok(serviceWithoutAccelerator, 'Should create service with accelerator disabled');

    // Create without specifying (default to false)
    const serviceDefault = new LiteFifoConsumeService('test-client', messageListener);
    assert.ok(serviceDefault, 'Should create service with default accelerator setting');
  });

  it('should group messages by lite topic', () => {
    const messageListener = createMessageListener();
    const service = new LiteFifoConsumeService('test-client', messageListener, true);

    // Expose protected method for testing
    const getMessageGroupKey = (service as any).getMessageGroupKey.bind(service);

    const messageWithLiteTopic = {
      liteTopic: 'lite-topic-a',
      messageGroup: 'fifo-group',
    } as MessageView;
    assert.strictEqual(getMessageGroupKey(messageWithLiteTopic), 'lite-topic-a',
      'Should use lite topic as group key');

    const messageWithoutLiteTopic = {
      liteTopic: undefined,
      messageGroup: 'fifo-group',
    } as MessageView;
    assert.strictEqual(getMessageGroupKey(messageWithoutLiteTopic), '',
      'Should return empty string when lite topic is absent');
  });

  it('should suspend remaining messages with the same lite topic', async () => {
    const suspendResult = ConsumeResultSuspend.of(100);
    const messageListener = createMessageListener(suspendResult);
    const service = new LiteFifoConsumeService('test-client', messageListener, false);
    const pq = createMockProcessQueue();

    const msg1 = createMessageView('1', 'topic-a');
    const msg2 = createMessageView('2', 'topic-a');
    const msg3 = createMessageView('3', 'topic-b');
    const msg4 = createMessageView('4', 'topic-a');

    const consumeIteratively = (service as any).consumeIteratively.bind(service);
    await consumeIteratively(pq, [ msg1, msg2, msg3, msg4 ], 0);

    const erased = (pq as any).getErased();
    // msg1 consumed and suspended, msg2/msg4 suspended without consume, msg3 consumed in continuation
    assert.strictEqual(erased.length, 4, 'All messages should be erased');
    assert.strictEqual(erased[0].messageView.messageId, '1');
    assert.strictEqual(erased[0].result, suspendResult);
    assert.strictEqual(erased[1].messageView.messageId, '2');
    assert.strictEqual(erased[1].result, suspendResult);
    assert.strictEqual(erased[2].messageView.messageId, '4');
    assert.strictEqual(erased[2].result, suspendResult);
    assert.strictEqual(erased[3].messageView.messageId, '3');
    assert.strictEqual(erased[3].result, suspendResult);
  });

  it('should continue consuming messages with different lite topic after suspend', async () => {
    const suspendResult = ConsumeResultSuspend.of(100);
    let callCount = 0;
    const messageListener: MessageListener = {
      async consume(messageView: MessageView): Promise<ConsumeResult> {
        callCount++;
        if (messageView.liteTopic === 'topic-a') {
          return suspendResult;
        }
        return ConsumeResult.SUCCESS;
      },
    };
    const service = new LiteFifoConsumeService('test-client', messageListener, false);
    const pq = createMockProcessQueue();

    const msg1 = createMessageView('1', 'topic-a');
    const msg2 = createMessageView('2', 'topic-b');
    const msg3 = createMessageView('3', 'topic-a');

    const consumeIteratively = (service as any).consumeIteratively.bind(service);
    await consumeIteratively(pq, [ msg1, msg2, msg3 ], 0);

    const erased = (pq as any).getErased();
    // msg1 consumed and suspended, msg3 suspended without consume, msg2 consumed in continuation
    assert.strictEqual(callCount, 2, 'Only msg1 and msg2 should be consumed by listener');
    assert.strictEqual(erased.length, 3);
    assert.strictEqual(erased[0].messageView.messageId, '1');
    assert.strictEqual(erased[0].result, suspendResult);
    assert.strictEqual(erased[1].messageView.messageId, '3');
    assert.strictEqual(erased[1].result, suspendResult);
    assert.strictEqual(erased[2].messageView.messageId, '2');
    assert.strictEqual(erased[2].result, ConsumeResult.SUCCESS);
  });
});
