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
 * Real-scenario integration tests for LiteSimpleConsumer.
 *
 * Requires a running proxy at the configured endpoints (default localhost:8081)
 * with the following prerequisites:
 * - the bind topic exists and is created with the LITE message type;
 * - the consumer group exists and carries the attribute
 *   lite.bind.topic=<bindTopic> (lite group).
 *
 * The local development broker must run with enableLmq=true and
 * enableMultiDispatch=true so lite messages reach the LMQ queues.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert';
import { Producer } from '../../src';
import { LiteSimpleConsumerImpl } from '../../src/consumer/LiteSimpleConsumerImpl';
import { OffsetOption } from '../../src/consumer/OffsetOption';
import { endpoints, namespace } from '../helper';

const BIND_TOPIC = process.env.ROCKETMQ_NODEJS_CLIENT_LITE_TOPIC ?? 'lite-parent-topic';
const CONSUMER_GROUP = process.env.ROCKETMQ_NODEJS_CLIENT_LITE_GROUP ?? 'nodejs-lite-unittest-group';

describe('test/consumer/LiteSimpleConsumer.integration.test.ts', () => {
  let producer: Producer | null = null;
  let consumer: LiteSimpleConsumerImpl | null = null;

  afterEach(async () => {
    if (consumer) {
      await consumer.shutdown().catch(() => undefined);
      consumer = null;
    }
    if (producer) {
      await producer.shutdown().catch(() => undefined);
      producer = null;
    }
  });

  async function startClientPair() {
    producer = new Producer({
      endpoints,
      namespace,
      topic: BIND_TOPIC,
    });
    await producer.startup();

    consumer = new LiteSimpleConsumerImpl({
      endpoints,
      namespace,
      consumerGroup: CONSUMER_GROUP,
      bindTopic: BIND_TOPIC,
      awaitDuration: 5000,
    });
    await consumer.startup();
    assert.strictEqual(consumer.getConsumerGroup(), CONSUMER_GROUP);
    assert.strictEqual(consumer.getLiteTopicSet().size, 0);
  }

  /**
   * Send a lite message with retry. Right after the LITE topic is created, the
   * proxy may serve a cached topic route without the message.type attribute,
   * which makes the producer reject the send client-side. Refreshing the route
   * cache and retrying bridges that propagation window.
   */
  async function sendLiteWithRetry(options: Parameters<Producer['send']>[0]) {
    let lastErr: unknown = null;
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      try {
        return await producer!.send(options);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (!message.includes('message type not match')) {
          throw err;
        }
        lastErr = err;
        await (producer as unknown as { updateRoutes(): Promise<void> }).updateRoutes();
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    throw lastErr ?? new Error('sendLiteWithRetry deadline exceeded');
  }

  it('should send and receive lite messages end-to-end', async () => {
    await startClientPair();

    const liteTopic = `lite-topic-it-${Date.now()}`;
    await consumer!.subscribeLite(liteTopic, OffsetOption.MIN_OFFSET);
    assert.ok(consumer!.getLiteTopicSet().has(liteTopic));

    const sentIds: string[] = [];
    for (let i = 0; i < 5; i++) {
      const receipt = await sendLiteWithRetry({
        topic: BIND_TOPIC,
        liteTopic,
        keys: [ `lite-it-key-${i}` ],
        body: Buffer.from(`lite-it-body-${i}`),
      });
      assert.ok(receipt.messageId);
      sentIds.push(receipt.messageId);
    }
    assert.strictEqual(sentIds.length, 5);

    // Receive with a fixed invisible duration and ack every message.
    const receivedIds: string[] = [];
    const receivedBodies: string[] = [];
    const deadline = Date.now() + 30000;
    while (receivedIds.length < sentIds.length && Date.now() < deadline) {
      const views = await consumer!.receive(10, 15000);
      for (const view of views) {
        receivedIds.push(view.messageId as string);
        receivedBodies.push(Buffer.from(view.body as Uint8Array).toString());
        await consumer!.ack(view);
      }
    }

    assert.strictEqual(receivedIds.length, sentIds.length);
    assert.deepStrictEqual(receivedIds.sort(), sentIds.sort());
    for (let i = 0; i < 5; i++) {
      assert.ok(receivedBodies.includes(`lite-it-body-${i}`));
    }
  });

  it('should deliver messages sent after subscribeLite without offset option', async () => {
    await startClientPair();

    const liteTopic = `lite-topic-live-${Date.now()}`;
    await consumer!.subscribeLite(liteTopic);
    assert.ok(consumer!.getLiteTopicSet().has(liteTopic));

    const sentIds: string[] = [];
    for (let i = 0; i < 3; i++) {
      const receipt = await sendLiteWithRetry({
        topic: BIND_TOPIC,
        liteTopic,
        keys: [ `lite-live-key-${i}` ],
        body: Buffer.from(`lite-live-body-${i}`),
      });
      sentIds.push(receipt.messageId as string);
    }

    const receivedIds: string[] = [];
    const deadline = Date.now() + 30000;
    while (receivedIds.length < sentIds.length && Date.now() < deadline) {
      const views = await consumer!.receive(10, 15000);
      for (const view of views) {
        receivedIds.push(view.messageId as string);
        await consumer!.ack(view);
      }
    }
    assert.strictEqual(receivedIds.length, sentIds.length);
    assert.deepStrictEqual(receivedIds.sort(), sentIds.sort());
  });

  it('should stop delivering after unsubscribeLite', async () => {
    await startClientPair();

    const liteTopic = `lite-topic-unsub-${Date.now()}`;
    await consumer!.subscribeLite(liteTopic);
    await consumer!.unsubscribeLite(liteTopic);
    assert.strictEqual(consumer!.getLiteTopicSet().size, 0);

    // Messages for the unsubscribed lite topic must not be delivered.
    await producer!.send({
      topic: BIND_TOPIC,
      liteTopic,
      keys: [ 'lite-unsub-key' ],
      body: Buffer.from('lite-unsub-body'),
    });

    const views = await consumer!.receive(10, 15000);
    assert.strictEqual(views.length, 0);
  });

  it('should reject blank lite topic names', async () => {
    await startClientPair();
    await assert.rejects(async () => {
      await consumer!.subscribeLite('  ');
    }, /liteTopic should not be blank/);
    await assert.rejects(async () => {
      await consumer!.unsubscribeLite('');
    }, /liteTopic should not be blank/);
  });
});
