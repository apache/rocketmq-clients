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
 * Test for LitePushConsumerBuilder
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { LitePushConsumerBuilder } from '../../src';
import { BaseClientOptions } from '../../src/client';
import { ConsumeResult } from '../../src';

describe('LitePushConsumerBuilder', () => {
  const createMockClientConfig = (): BaseClientOptions => {
    return {
      endpoints: '127.0.0.1:8080',
      namespace: 'test-namespace',
    };
  };

  const createMockMessageListener = () => {
    return {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      consume: async (_messageView: any) => {
        // Mock listener
        return ConsumeResult.SUCCESS;
      },
    };
  };

  describe('bindTopic()', () => {
    it('should accept valid topic', () => {
      const builder = new LitePushConsumerBuilder();
      const result = builder.bindTopic('test-topic');
      assert.strictEqual(result, builder);
    });

    it('should throw error for blank topic', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.bindTopic('');
      }, /bindTopic should not be blank/);
    });

    it('should throw error for null topic', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.bindTopic(null as any);
      }, /bindTopic should not be blank/);
    });
  });

  describe('setConsumerGroup()', () => {
    it('should accept valid consumer group', () => {
      const builder = new LitePushConsumerBuilder();
      const result = builder.setConsumerGroup('test-group');
      assert.strictEqual(result, builder);
    });

    it('should throw error for null consumer group', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setConsumerGroup(null as any);
      }, /consumerGroup should not be null/);
    });

    it('should throw error for invalid consumer group format', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setConsumerGroup('invalid@group');
      }, /does not match the pattern/);
    });
  });

  describe('setMessageListener()', () => {
    it('should accept valid message listener', () => {
      const builder = new LitePushConsumerBuilder();
      const listener = createMockMessageListener();
      const result = builder.setMessageListener(listener);
      assert.strictEqual(result, builder);
    });

    it('should throw error for null message listener', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setMessageListener(null as any);
      }, /messageListener should not be null/);
    });
  });

  describe('setMaxCacheMessageCount()', () => {
    it('should accept positive value', () => {
      const builder = new LitePushConsumerBuilder();
      const result = builder.setMaxCacheMessageCount(512);
      assert.strictEqual(result, builder);
    });

    it('should throw error for zero value', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setMaxCacheMessageCount(0);
      }, /maxCacheMessageCount should be positive/);
    });

    it('should throw error for negative value', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setMaxCacheMessageCount(-1);
      }, /maxCacheMessageCount should be positive/);
    });
  });

  describe('setMaxCacheMessageSizeInBytes()', () => {
    it('should accept positive value', () => {
      const builder = new LitePushConsumerBuilder();
      const result = builder.setMaxCacheMessageSizeInBytes(32 * 1024 * 1024);
      assert.strictEqual(result, builder);
    });

    it('should throw error for zero value', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setMaxCacheMessageSizeInBytes(0);
      }, /maxCacheMessageSizeInBytes should be positive/);
    });
  });

  describe('setConsumptionThreadCount()', () => {
    it('should accept positive value', () => {
      const builder = new LitePushConsumerBuilder();
      const result = builder.setConsumptionThreadCount(10);
      assert.strictEqual(result, builder);
    });

    it('should throw error for zero value', () => {
      const builder = new LitePushConsumerBuilder();
      assert.throws(() => {
        builder.setConsumptionThreadCount(0);
      }, /consumptionThreadCount should be positive/);
    });
  });

  describe('build()', () => {
    it('should throw error when clientConfiguration not set', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setConsumerGroup('test-group');
      builder.setMessageListener(createMockMessageListener());
      builder.bindTopic('test-topic');

      await assert.rejects(() => builder.build(), /clientConfiguration has not been set yet/);
    });

    it('should accept valid configuration and build', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setClientConfiguration(createMockClientConfig());
      builder.setConsumerGroup('test-group');
      builder.setMessageListener(createMockMessageListener());
      builder.bindTopic('test-topic');

      // Should reject with "not implemented" error since we haven't implemented the full consumer yet
      await assert.rejects(
        () => builder.build(),
        /LitePushConsumerImpl not yet implemented/,
      );
    });

    it('should throw error when consumerGroup not set', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setClientConfiguration(createMockClientConfig());
      builder.setMessageListener(createMockMessageListener());
      builder.bindTopic('test-topic');

      await assert.rejects(() => builder.build(), /consumerGroup has not been set yet/);
    });

    it('should throw error when messageListener not set', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setClientConfiguration(createMockClientConfig());
      builder.setConsumerGroup('test-group');
      builder.bindTopic('test-topic');

      await assert.rejects(() => builder.build(), /messageListener has not been set yet/);
    });

    it('should throw error when bindTopic not set', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setClientConfiguration(createMockClientConfig());
      builder.setConsumerGroup('test-group');
      builder.setMessageListener(createMockMessageListener());

      await assert.rejects(() => builder.build(), /bindTopic has not been set yet/);
    });

    it('should throw not implemented error when all params set', async () => {
      const builder = new LitePushConsumerBuilder();
      builder.setClientConfiguration(createMockClientConfig());
      builder.setConsumerGroup('test-group');
      builder.setMessageListener(createMockMessageListener());
      builder.bindTopic('test-topic');

      await assert.rejects(
        () => builder.build(),
        /LitePushConsumerImpl not yet implemented/,
      );
    });
  });
});
