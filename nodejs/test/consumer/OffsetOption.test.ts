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
 * Test for OffsetOption
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { OffsetOption, OffsetType } from '../../src/consumer/OffsetOption';

describe('OffsetOption', () => {
  describe('Predefined constants', () => {
    it('should have LAST_OFFSET constant', () => {
      assert.strictEqual(OffsetOption.LAST_OFFSET.getType(), OffsetType.POLICY);
      assert.strictEqual(OffsetOption.LAST_OFFSET.getValue(), BigInt(0));
    });

    it('should have MIN_OFFSET constant', () => {
      assert.strictEqual(OffsetOption.MIN_OFFSET.getType(), OffsetType.POLICY);
      assert.strictEqual(OffsetOption.MIN_OFFSET.getValue(), BigInt(1));
    });

    it('should have MAX_OFFSET constant', () => {
      assert.strictEqual(OffsetOption.MAX_OFFSET.getType(), OffsetType.POLICY);
      assert.strictEqual(OffsetOption.MAX_OFFSET.getValue(), BigInt(2));
    });
  });

  describe('ofOffset()', () => {
    it('should create offset from number', () => {
      const option = OffsetOption.ofOffset(100);
      assert.strictEqual(option.getType(), OffsetType.OFFSET);
      assert.strictEqual(option.getValue(), BigInt(100));
    });

    it('should create offset from bigint', () => {
      const option = OffsetOption.ofOffset(BigInt(100));
      assert.strictEqual(option.getType(), OffsetType.OFFSET);
      assert.strictEqual(option.getValue(), BigInt(100));
    });

    it('should throw error for negative offset', () => {
      assert.throws(() => {
        OffsetOption.ofOffset(-1);
      }, /offset must be greater than or equal to 0/);
    });
  });

  describe('ofTailN()', () => {
    it('should create tailN from number', () => {
      const option = OffsetOption.ofTailN(50);
      assert.strictEqual(option.getType(), OffsetType.TAIL_N);
      assert.strictEqual(option.getValue(), BigInt(50));
    });

    it('should create tailN from bigint', () => {
      const option = OffsetOption.ofTailN(BigInt(50));
      assert.strictEqual(option.getType(), OffsetType.TAIL_N);
      assert.strictEqual(option.getValue(), BigInt(50));
    });

    it('should throw error for negative tailN', () => {
      assert.throws(() => {
        OffsetOption.ofTailN(-1);
      }, /tailN must be greater than or equal to 0/);
    });
  });

  describe('ofTimestamp()', () => {
    it('should create timestamp from number', () => {
      const timestamp = Date.now();
      const option = OffsetOption.ofTimestamp(timestamp);
      assert.strictEqual(option.getType(), OffsetType.TIMESTAMP);
      assert.strictEqual(option.getValue(), BigInt(timestamp));
    });

    it('should create timestamp from bigint', () => {
      const timestamp = BigInt(Date.now());
      const option = OffsetOption.ofTimestamp(timestamp);
      assert.strictEqual(option.getType(), OffsetType.TIMESTAMP);
      assert.strictEqual(option.getValue(), timestamp);
    });

    it('should throw error for negative timestamp', () => {
      assert.throws(() => {
        OffsetOption.ofTimestamp(-1);
      }, /timestamp must be greater than or equal to 0/);
    });
  });

  describe('toString()', () => {
    it('should return string representation', () => {
      const option = OffsetOption.ofOffset(100);
      const str = option.toString();
      assert.ok(str.includes('type=OFFSET'));
      assert.ok(str.includes('value=100'));
    });
  });
});
