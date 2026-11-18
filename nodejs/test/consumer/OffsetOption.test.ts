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

import * as assert from 'node:assert';
import { OffsetOption, OffsetType } from '../../src/consumer/OffsetOption';
import { OffsetOption as ProtoOffsetOption } from '../../proto/apache/rocketmq/v2/definition_pb';

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

  describe('协议与数值边界', () => {
    it('三种 policy 都使用对应的 oneof 字段', () => {
      for (const [ option, value ] of [
        [ OffsetOption.LAST_OFFSET, 0 ], [ OffsetOption.MIN_OFFSET, 1 ], [ OffsetOption.MAX_OFFSET, 2 ],
      ] as const) {
        const proto = option.toProtobuf();
        assert.strictEqual(proto.getOffsetTypeCase(), ProtoOffsetOption.OffsetTypeCase.POLICY);
        assert.strictEqual(proto.getPolicy(), value);
      }
    });

    const factories = [
      { name: 'offset', create: OffsetOption.ofOffset, type: OffsetType.OFFSET,
        protoCase: ProtoOffsetOption.OffsetTypeCase.OFFSET, read: (p: ProtoOffsetOption) => p.getOffset() },
      { name: 'tailN', create: OffsetOption.ofTailN, type: OffsetType.TAIL_N,
        protoCase: ProtoOffsetOption.OffsetTypeCase.TAIL_N, read: (p: ProtoOffsetOption) => p.getTailN() },
      { name: 'timestamp', create: OffsetOption.ofTimestamp, type: OffsetType.TIMESTAMP,
        protoCase: ProtoOffsetOption.OffsetTypeCase.TIMESTAMP, read: (p: ProtoOffsetOption) => p.getTimestamp() },
    ];
    for (const { name, create, type, protoCase, read } of factories) {
      it(`${name} 接受非负安全整数 number 和 bigint 并保留协议字段`, () => {
        for (const value of [ 0, 1, Number.MAX_SAFE_INTEGER - 1, Number.MAX_SAFE_INTEGER ]) {
          for (const input of [ value, BigInt(value) ]) {
            const option = create(input);
            assert.strictEqual(option.getType(), type);
            assert.strictEqual(option.getValue(), BigInt(value));
            const proto = option.toProtobuf();
            assert.strictEqual(proto.getOffsetTypeCase(), protoCase);
            assert.strictEqual(read(proto), value);
          }
        }
      });

      it(`${name} 在工厂调用时拒绝不安全 number、NaN、无限值和小数`, () => {
        for (const value of [ Number.MAX_SAFE_INTEGER + 1, Number.MAX_SAFE_INTEGER + 2,
          Number.MIN_SAFE_INTEGER - 1, NaN, Infinity, -Infinity, 0.5 ]) {
          assert.throws(() => create(value), RangeError);
        }
      });

      it(`${name} 保留负数校验和超大 bigint 的延迟序列化校验`, () => {
        for (const value of [ -1, BigInt(-1) ]) {
          assert.throws(() => create(value), new RegExp(`${name} must be greater than or equal to 0`));
        }
        const value = BigInt(Number.MAX_SAFE_INTEGER) + BigInt(1);
        const option = create(value);
        assert.strictEqual(option.getValue(), value);
        assert.throws(() => option.toProtobuf(), /out of safe number range/);
      });
    }

    it('equals 比较类型和值；hashCode 对相等对象稳定', () => {
      const option = OffsetOption.ofOffset(1);
      assert.ok(option.equals(option));
      assert.ok(option.equals(OffsetOption.ofOffset(BigInt(1))));
      assert.ok(!option.equals(OffsetOption.ofOffset(2)));
      assert.ok(!option.equals(OffsetOption.ofTailN(1)));
      assert.ok(!option.equals(null as unknown as OffsetOption));
      assert.strictEqual(option.hashCode(), OffsetOption.ofOffset(BigInt(1)).hashCode());
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
