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
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import * as sinon from 'sinon';
import {
  createDuration,
  getTimestamp,
  calculateStringSipHash24,
  crc32CheckSum, md5CheckSum, sha1CheckSum, hashCodeOfString, getRequestDateTime, sign, createResource,
} from '../../src/util';

describe('test/util/index.test.ts', () => {
  describe('getTimestamp()', () => {
    it('should work', () => {
      const timestamp = getTimestamp();
      assert(timestamp.seconds);
      assert(timestamp.nanos);
    });
  });

  describe('createDuration()', () => {
    const cases = [
      [ 0, 0, 0 ], [ -0, 0, 0 ], [ 0.000001, 0, 1 ], [ 0.1234567, 0, 123456 ],
      [ 999.999999, 0, 999999999 ], [ 1000, 1, 0 ], [ 1000.5, 1, 500000 ],
      [ -0.5, 0, -500000 ], [ -999, 0, -999000000 ], [ -1000, -1, 0 ], [ -1000.5, -1, -500000 ],
      [ -2173, -2, -173000000 ], [ 315576000000000, 315576000000, 0 ],
      [ -315576000000000, -315576000000, 0 ],
    ];
    for (const [ ms, seconds, nanos ] of cases) {
      it(`${ms}ms 序列化保留整数 seconds、nanos 与同号余数`, () => {
        const duration = createDuration(ms);
        assert.equal(duration.getSeconds(), seconds);
        assert.equal(duration.getNanos(), nanos);
        assert.deepEqual(Duration.deserializeBinary(duration.serializeBinary()).toObject(), duration.toObject());
      });
    }
    for (const ms of [ NaN, Infinity, -Infinity, 315576000000001, -315576000000001 ]) {
      it(`${ms} 在调用时抛 RangeError`, () => assert.throws(() => createDuration(ms), RangeError));
    }
    it('should split whole seconds correctly', () => {
      const duration = createDuration(2000);
      assert.equal(duration.getSeconds(), 2);
      assert.equal(duration.getNanos(), 0);
      assert(duration.serializeBinary().length > 0);
    });

    it('should split non-whole-second milliseconds correctly', () => {
      const duration = createDuration(2173);
      assert.equal(duration.getSeconds(), 2);
      assert.equal(duration.getNanos(), 173000000);
      // protobuf Duration.seconds is int64, serialization must not throw
      assert(duration.serializeBinary().length > 0);
    });
  });

  describe('Java 固定向量', () => {
    // JShell 21 + Guava Hashing.sipHash24().hashBytes(UTF_8).asLong()。
    const vectors = [
      [ '', 0, 8246050544436514353n ], [ 'abc', 96354, 6754548778392356773n ],
      [ 'RocketMQ', -453554280, -4355033068952641245n ], [ '你好', 652829, -4706515592422728576n ],
      [ '😄', 1772903, -3031427512150344328n ], [ 'a😄z', 57849842, 5994498846521405229n ],
      [ 'foo哈哈😄2222哈哈', -1149599763, -705383726186849630n ],
    ] as const;
    for (const [ value, hashCode, javaSipHash ] of vectors) {
      it(`${value} 的 UTF-16 hashCode 与 SipHash 字节对齐 Java`, () => {
        assert.equal(hashCodeOfString(value), hashCode);
        // Node 公开结果为摘要字节的无符号大端值；Java asLong 为小端有符号值。
        const bytes = Buffer.alloc(8);
        bytes.writeBigUInt64BE(calculateStringSipHash24(value));
        assert.equal(bytes.readBigInt64LE(), javaSipHash);
      });
    }
  });

  describe('摘要、签名与请求时间', () => {
    const vectors = [
      [ '', '0', 'D41D8CD98F00B204E9800998ECF8427E', 'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709' ],
      [ '123456789', '3421780262', '25F9E794323B453885F5181F1B624D0B', 'F7C3BC1D808E04732ADF679965CCC34CA7AE3441' ],
      [ 'abc', '891568578', '900150983CD24FB0D6963F7D28E17F72', 'A9993E364706816ABA3E25717850C26C9CD0D89D' ],
    ];
    for (const [ input, crc32, md5, sha1 ] of vectors) {
      it(`${input} 的标准摘要向量`, () => {
        const bytes = Buffer.from(input);
        assert.equal(crc32CheckSum(bytes), crc32);
        assert.equal(md5CheckSum(bytes), md5);
        assert.equal(sha1CheckSum(bytes), sha1);
      });
    }
    it('HMAC-SHA1 使用大写十六进制', () => {
      assert.equal(sign('key', 'The quick brown fox jumps over the lazy dog'), 'DE7C9B85B8B78AA6BC8A7A36F70A90701C9DB4D9');
    });
    it('请求日期使用 UTC 格式且资源默认 namespace 为空', () => {
      const clock = sinon.useFakeTimers({ now: Date.parse('2026-11-18T12:34:56.789Z'), toFake: [ 'Date' ] });
      try {
        assert.equal(getRequestDateTime(), '20261118T123456Z');
        assert.deepEqual(createResource('test-topic').toObject(), { name: 'test-topic', resourceNamespace: '' });
      } finally {
        clock.restore();
      }
    });
  });

  describe('calculateStringSipHash24()', () => {
    it('should work', () => {
      assert.equal(calculateStringSipHash24('foo哈哈😄2222哈哈'), 11716758754047899126n);
      assert.equal(calculateStringSipHash24('foo哈哈😄2222哈哈') % 3n, 2n);
    });
  });
});
