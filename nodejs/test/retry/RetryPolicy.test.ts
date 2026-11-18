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
import { RetryPolicy as RetryPolicyPB, ExponentialBackoff } from '../../proto/apache/rocketmq/v2/definition_pb';
import { ExponentialBackoffRetryPolicy, CustomizedBackoffRetryPolicy } from '../../src/retry';

function wireCopy(policy: RetryPolicyPB) {
  return RetryPolicyPB.deserializeBinary(policy.serializeBinary());
}

describe('RetryPolicy 对齐 Java 的毫秒行为', () => {
  const exponential = new ExponentialBackoffRetryPolicy(7, 125.5, 1000.25, 2);
  const customized = new CustomizedBackoffRetryPolicy([ 0, 125.5, 2500.25 ], 7);
  for (const [ attempt, delay ] of [[ 1, 125.5 ], [ 2, 251 ], [ 3, 502 ], [ 4, 1000.25 ], [ 100000, 1000.25 ]]) {
    it(`指数策略 attempt=${attempt} 延迟=${delay}`, () => assert.equal(exponential.getNextAttemptDelay(attempt), delay));
  }
  for (const [ attempt, delay ] of [[ 1, 0 ], [ 2, 125.5 ], [ 3, 2500.25 ], [ 4, 2500.25 ], [ 100000, 2500.25 ]]) {
    it(`自定义策略 attempt=${attempt} 延迟=${delay}`, () => assert.equal(customized.getNextAttemptDelay(attempt), delay));
  }

  for (const policy of [ exponential, customized ]) {
    for (const attempt of [ 0, -1, NaN ]) {
      it(`${policy.constructor.name} 拒绝非正 attempt ${attempt}`, () => assert.throws(() => policy.getNextAttemptDelay(attempt)));
    }
    it(`${policy.constructor.name} protobuf 往返保留亚秒精度，inherit 保留本地 maxAttempts`, () => {
      const protobuf = wireCopy(policy.toProtobuf().setMaxAttempts(99));
      const inherited = policy.inheritBackoff(protobuf);
      assert.equal(inherited.getMaxAttempts(), 7);
      assert.equal(policy.getMaxAttempts(), 7);
      for (const attempt of [ 1, 2, 3, 4, 100000 ]) {
        assert.equal(inherited.getNextAttemptDelay(attempt), policy.getNextAttemptDelay(attempt));
      }
      assert.deepEqual(wireCopy(inherited.toProtobuf()).toObject(), wireCopy(policy.toProtobuf()).toObject());
    });
    it(`${policy.constructor.name} 拒绝缺失 strategy`, () => assert.throws(() => policy.inheritBackoff(new RetryPolicyPB())));
  }

  it('两个策略拒绝继承另一种 strategy', () => {
    assert.throws(() => exponential.inheritBackoff(customized.toProtobuf()));
    assert.throws(() => customized.inheritBackoff(exponential.toProtobuf()));
  });

  it('立即重试及默认构造保持零延迟', () => {
    for (const policy of [ ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(3), new ExponentialBackoffRetryPolicy(3) ]) {
      assert.equal(policy.getMaxAttempts(), 3);
      assert.equal(policy.getNextAttemptDelay(1), 0);
      assert.equal(policy.getNextAttemptDelay(100000), 0);
      assert.equal(wireCopy(policy.toProtobuf()).getMaxAttempts(), 3);
    }
    assert.equal(new ExponentialBackoffRetryPolicy(3, -1, 100, 2).getNextAttemptDelay(1), 0);
  });

  it('指数策略继承缺省 Duration 使用零值', () => {
    const inherited = exponential.inheritBackoff(new RetryPolicyPB().setExponentialBackoff(new ExponentialBackoff().setMultiplier(1)));
    assert.equal(inherited.getNextAttemptDelay(1), 0);
  });

  it('自定义策略拒绝空 durations，Duration 转换保留纳秒', () => {
    assert.throws(() => new CustomizedBackoffRetryPolicy([], 3));
    assert.deepEqual(CustomizedBackoffRetryPolicy.durationsToMillis(customized.toProtobuf().getCustomizedBackoff()!), [ 0, 125.5, 2500.25 ]);
  });
});
