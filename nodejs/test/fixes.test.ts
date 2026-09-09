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
 * Regression tests for defect fixes aligned with the Java client:
 *  - Retry backoff unit contract (milliseconds, sub-second precision kept)
 *  - CustomizedBackoffRetryPolicy implementation
 *  - Endpoints string parsing (IPv6 / http(s) prefix)
 *  - StatusChecker mappings (ILLEGAL_LITE_TOPIC / MESSAGE_BODY_EMPTY /
 *    LITE_SUBSCRIPTION_QUOTA_EXCEEDED)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import {
  ExponentialBackoffRetryPolicy,
  CustomizedBackoffRetryPolicy,
} from '../src/retry';
import { Endpoints } from '../src/route';
import {
  StatusChecker,
  BadRequestException,
  PayloadEmptyException,
  LiteSubscriptionQuotaExceededException,
} from '../src/exception';
import { Code, RetryPolicy as RetryPolicyPB, ExponentialBackoff, CustomizedBackoff } from '../proto/apache/rocketmq/v2/definition_pb';
import { Status } from '../proto/apache/rocketmq/v2/definition_pb';

function statusOf(code: Code): Status.AsObject {
  return { code, message: 'mock message', requestId: '' } as unknown as Status.AsObject;
}

describe('ExponentialBackoffRetryPolicy (ms contract)', () => {
  it('should return delays in milliseconds', () => {
    const policy = new ExponentialBackoffRetryPolicy(3, 1000, 10000, 2);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 2000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 4000);
    assert.strictEqual(policy.getNextAttemptDelay(4), 8000);
    // capped by maxBackoff
    assert.strictEqual(policy.getNextAttemptDelay(5), 10000);
  });

  it('should keep sub-second precision when inheriting backoff', () => {
    const retryPolicy = new RetryPolicyPB().setExponentialBackoff(
      new ExponentialBackoff()
        .setInitial(new Duration().setSeconds(0).setNanos(500000000)) // 0.5s
        .setMax(new Duration().setSeconds(3).setNanos(250000000)) // 3.25s
        .setMultiplier(2));
    const policy = new ExponentialBackoffRetryPolicy(3).inheritBackoff(retryPolicy) as ExponentialBackoffRetryPolicy;
    assert.strictEqual(policy.getNextAttemptDelay(1), 500);
    assert.strictEqual(policy.getNextAttemptDelay(2), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 2000);
    // capped by max backoff (3.25s, sub-second precision kept)
    assert.strictEqual(policy.getNextAttemptDelay(4), 3250);
  });

  it('should serialize back to protobuf without losing precision', () => {
    const policy = new ExponentialBackoffRetryPolicy(3, 500, 3250, 2);
    const pb = policy.toProtobuf();
    const exponential = pb.getExponentialBackoff()!;
    assert.strictEqual(exponential.getInitial()!.getSeconds(), 0);
    assert.strictEqual(exponential.getInitial()!.getNanos(), 500000000);
    assert.strictEqual(exponential.getMax()!.getSeconds(), 3);
    assert.strictEqual(exponential.getMax()!.getNanos(), 250000000);
  });
});

describe('CustomizedBackoffRetryPolicy', () => {
  it('should return the Nth duration and clamp to the last one', () => {
    const policy = new CustomizedBackoffRetryPolicy([1000, 5000, 10000], 5);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 5000);
    assert.strictEqual(policy.getNextAttemptDelay(3), 10000);
    assert.strictEqual(policy.getNextAttemptDelay(4), 10000);
    assert.strictEqual(policy.getMaxAttempts(), 5);
  });

  it('should inherit customized backoff from protobuf', () => {
    const customizedBackoff = new CustomizedBackoff();
    customizedBackoff.addNext(new Duration().setSeconds(1));
    customizedBackoff.addNext(new Duration().setNanos(500000000));
    const retryPolicy = new RetryPolicyPB()
      .setMaxAttempts(4)
      .setCustomizedBackoff(customizedBackoff);
    const policy = new CustomizedBackoffRetryPolicy([100], 3).inheritBackoff(retryPolicy);
    assert.strictEqual(policy.getMaxAttempts(), 3);
    assert.strictEqual(policy.getNextAttemptDelay(1), 1000);
    assert.strictEqual(policy.getNextAttemptDelay(2), 500);
  });
});

describe('Endpoints parsing', () => {
  it('should parse IPv4 endpoints with port', () => {
    const endpoints = new Endpoints('127.0.0.1:10911');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: '127.0.0.1', port: 10911 }]);
    assert.strictEqual(endpoints.scheme, 1); // IPV4
  });

  it('should parse domain endpoints', () => {
    const endpoints = new Endpoints('example.com:443');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: 'example.com', port: 443 }]);
    assert.strictEqual(endpoints.scheme, 3); // DOMAIN_NAME
  });

  it('should parse bracketed IPv6 endpoints with port', () => {
    const endpoints = new Endpoints('[::1]:10911');
    assert.deepStrictEqual(endpoints.addressesList, [{ host: '::1', port: 10911 }]);
    assert.strictEqual(endpoints.scheme, 2); // IPV6
  });

  it('should parse bare IPv6 endpoints without port', () => {
    const endpoints = new Endpoints('1050:0000:0000:0000:0005:0600:300c:326b');
    assert.strictEqual(endpoints.addressesList[0].host, '1050:0000:0000:0000:0005:0600:300c:326b');
    assert.strictEqual(endpoints.addressesList[0].port, 80);
    assert.strictEqual(endpoints.scheme, 2); // IPV6
  });

  it('should strip http(s) prefixes and parse multiple endpoints', () => {
    const endpoints = new Endpoints('http://127.0.0.1:10911;https://example.com:8080');
    assert.strictEqual(endpoints.addressesList.length, 2);
    assert.deepStrictEqual(endpoints.addressesList[0], { host: '127.0.0.1', port: 10911 });
    assert.deepStrictEqual(endpoints.addressesList[1], { host: 'example.com', port: 8080 });
  });
});

describe('StatusChecker mappings vs Java', () => {
  it('should map ILLEGAL_LITE_TOPIC to BadRequestException', () => {
    assert.throws(() => StatusChecker.check(statusOf(Code.ILLEGAL_LITE_TOPIC)), BadRequestException);
  });

  it('should map MESSAGE_BODY_EMPTY to PayloadEmptyException', () => {
    assert.throws(() => StatusChecker.check(statusOf(Code.MESSAGE_BODY_EMPTY)), PayloadEmptyException);
  });

  it('should map LITE_SUBSCRIPTION_QUOTA_EXCEEDED to LiteSubscriptionQuotaExceededException', () => {
    assert.throws(
      () => StatusChecker.check(statusOf(Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED)),
      LiteSubscriptionQuotaExceededException);
  });
});
