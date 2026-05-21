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
 * Test for LiteSubscriptionQuotaExceededException
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { LiteSubscriptionQuotaExceededException } from '../../src/exception/LiteSubscriptionQuotaExceededException';

describe('LiteSubscriptionQuotaExceededException', () => {
  it('should create exception with correct properties', () => {
    const responseCode = 429;
    const requestId = 'test-request-id';
    const message = 'Lite subscription quota exceeded 100';

    const exception = new LiteSubscriptionQuotaExceededException(
      responseCode,
      requestId,
      message,
    );

    assert.strictEqual(exception.code, responseCode);
    assert.ok(exception.message.includes(requestId));
    assert.ok(exception.message.includes(message));
    assert.strictEqual(exception.name, 'LiteSubscriptionQuotaExceededException');
  });

  it('should create exception with null requestId', () => {
    const responseCode = 429;
    const message = 'Quota exceeded';

    const exception = new LiteSubscriptionQuotaExceededException(
      responseCode,
      null,
      message,
    );

    assert.strictEqual(exception.code, responseCode);
    assert.ok(exception.message.includes(message));
  });

  it('should be instance of Error', () => {
    const exception = new LiteSubscriptionQuotaExceededException(
      429,
      null,
      'Test error',
    );

    assert.ok(exception instanceof Error);
    assert.ok(exception instanceof LiteSubscriptionQuotaExceededException);
  });

  it('should have proper stack trace', () => {
    try {
      throw new LiteSubscriptionQuotaExceededException(
        429,
        'request-123',
        'Test quota exceeded error',
      );
    } catch (error: any) {
      assert.ok(error.stack);
      assert.ok(error.stack.includes('LiteSubscriptionQuotaExceededException'));
    }
  });
});
