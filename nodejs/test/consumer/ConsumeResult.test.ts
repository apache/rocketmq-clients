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

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { ConsumeResult, ConsumeResultSuspend } from '../../src/consumer/ConsumeResult';

describe('ConsumeResult', () => {
  it('should have SUCCESS and FAILURE singletons', () => {
    assert.strictEqual(ConsumeResult.SUCCESS.name, 'SUCCESS');
    assert.strictEqual(ConsumeResult.FAILURE.name, 'FAILURE');
    assert.strictEqual(ConsumeResult.SUCCESS.toString(), 'SUCCESS');
    assert.strictEqual(ConsumeResult.FAILURE.toString(), 'FAILURE');
    assert.strictEqual(ConsumeResult.SUCCESS, ConsumeResult.SUCCESS);
  });

  it('should create ConsumeResultSuspend with valid suspend time', () => {
    const suspend = ConsumeResultSuspend.of(100);
    assert.strictEqual(suspend.name, 'SUSPEND');
    assert.strictEqual(suspend.suspendTimeMs, 100);
    assert.strictEqual(suspend.toString(), 'SUSPEND(100ms)');
  });

  it('should reject suspend time less than 50ms', () => {
    assert.throws(() => {
      ConsumeResultSuspend.of(49);
    }, /suspend time cannot be less than 50ms/);
  });
});
