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
import { Semaphore } from '../../src/util/Semaphore';

describe('Semaphore (consumption permit primitive)', () => {
  it('starts with the configured number of permits', () => {
    const s = new Semaphore(3);
    assert.strictEqual(s.availablePermits, 3);
  });

  it('tryAcquire consumes a permit and returns false when exhausted', () => {
    const s = new Semaphore(1);
    assert.strictEqual(s.tryAcquire(), true);
    assert.strictEqual(s.availablePermits, 0);
    assert.strictEqual(s.tryAcquire(), false);
    assert.strictEqual(s.availablePermits, 0);
  });

  it('rejects non-negative integer construction', () => {
    assert.throws(() => new Semaphore(-1), RangeError);
    assert.throws(() => new Semaphore(1.5), RangeError);
  });

  it('acquire resolves immediately when a permit is available', async () => {
    const s = new Semaphore(1);
    await s.acquire();
    assert.strictEqual(s.availablePermits, 0);
  });

  it('acquire blocks until a permit is released, then resumes', async () => {
    const s = new Semaphore(0);
    let resumed = false;
    const p = s.acquire().then(() => { resumed = true; });
    // Permit not yet available.
    await new Promise(r => setTimeout(r, 10));
    assert.strictEqual(resumed, false);
    s.release();
    await p;
    assert.strictEqual(resumed, true);
  });

  it('serves multiple waiters in FIFO order on successive releases', async () => {
    const s = new Semaphore(0);
    const order: number[] = [];
    const a = s.acquire().then(() => order.push(1));
    const b = s.acquire().then(() => order.push(2));
    const c = s.acquire().then(() => order.push(3));
    s.release();
    s.release();
    s.release();
    await Promise.all([a, b, c]);
    assert.deepStrictEqual(order, [1, 2, 3]);
    assert.strictEqual(s.availablePermits, 0);
  });

  it('release hands the permit to a waiter without incrementing the pool', () => {
    const s = new Semaphore(0);
    const p = s.acquire();
    s.release(); // should go straight to the waiter, not the pool
    assert.strictEqual(s.availablePermits, 0);
    return p;
  });
});
