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
import {
  getTimestamp,
  calculateStringSipHash24,
} from '../../src/util';

describe('test/util/index.test.ts', () => {
  describe('getTimestamp()', () => {
    it('should work', () => {
      const timestamp = getTimestamp();
      assert(timestamp.seconds);
      assert(timestamp.nanos);
    });
  });

  describe('calculateStringSipHash24()', () => {
    it('should work', () => {
      assert.equal(calculateStringSipHash24('foo哈哈😄2222哈哈'), 11716758754047899126n);
      assert.equal(calculateStringSipHash24('foo哈哈😄2222哈哈') % 3n, 2n);
    });
  });
});
