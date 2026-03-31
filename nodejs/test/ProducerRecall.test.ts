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
import { RecallReceipt } from '../src/producer/RecallReceipt';

describe('RecallReceipt', () => {
  it('should create recall receipt with messageId', () => {
    const messageId = 'test-message-id-12345';
    const receipt = new RecallReceipt(messageId);
    assert.strictEqual(receipt.messageId, messageId);
    assert.ok(receipt.toString().includes(messageId));
  });
});
