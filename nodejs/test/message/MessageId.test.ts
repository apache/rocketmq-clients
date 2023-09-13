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
import { MessageIdFactory } from '../../src';

describe('test/message/MessageId.test.ts', () => {
  describe('MessageIdFactory()', () => {
    it('should decode success', async () => {
      const messageId = MessageIdFactory.decode('0156F7E71C361B21BC024CCDBE00000000');
      // console.log('messageId %o, toString %s', messageId, messageId);
      assert.equal(messageId.version, 1);
      assert.equal(messageId.macAddress, '56f7e71c361b');
      assert.equal(messageId.processId, 8636);
      assert.equal(messageId.timestamp, 38587838);
      assert.equal(messageId.sequence, 0);
      // support lower case
      const messageId2 = MessageIdFactory.decode('0156F7E71C361B21BC024CCDBE00000000'.toLowerCase());
      assert.equal(messageId2.version, 1);
      assert.equal(messageId2.toString().toUpperCase(), messageId.toString());
    });

    it('should create success', async () => {
      const messageId = MessageIdFactory.create();
      // console.log('messageId %o, toString %s', messageId, messageId);
      assert.equal(messageId.version, 1);
      const decodeMessageId = MessageIdFactory.decode(messageId.toString());
      assert.equal(decodeMessageId.toString(), messageId.toString());
      assert.deepEqual(decodeMessageId, messageId);
      const messageId2 = MessageIdFactory.create();
      assert.equal(messageId2.sequence, messageId.sequence + 1);
      // console.log('messageId2 %o, toString %s', messageId2, messageId2);
    });
  });
});
