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
 * Test for FIFO Consume Accelerator
 */

import { describe, it } from 'node:test';
import assert = require('node:assert');
import { FifoConsumeService } from '../../src/consumer/FifoConsumeService';
import { MessageListener } from '../../src/consumer/MessageListener';
import { ConsumeResult } from '../../src/consumer/ConsumeResult';
import type { MessageView } from '../../src/message';

describe('FifoConsumeService', () => {
  it('should support enableFifoConsumeAccelerator option', () => {
    const messageListener: MessageListener = {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      async consume(_messageView: MessageView): Promise<ConsumeResult> {
        return ConsumeResult.SUCCESS;
      },
    };

    // Create with accelerator enabled
    const serviceWithAccelerator = new FifoConsumeService('test-client', messageListener, true);
    assert.ok(serviceWithAccelerator, 'Should create service with accelerator enabled');

    // Create with accelerator disabled (default)
    const serviceWithoutAccelerator = new FifoConsumeService('test-client', messageListener, false);
    assert.ok(serviceWithoutAccelerator, 'Should create service with accelerator disabled');

    // Create without specifying (default to false)
    const serviceDefault = new FifoConsumeService('test-client', messageListener);
    assert.ok(serviceDefault, 'Should create service with default accelerator setting');
  });
});
