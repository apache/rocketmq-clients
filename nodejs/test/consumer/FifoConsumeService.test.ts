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
