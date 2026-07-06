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

import { MessageView } from '../message';
import { ConsumeResultSuspend } from './ConsumeResult';
import { FifoConsumeService } from './FifoConsumeService';
import { MessageListener } from './MessageListener';
import type { ProcessQueue } from './ProcessQueue';
import { ILogger, getDefaultLogger } from '../client/Logger';

/**
 * FIFO consume service for lite push consumer.
 *
 * <p>Similar to {@link FifoConsumeService}, but groups messages by lite topic
 * instead of message group. This ensures messages with the same lite topic are
 * consumed sequentially, while messages with different lite topics can be
 * consumed in parallel when the accelerator is enabled.</p>
 *
 * <p>When the message listener returns {@link ConsumeResultSuspend}, all
 * remaining messages with the same lite topic in the current batch are also
 * suspended, while messages with different lite topics continue to be consumed.</p>
 */
export class LiteFifoConsumeService extends FifoConsumeService {
  readonly #logger: ILogger;

  constructor(clientId: string, messageListener: MessageListener, enableFifoConsumeAccelerator?: boolean) {
    super(clientId, messageListener, enableFifoConsumeAccelerator);
    this.#logger = getDefaultLogger();
  }

  /**
   * Use lite topic as the group key for FIFO consumption.
   *
   * @param messageView - The message view
   * @return {string} The lite topic, empty string if not present
   */
  protected getMessageGroupKey(messageView: MessageView): string {
    return messageView.liteTopic || '';
  }

  protected consumeIteratively(pq: ProcessQueue, messageViews: MessageView[], startIndex: number): Promise<void> {
    if (!pq || !messageViews) {
      this.#logger.error('[Bug] Invalid arguments for consumeIteratively, pq=%s, messageViews=%s, clientId=%s',
        pq, messageViews, this.clientId);
      return Promise.resolve();
    }

    const next = this.getNextValidMessage(pq, messageViews, startIndex);
    if (!next) {
      return Promise.resolve();
    }
    const { messageView, nextIndex } = next;

    return this.consumeMessage(messageView)
      .then(async result => {
        await pq.eraseFifoMessage(messageView, result);
        return result;
      })
      .then(async result => {
        if (result instanceof ConsumeResultSuspend) {
          const currentLiteTopic = messageView.liteTopic ?? '';
          const continuation: MessageView[] = [];
          const suspendTasks: Array<Promise<void>> = [];

          // Suspend all messages with the same liteTopic in this batch.
          for (let i = nextIndex; i < messageViews.length; i++) {
            const msg = messageViews[i];
            if ((msg.liteTopic ?? '') === currentLiteTopic) {
              suspendTasks.push(pq.eraseFifoMessage(msg, result));
            } else {
              continuation.push(msg);
            }
          }

          this.#logger.debug?.('LiteFifoConsumeService suspend, liteTopic=%s, suspendCount=%d, continuationCount=%d, '
            + 'clientId=%s', currentLiteTopic, suspendTasks.length, continuation.length, this.clientId);

          await Promise.all(suspendTasks);
          return this.consumeIteratively(pq, continuation, 0);
        }
        return this.consumeIteratively(pq, messageViews, nextIndex);
      })
      .catch(err => {
        this.#logger.error('[Bug] Exception raised in lite FIFO consumption callback, clientId=%s, error=%s',
          this.clientId, err);
        return this.consumeIteratively(pq, messageViews, nextIndex);
      });
  }
}
