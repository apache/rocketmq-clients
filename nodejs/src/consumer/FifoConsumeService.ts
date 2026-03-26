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
import { ConsumeService } from './ConsumeService';
import { MessageListener } from './MessageListener';
import type { ProcessQueue } from './ProcessQueue';

export class FifoConsumeService extends ConsumeService {
  readonly #enableAccelerator: boolean;
  readonly #groupProcessing = new Map<string /* messageGroup */, Promise<void>>();

  constructor(clientId: string, messageListener: MessageListener, enableAccelerator?: boolean) {
    super(clientId, messageListener);
    this.#enableAccelerator = enableAccelerator ?? false;
  }

  consume(pq: ProcessQueue, messageViews: MessageView[]): void {
    // Use iterative consumption when accelerator is disabled or only one message
    if (!this.#enableAccelerator || messageViews.length <= 1) {
      this.#consumeIteratively(pq, messageViews, 0);
      return;
    }
    this.#consumeWithAccelerator(pq, messageViews);
  }

  /**
   * FIFO consume accelerator mode:
   * - Messages with the same messageGroup are consumed sequentially
   * - Messages with different messageGroups are consumed in parallel
   * - Messages without messageGroup are consumed in parallel
   */
  #consumeWithAccelerator(pq: ProcessQueue, messageViews: MessageView[]): void {
    // Group messages by messageGroup
    const groupedMessages = new Map<string, MessageView[]>();
    const ungroupedMessages: MessageView[] = [];

    for (const messageView of messageViews) {
      if (messageView.corrupted) {
        pq.discardFifoMessage(messageView);
        continue;
      }

      const messageGroup = messageView.messageGroup || '';
      if (messageGroup) {
        const group = groupedMessages.get(messageGroup) || [];
        group.push(messageView);
        groupedMessages.set(messageGroup, group);
      } else {
        ungroupedMessages.push(messageView);
      }
    }

    // Log parallel consumption info
    const groupCount = groupedMessages.size + (ungroupedMessages.length > 0 ? 1 : 0);
    console.debug('FifoConsumeService parallel consume, messageViewsNum=%d, groupNum=%d',
      messageViews.length, groupCount);

    // Process grouped messages (each group sequentially, groups in parallel)
    for (const [ , messages ] of groupedMessages.entries()) {
      this.#processMessageGroup(pq, messages);
    }

    // Process ungrouped messages in parallel
    for (const messageView of ungroupedMessages) {
      this.consumeMessage(messageView)
        .then(result => pq.eraseFifoMessage(messageView, result))
        .catch(() => {
          // Error already logged, continue with next message
        });
    }
  }

  /**
   * Process messages within the same group sequentially
   */
  async #processMessageGroup(pq: ProcessQueue, messages: MessageView[]): Promise<void> {
    // Check if there's already processing happening for this group
    const messageGroup = messages[0]?.messageGroup || 'NO_GROUP';
    const existingPromise = this.#groupProcessing.get(messageGroup);
    const processTask = (async () => {
      // Wait for previous processing to complete if any
      if (existingPromise) {
        await existingPromise.catch(() => {
          // Ignore previous errors, continue processing
        });
      }

      // Process messages in sequence
      for (const messageView of messages) {
        try {
          const result = await this.consumeMessage(messageView);
          await pq.eraseFifoMessage(messageView, result);
        } catch (error) {
          console.error('Failed to process FIFO message, messageGroup=%s, messageId=%s',
            messageGroup, messageView.messageId, error);
          // Continue with next message even if current fails
        }
      }
    })();

    // Store the promise for this group
    this.#groupProcessing.set(messageGroup, processTask);

    // Clean up when done
    processTask.finally(() => {
      this.#groupProcessing.delete(messageGroup);
    });
  }

  #consumeIteratively(pq: ProcessQueue, messageViews: MessageView[], index: number): void {
    if (index >= messageViews.length) {
      return;
    }

    const messageView = messageViews[index];

    if (messageView.corrupted) {
      pq.discardFifoMessage(messageView);
      this.#consumeIteratively(pq, messageViews, index + 1);
      return;
    }

    this.consumeMessage(messageView)
      .then(result => pq.eraseFifoMessage(messageView, result))
      .then(() => this.#consumeIteratively(pq, messageViews, index + 1))
      .catch(() => this.#consumeIteratively(pq, messageViews, index + 1));
  }
}
