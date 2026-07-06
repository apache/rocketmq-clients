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
import { ILogger, getDefaultLogger } from '../client/Logger';

export class FifoConsumeService extends ConsumeService {
  readonly #enableFifoConsumeAccelerator: boolean;
  readonly #logger: ILogger;

  constructor(clientId: string, messageListener: MessageListener, enableFifoConsumeAccelerator?: boolean) {
    super(clientId, messageListener);
    this.#enableFifoConsumeAccelerator = enableFifoConsumeAccelerator ?? false;
    this.#logger = getDefaultLogger();
  }

  consume(pq: ProcessQueue, messageViews: MessageView[]): void {
    if (!pq || !messageViews) {
      this.#logger.error('[Bug] Invalid arguments for consume, pq=%s, messageViews=%s, clientId=%s',
        pq, messageViews, this.clientId);
      return;
    }
    if (!this.#enableFifoConsumeAccelerator || messageViews.length <= 1) {
      this.consumeIteratively(pq, messageViews, 0).catch(() => {
        // Error already logged, ignore
      });
      return;
    }

    // Group messages by group key. Default to null-key group for unkeyed messages.
    const messageViewsGroupByGroupKey = new Map<string, MessageView[]>();
    for (const messageView of messageViews) {
      const groupKey = this.getMessageGroupKey(messageView);
      let group = messageViewsGroupByGroupKey.get(groupKey);
      if (!group) {
        group = [];
        messageViewsGroupByGroupKey.set(groupKey, group);
      }
      group.push(messageView);
    }

    this.#logger.debug?.('FifoConsumeService parallel consume, messageViewsNum=%d, groupNum=%d',
      messageViews.length, messageViewsGroupByGroupKey.size);

    for (const list of messageViewsGroupByGroupKey.values()) {
      this.consumeIteratively(pq, list, 0).catch(() => {
        // Error already logged, ignore
      });
    }
  }

  /**
   * Get the group key for the given message view.
   * Subclasses can override this method to provide different grouping logic.
   *
   * @param messageView - The message view
   * @return {string} The group key, empty string means no grouping
   */
  protected getMessageGroupKey(messageView: MessageView): string {
    return messageView.messageGroup || '';
  }

  /**
   * Consume messages iteratively using the provided message view array.
   * This method handles corrupted messages and continues processing the next valid message.
   *
   * @param pq - The process queue
   * @param messageViews - The message views to consume
   * @param startIndex - The index to start consuming from
   * @return {Promise<void>} Promise that resolves when all messages are consumed
   */
  protected consumeIteratively(pq: ProcessQueue, messageViews: MessageView[], startIndex: number): Promise<void> {
    const next = this.getNextValidMessage(pq, messageViews, startIndex);
    if (!next) {
      return Promise.resolve();
    }
    const { messageView, nextIndex } = next;

    return this.consumeMessage(messageView)
      .then(result => pq.eraseFifoMessage(messageView, result))
      .then(() => this.consumeIteratively(pq, messageViews, nextIndex))
      .catch(() => this.consumeIteratively(pq, messageViews, nextIndex));
  }

  /**
   * Get the next valid message from the message view array.
   * Skip corrupted messages and return the first valid one.
   * Returns null if there are no more messages.
   *
   * @param pq - The process queue
   * @param messageViews - The message views
   * @param startIndex - The index to start searching from
   * @return {{ messageView: MessageView; nextIndex: number } | null} The next valid message and the index after it, or null
   */
  protected getNextValidMessage(
    pq: ProcessQueue,
    messageViews: MessageView[],
    startIndex: number,
  ): { messageView: MessageView; nextIndex: number } | null {
    for (let i = startIndex; i < messageViews.length; i++) {
      const messageView = messageViews[i];
      if (messageView.corrupted) {
        // Discard corrupted message.
        this.#logger.error('Message is corrupted for FIFO consumption, prepare to discard it, mq=%s, '
          + 'messageId=%s, clientId=%s',
        pq.getMessageQueue(), messageView.messageId, this.clientId);
        pq.discardFifoMessage(messageView);
        continue;
      }
      return { messageView, nextIndex: i + 1 };
    }
    return null;
  }
}
