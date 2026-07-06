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

/**
 * Standard consume service for lite push consumer.
 *
 * <p>Similar to {@link StandardConsumeService}, but uses
 * {@link ProcessQueue#eraseFifoMessage} for local retry on failure, avoiding
 * frequent server requests.</p>
 */
export class LiteStandardConsumeService extends ConsumeService {
  readonly #logger: ILogger;

  constructor(clientId: string, messageListener: MessageListener) {
    super(clientId, messageListener);
    this.#logger = getDefaultLogger();
  }

  consume(pq: ProcessQueue, messageViews: MessageView[]): void {
    if (!pq || !messageViews) {
      this.#logger.error('[Bug] Invalid arguments for consume, pq=%s, messageViews=%s, clientId=%s', pq, messageViews, this.clientId);
      return;
    }

    this.#logger.debug?.('LiteStandardConsumeService.consume called, messageCount=%d, clientId=%s', messageViews.length, this.clientId);

    for (const messageView of messageViews) {
      if (messageView.corrupted) {
        this.#logger.error('Message is corrupted for lite standard consumption, prepare to discard it, mq=%s, '
          + 'messageId=%s, clientId=%s',
        pq.getMessageQueue(), messageView.messageId, this.clientId);
        pq.discardMessage(messageView);
        continue;
      }

      this.#logger.debug?.('Consuming lite message, messageId=%s, topic=%s, liteTopic=%s, clientId=%s',
        messageView.messageId, messageView.topic, messageView.liteTopic, this.clientId);

      this.consumeMessage(messageView)
        .then(result => {
          this.#logger.debug?.('Lite message consumed successfully, messageId=%s, result=%s, clientId=%s',
            messageView.messageId, result, this.clientId);
          // Use eraseFifoMessage for local retry on failure, avoiding frequent server requests.
          pq.eraseFifoMessage(messageView, result);
        })
        .catch(err => {
          // Should never reach here.
          this.#logger.error('[Bug] Exception raised in lite standard consumption callback, clientId=%s, error=%s',
            this.clientId, err);
        });
    }
  }
}
