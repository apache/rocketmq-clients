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
import { getDefaultLogger } from '../client';

export class StandardConsumeService extends ConsumeService {
  private readonly logger = getDefaultLogger();

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(clientId: string, messageListener: MessageListener) {
    super(clientId, messageListener);
  }

  consume(pq: ProcessQueue, messageViews: MessageView[]): void {
    this.logger.debug?.('StandardConsumeService.consume called, messageCount=%d', messageViews.length);
    for (const messageView of messageViews) {
      if (messageView.corrupted) {
        this.logger.warn('Message corrupted, discarding, messageId=%s', messageView.messageId);
        pq.discardMessage(messageView);
        continue;
      }

      this.logger.debug?.('Consuming message, messageId=%s, topic=%s',
        messageView.messageId, messageView.topic);

      this.consumeMessage(messageView)
        .then(result => {
          this.logger.debug?.('Message consumed successfully, messageId=%s, result=%s',
            messageView.messageId, result);
          pq.eraseMessage(messageView, result);
        })
        .catch(err => {
          this.logger.error('Message consumption failed, messageId=%s, error=%s',
            messageView.messageId, err);
          // Should never reach here.
        });
    }
  }
}
