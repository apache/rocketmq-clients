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
  constructor(clientId: string, messageListener: MessageListener) {
    super(clientId, messageListener);
  }

  consume(pq: ProcessQueue, messageViews: MessageView[]): void {
    this.#consumeIteratively(pq, messageViews, 0);
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
