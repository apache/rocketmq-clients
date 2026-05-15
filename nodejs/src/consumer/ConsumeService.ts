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
import { ConsumeResult } from './ConsumeResult';
import { ConsumeTask } from './ConsumeTask';
import { MessageListener } from './MessageListener';
import type { ProcessQueue } from './ProcessQueue';

export abstract class ConsumeService {
  protected readonly clientId: string;
  readonly #messageListener: MessageListener;
  #aborted = false;
  #pendingTimers: Set<NodeJS.Timeout> = new Set();

  constructor(clientId: string, messageListener: MessageListener) {
    this.clientId = clientId;
    this.#messageListener = messageListener;
  }

  abstract consume(pq: ProcessQueue, messageViews: MessageView[]): void;

  async consumeMessage(messageView: MessageView, delay = 0): Promise<ConsumeResult> {
    if (this.#aborted) {
      return ConsumeResult.FAILURE;
    }
    const task = new ConsumeTask(this.clientId, this.#messageListener, messageView);
    if (delay <= 0) {
      return this.#executeTask(task);
    }
    return new Promise(resolve => {
      const timer = setTimeout(async () => {
        this.#pendingTimers.delete(timer);
        if (this.#aborted) {
          resolve(ConsumeResult.FAILURE);
          return;
        }
        const result = await this.#executeTask(task);
        resolve(result);
      }, delay);
      this.#pendingTimers.add(timer);
    });
  }

  async #executeTask(task: ConsumeTask): Promise<ConsumeResult> {
    try {
      return await task.call();
    } catch (error) {
      return ConsumeResult.FAILURE;
    }
  }
  abort() {
    this.#aborted = true;
    for (const timer of this.#pendingTimers) {
      clearTimeout(timer);
    }
    this.#pendingTimers.clear();
  }
}
