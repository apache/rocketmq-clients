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

import { randomInt } from 'node:crypto';
import { Code, MessageQueue as MessageQueuePB, Permission } from '../../proto/apache/rocketmq/v2/definition_pb';
import { NotFoundException } from '../exception/NotFoundException';
import { MASTER_BROKER_ID } from '../util';
import { Endpoints } from './Endpoints';
import { MessageQueue } from './MessageQueue';

export class TopicRouteData {
  readonly messageQueues: MessageQueue[] = [];
  // Randomize the round-robin starting point to spread initial assignment queries across clients.
  #index = randomInt(0, 2 ** 31 - 1);

  constructor(messageQueues: MessageQueuePB[]) {
    for (const mq of messageQueues) {
      this.messageQueues.push(new MessageQueue(mq));
    }
  }

  getTotalEndpoints() {
    const endpointsMap = new Map<string, Endpoints>();
    for (const mq of this.messageQueues) {
      endpointsMap.set(mq.broker.endpoints.facade, mq.broker.endpoints);
    }
    return Array.from(endpointsMap.values());
  }

  /**
   * Rotate across master endpoints with non-NONE permissions, matching Java's selection rules.
   */
  pickEndpointsToQueryAssignments(): Endpoints {
    const size = this.messageQueues.length;
    for (let i = 0; i < size; i++) {
      const mq = this.messageQueues[this.#index % size];
      this.#index = (this.#index + 1) % size;
      if (mq.broker.id !== MASTER_BROKER_ID || mq.permission === Permission.NONE) {
        continue;
      }
      return mq.broker.endpoints;
    }
    throw new NotFoundException(Code.NOT_FOUND, 'Failed to pick endpoints to query assignment');
  }

  /**
   * Get the count of message queues.
   * @return number of message queues
   */
  getQueueCount(): number {
    return this.messageQueues.length;
  }

  /**
   * Check if the route data is empty.
   * @return true if no message queues available
   */
  isEmpty(): boolean {
    return this.messageQueues.length === 0;
  }

  equals(other: TopicRouteData): boolean {
    if (this === other) return true;
    if (!other) return false;
    if (this.messageQueues.length !== other.messageQueues.length) return false;
    for (let i = 0; i < this.messageQueues.length; i++) {
      if (!this.messageQueues[i].equals(other.messageQueues[i])) {
        return false;
      }
    }
    return true;
  }

  hashCode(): number {
    let hash = 17;
    for (const mq of this.messageQueues) {
      // eslint-disable-next-line no-bitwise
      hash = (hash * 31 + mq.hashCode()) | 0;
    }
    return hash;
  }
}
