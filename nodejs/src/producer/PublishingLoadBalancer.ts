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
import { Permission } from '../../proto/apache/rocketmq/v2/definition_pb';
import { Endpoints, MessageQueue, TopicRouteData } from '../route';
import { MASTER_BROKER_ID, calculateStringSipHash24 } from '../util';

export class PublishingLoadBalancer {
  #index: number;
  #messageQueues: MessageQueue[];

  constructor(topicRouteData: TopicRouteData, index?: number) {
    this.#messageQueues = topicRouteData.messageQueues.filter(mq => {
      return mq.queueId === MASTER_BROKER_ID && (mq.permission === Permission.WRITE || mq.permission === Permission.READ_WRITE);
    });
    this.#index = index === undefined ? randomInt(this.#messageQueues.length) : index;
    if (this.#messageQueues.length === 0) {
      throw new Error(`No writable message queue found, topicRouteData=${JSON.stringify(topicRouteData)}`);
    }
  }

  update(topicRouteData: TopicRouteData) {
    return new PublishingLoadBalancer(topicRouteData, this.#index);
  }

  takeMessageQueues(excluded: Map<string, Endpoints>, count: number) {
    if (this.#index >= this.#messageQueues.length) {
      this.#index = 0;
    }
    let next = this.#index++;
    const candidates: MessageQueue[] = [];
    const candidateBrokerNames = new Set<string>();

    const size = this.#messageQueues.length;
    for (let i = 0; i < size; i++) {
      const messageQueue = this.#messageQueues[next++ % size];
      const broker = messageQueue.broker;
      const brokerName = broker.name;
      if (!excluded.has(broker.endpoints.facade) && !candidateBrokerNames.has(brokerName)) {
        candidateBrokerNames.add(brokerName);
        candidates.push(messageQueue);
      }
      if (candidates.length >= count) {
        return candidates;
      }
    }
    // If all endpoints are isolated.
    if (candidates.length === 0) {
      for (let i = 0; i < size; i++) {
        const messageQueue = this.#messageQueues[next++ % size];
        const broker = messageQueue.broker;
        const brokerName = broker.name;
        if (!candidateBrokerNames.has(brokerName)) {
          candidateBrokerNames.add(brokerName);
          candidates.push(messageQueue);
        }
        if (candidates.length >= count) {
          return candidates;
        }
      }
    }
    return candidates;
  }

  takeMessageQueueByMessageGroup(messageGroup: string) {
    // SipHash-2-4 returns an unsigned 64-bit value in JS (readBigUInt64BE), while
    // Java's String.hashCode-style paths use signed semantics. Interpret the hash as
    // a signed 64-bit integer and apply floorMod so the result is always non-negative,
    // exactly matching Java LongMath.mod(hashCode, size) — otherwise FIFO messages
    // with "negative" hashes route to different queues than the Java client.
    const hashCode = BigInt.asIntN(64, calculateStringSipHash24(messageGroup));
    const size = this.#messageQueues.length;
    const index = Number(((hashCode % BigInt(size)) + BigInt(size)) % BigInt(size));
    return this.#messageQueues[index];
  }

  equals(other: PublishingLoadBalancer): boolean {
    if (this === other) return true;
    if (!other) return false;
    if (this.#messageQueues.length !== other.#messageQueues.length) return false;
    for (let i = 0; i < this.#messageQueues.length; i++) {
      if (!this.#messageQueues[i].equals(other.#messageQueues[i])) {
        return false;
      }
    }
    return true;
  }

  hashCode(): number {
    let hash = 17;
    for (const mq of this.#messageQueues) {
      // eslint-disable-next-line no-bitwise
      hash = (hash * 31 + mq.hashCode()) | 0;
    }
    return hash;
  }
}
