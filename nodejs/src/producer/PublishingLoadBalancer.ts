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
    const hashCode = calculateStringSipHash24(messageGroup);
    const index = parseInt(`${hashCode % BigInt(this.#messageQueues.length)}`);
    return this.#messageQueues[index];
  }
}
