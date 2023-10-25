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
import { MessageQueue, TopicRouteData } from '../route';
import { MASTER_BROKER_ID } from '../util';

export class SubscriptionLoadBalancer {
  #index: number;
  #messageQueues: MessageQueue[];

  constructor(topicRouteData: TopicRouteData, index?: number) {
    this.#messageQueues = topicRouteData.messageQueues.filter(mq => {
      return mq.queueId === MASTER_BROKER_ID && (mq.permission === Permission.READ || mq.permission === Permission.READ_WRITE);
    });
    this.#index = index === undefined ? randomInt(this.#messageQueues.length) : index;
    if (this.#messageQueues.length === 0) {
      throw new Error(`No readable message queue found, topicRouteData=${JSON.stringify(topicRouteData)}`);
    }
  }

  update(topicRouteData: TopicRouteData) {
    return new SubscriptionLoadBalancer(topicRouteData, this.#index);
  }

  takeMessageQueue() {
    if (this.#index >= this.#messageQueues.length) {
      this.#index = 0;
    }
    return this.#messageQueues[this.#index++];
  }
}
