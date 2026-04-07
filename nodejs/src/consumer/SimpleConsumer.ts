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

import { ClientType } from '../../proto/apache/rocketmq/v2/definition_pb';
import { HeartbeatRequest, NotifyClientTerminationRequest } from '../../proto/apache/rocketmq/v2/service_pb';
import { MessageView } from '../message';
import { TopicRouteData } from '../route';
import { createResource } from '../util';
import { FilterExpression } from './FilterExpression';
import { SimpleSubscriptionSettings } from './SimpleSubscriptionSettings';
import { SubscriptionLoadBalancer } from './SubscriptionLoadBalancer';
import { Consumer, ConsumerOptions } from './Consumer';

const RANDOM_INDEX = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);

export interface SimpleConsumerOptions extends ConsumerOptions {
  /**
   * support tag string as filter, e.g.:
   * ```ts
   * new Map()
   *  .set('TestTopic1', 'TestTag1')
   *  .set('TestTopic2', 'TestTag2')
   * ```
   */
  subscriptions: Map<string/* topic */, FilterExpression | string>;
  /**
   * set await duration for long-polling, default is 30000ms
   */
  awaitDuration?: number;
}

export class SimpleConsumer extends Consumer {
  readonly #simpleSubscriptionSettings: SimpleSubscriptionSettings;
  readonly #subscriptionExpressions = new Map<string, FilterExpression>();
  readonly #subscriptionRouteDataCache = new Map<string, SubscriptionLoadBalancer>();
  readonly #awaitDuration: number;
  #topicIndex = RANDOM_INDEX;

  constructor(options: SimpleConsumerOptions) {
    options.topics = Array.from(options.subscriptions.keys());
    super(options);
    for (const [ topic, filter ] of options.subscriptions.entries()) {
      if (typeof filter === 'string') {
        // filter is tag string
        this.#subscriptionExpressions.set(topic, new FilterExpression(filter));
      } else {
        this.#subscriptionExpressions.set(topic, filter);
      }
    }
    this.#awaitDuration = options.awaitDuration ?? 30000;
    this.#simpleSubscriptionSettings = new SimpleSubscriptionSettings(options.namespace, this.clientId,
      this.getClientType(), this.endpoints, this.consumerGroup, this.requestTimeout,
      this.#awaitDuration, this.#subscriptionExpressions);
  }

  protected getSettings() {
    return this.#simpleSubscriptionSettings;
  }

  /**
   * Get the client type.
   *
   * @return The client type identifier for simple consumer
   */
  protected getClientType(): ClientType {
    return ClientType.SIMPLE_CONSUMER;
  }

  protected wrapHeartbeatRequest() {
    return new HeartbeatRequest()
      .setClientType(this.getClientType())
      .setGroup(createResource(this.consumerGroup));
  }

  protected wrapNotifyClientTerminationRequest() {
    return new NotifyClientTerminationRequest()
      .setGroup(createResource(this.consumerGroup));
  }

  protected onTopicRouteDataUpdate(topic: string, topicRouteData: TopicRouteData) {
    this.#updateSubscriptionLoadBalancer(topic, topicRouteData);
  }

  #updateSubscriptionLoadBalancer(topic: string, topicRouteData: TopicRouteData) {
    let subscriptionLoadBalancer = this.#subscriptionRouteDataCache.get(topic);
    if (!subscriptionLoadBalancer) {
      subscriptionLoadBalancer = new SubscriptionLoadBalancer(topicRouteData);
    } else {
      subscriptionLoadBalancer = subscriptionLoadBalancer.update(topicRouteData);
    }
    this.#subscriptionRouteDataCache.set(topic, subscriptionLoadBalancer);
    return subscriptionLoadBalancer;
  }

  async #getSubscriptionLoadBalancer(topic: string) {
    let loadBalancer = this.#subscriptionRouteDataCache.get(topic);
    if (!loadBalancer) {
      const topicRouteData = await this.getRouteData(topic);
      loadBalancer = this.#updateSubscriptionLoadBalancer(topic, topicRouteData);
    }
    return loadBalancer;
  }

  async subscribe(topic: string, filterExpression: FilterExpression) {
    await this.getRouteData(topic);
    this.#subscriptionExpressions.set(topic, filterExpression);
  }

  unsubscribe(topic: string) {
    this.#subscriptionExpressions.delete(topic);
  }

  async receive(maxMessageNum = 10, invisibleDuration = 15000) {
    if (maxMessageNum <= 0) {
      throw new Error(`maxMessageNum must be greater than 0, but got ${maxMessageNum}`);
    }

    const topic = this.#nextTopic();
    const filterExpression = this.#subscriptionExpressions.get(topic);
    if (!filterExpression) {
      throw new Error(`No subscription found for topic=${topic}, please subscribe first`);
    }

    const loadBalancer = await this.#getSubscriptionLoadBalancer(topic);
    const mq = loadBalancer.takeMessageQueue();
    const request = this.wrapReceiveMessageRequest(maxMessageNum, mq, filterExpression,
      invisibleDuration, this.#awaitDuration);
    return await this.receiveMessage(request, mq, this.#awaitDuration);
  }

  async ack(message: MessageView) {
    await this.ackMessage(message);
  }

  async changeInvisibleDuration(message: MessageView, invisibleDuration: number) {
    const response = await this.invisibleDuration(message, invisibleDuration);
    // Refresh receipt handle manually
    (message as any).receiptHandle = response;
  }

  #nextTopic() {
    const topics = Array.from(this.#subscriptionExpressions.keys());
    if (topics.length === 0) {
      throw new Error('No subscriptions available to receive messages');
    }
    const index = Math.abs(this.#topicIndex++ % topics.length);
    return topics[index];
  }
}
