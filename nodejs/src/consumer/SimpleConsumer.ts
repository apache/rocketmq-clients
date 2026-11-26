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
import { InternalErrorException } from '../exception';

const RANDOM_INDEX = Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
const DEFAULT_MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 1000;

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
  /**
   * max retry attempts for temporary errors (e.g., internal server error), default is 3
   * Includes the initial request and must be a positive safe integer; 1 disables retries.
   * Unlike Java's single receive attempt, Node retains its existing retry configuration.
   * Once routing is ready, the maximum RPC wait and backoff budget is
   * maxRetryAttempts * (requestTimeout + awaitDuration) + (maxRetryAttempts - 1) * 1000ms,
   * excluding route queries, message decoding, hooks, and event-loop scheduling.
   */
  maxRetryAttempts?: number;
}

export class SimpleConsumer extends Consumer {
  readonly #simpleSubscriptionSettings: SimpleSubscriptionSettings;
  readonly #subscriptionExpressions = new Map<string, FilterExpression>();
  readonly #subscriptionRouteDataCache = new Map<string, SubscriptionLoadBalancer>();
  readonly #awaitDuration: number;
  readonly #maxRetryAttempts: number;
  #topicIndex = RANDOM_INDEX;

  constructor(options: SimpleConsumerOptions) {
    if (options.awaitDuration !== undefined && (!Number.isFinite(options.awaitDuration) || options.awaitDuration <= 0)) {
      throw new TypeError('awaitDuration should be positive');
    }
    if (!(options.subscriptions instanceof Map) || options.subscriptions.size === 0) {
      throw new TypeError('subscriptions must be a non-empty Map');
    }
    if (options.maxRetryAttempts !== undefined &&
      (!Number.isSafeInteger(options.maxRetryAttempts) || options.maxRetryAttempts <= 0)) {
      throw new TypeError('maxRetryAttempts must be a positive safe integer');
    }
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
    this.#maxRetryAttempts = options.maxRetryAttempts ?? DEFAULT_MAX_RETRY_ATTEMPTS;
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
      .setGroup(createResource(this.consumerGroup, this.namespace));
  }

  protected wrapNotifyClientTerminationRequest() {
    return new NotifyClientTerminationRequest()
      .setGroup(createResource(this.consumerGroup, this.namespace));
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
    if (!this.isRunning()) {
      throw new Error('Simple consumer is not running, please call startup() first or check shutdown state');
    }
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

    for (let attempt = 1; ; attempt++) {
      try {
        return await this.receiveMessage(request, mq, this.#awaitDuration);
      } catch (err) {
        if (!(err instanceof InternalErrorException) || attempt >= this.#maxRetryAttempts) {
          throw err;
        }
        this.logger.warn('Received temporary error from server, will retry after %dms, attempt=%d/%d, topic=%s, clientId=%s, error=%s',
          RETRY_DELAY_MS, attempt, this.#maxRetryAttempts, topic, this.clientId, err.message);
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      }
    }
  }

  async ack(message: MessageView) {
    if (!this.isRunning()) {
      throw new Error('Simple consumer is not running, cannot ack message');
    }
    await this.ackMessage(message);
  }

  async changeInvisibleDuration(message: MessageView, invisibleDuration: number) {
    if (!this.isRunning()) {
      throw new Error('Simple consumer is not running, cannot change invisible duration');
    }
    const response = await this.invisibleDuration(message, invisibleDuration);
    this.#refreshReceiptHandle(message, response);
  }

  #refreshReceiptHandle(message: MessageView, receiptHandle: string) {
    // Update the caller-readonly receiptHandle after successful renewal without changing MessageView's public signature.
    const mutableMessage = message as { receiptHandle: string };
    mutableMessage.receiptHandle = receiptHandle;
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
