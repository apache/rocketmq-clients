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

import {
  Settings as SettingsPB,
  ClientType,
  Subscription,
  RetryPolicy as RetryPolicyPB,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import { Endpoints } from '../route';
import { Settings, UserAgent } from '../client';
import { CustomizedBackoffRetryPolicy, ExponentialBackoffRetryPolicy, RetryPolicy } from '../retry';
import { createDuration, createResource } from '../util';
import { FilterExpression } from './FilterExpression';

export class PushSubscriptionSettings extends Settings {
  readonly #group: string;
  readonly #subscriptionExpressions: Map<string, FilterExpression>;
  #fifo = false;
  #receiveBatchSize = 32;
  #longPollingTimeout = 30000; // ms
  #consumeConcurrentlyMax = 32; // mirrors Java PushConsumerSettings default

  constructor(
    namespace: string,
    clientId: string,
    clientType: ClientType,
    accessPoint: Endpoints,
    consumerGroup: string,
    requestTimeout: number,
    subscriptionExpressions: Map<string, FilterExpression>,
    longPollingTimeout?: number,
    consumeConcurrentlyMax?: number,
  ) {
    super(namespace, clientId, clientType, accessPoint, requestTimeout);
    this.#group = consumerGroup;
    this.#subscriptionExpressions = subscriptionExpressions;
    if (longPollingTimeout !== undefined) {
      this.#longPollingTimeout = longPollingTimeout;
    }
    if (consumeConcurrentlyMax !== undefined) {
      this.#consumeConcurrentlyMax = consumeConcurrentlyMax;
    }
  }

  isFifo(): boolean {
    return this.#fifo;
  }

  getReceiveBatchSize(): number {
    return this.#receiveBatchSize;
  }

  /**
   * Maximum number of messages that may be in-flight (fetched but not yet
   * settled) per process queue. The Node client previously had no equivalent of
   * the Java {@code ProcessQueueImpl} permit, so consumption could grow
   * unbounded; this bounds it and feeds the {@link ProcessQueue} semaphore.
   */
  getConsumeConcurrentlyMax(): number {
    return this.#consumeConcurrentlyMax;
  }

  getLongPollingTimeout(): number {
    return this.#longPollingTimeout;
  }

  getRetryPolicy(): RetryPolicy | undefined {
    return this.retryPolicy;
  }

  toProtobuf(): SettingsPB {
    const subscription = new Subscription()
      .setGroup(createResource(this.#group));

    for (const [ topic, filterExpression ] of this.#subscriptionExpressions.entries()) {
      subscription.addSubscriptions()
        .setTopic(createResource(topic))
        .setExpression(filterExpression.toProtobuf());
    }

    return new SettingsPB()
      .setClientType(this.clientType)
      .setAccessPoint(this.accessPoint.toProtobuf())
      .setRequestTimeout(createDuration(this.requestTimeout))
      .setSubscription(subscription)
      .setUserAgent(UserAgent.INSTANCE.toProtobuf());
  }

  sync(settings: SettingsPB): void {
    if (settings.getPubSubCase() !== SettingsPB.PubSubCase.SUBSCRIPTION) {
      return;
    }
    const subscription = settings.getSubscription();
    if (subscription) {
      this.#fifo = subscription.getFifo() ?? false;
      this.#receiveBatchSize = subscription.getReceiveBatchSize() ?? 32;
      const longPollingTimeout = subscription.getLongPollingTimeout();
      if (longPollingTimeout) {
        this.#longPollingTimeout = longPollingTimeout.getSeconds() * 1000 +
          Math.floor(longPollingTimeout.getNanos() / 1000000);
      }
      // consumeConcurrentlyMax is absent from the generated proto in this fork;
      // guard the accessor so we keep the default (32) until the field lands.
      const consumeConcurrentlyMax = (subscription as { getConsumeConcurrentlyMax?: () => number | null }).getConsumeConcurrentlyMax?.();
      if (typeof consumeConcurrentlyMax === 'number' && consumeConcurrentlyMax > 0) {
        this.#consumeConcurrentlyMax = consumeConcurrentlyMax;
      }
    }
    const backoffPolicy = settings.getBackoffPolicy();
    if (backoffPolicy) {
      // Convert protobuf Duration (seconds + nanos) to milliseconds without
      // losing sub-second precision.
      const toMillis = (duration?: Duration) =>
        duration ? duration.getSeconds() * 1000 + duration.getNanos() / 1e6 : 0;
      switch (backoffPolicy.getStrategyCase()) {
        case RetryPolicyPB.StrategyCase.EXPONENTIAL_BACKOFF: {
          const exponential = backoffPolicy.getExponentialBackoff()!;
          this.retryPolicy = new ExponentialBackoffRetryPolicy(
            backoffPolicy.getMaxAttempts(),
            toMillis(exponential.getInitial()),
            toMillis(exponential.getMax()),
            exponential.getMultiplier(),
          );
          break;
        }
        case RetryPolicyPB.StrategyCase.CUSTOMIZED_BACKOFF: {
          const customizedBackoff = backoffPolicy.getCustomizedBackoff()!;
          const durations = customizedBackoff.getNextList().map((duration: Duration) => toMillis(duration));
          if (durations.length > 0) {
            this.retryPolicy = new CustomizedBackoffRetryPolicy(durations, backoffPolicy.getMaxAttempts());
          }
          break;
        }
        default:
          break;
      }
    }
  }
}
