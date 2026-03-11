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
import { Endpoints } from '../route';
import { Settings, UserAgent } from '../client';
import { ExponentialBackoffRetryPolicy, RetryPolicy } from '../retry';
import { createDuration, createResource } from '../util';
import { FilterExpression } from './FilterExpression';

export class PushSubscriptionSettings extends Settings {
  readonly #group: string;
  readonly #subscriptionExpressions: Map<string, FilterExpression>;
  #fifo = false;
  #receiveBatchSize = 32;
  #longPollingTimeout = 30000; // ms

  constructor(
    namespace: string,
    clientId: string,
    accessPoint: Endpoints,
    consumerGroup: string,
    requestTimeout: number,
    subscriptionExpressions: Map<string, FilterExpression>,
  ) {
    super(namespace, clientId, ClientType.PUSH_CONSUMER, accessPoint, requestTimeout);
    this.#group = consumerGroup;
    this.#subscriptionExpressions = subscriptionExpressions;
  }

  isFifo(): boolean {
    return this.#fifo;
  }

  getReceiveBatchSize(): number {
    return this.#receiveBatchSize;
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

    for (const [topic, filterExpression] of this.#subscriptionExpressions.entries()) {
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
      this.#fifo = subscription.getFifo();
      this.#receiveBatchSize = subscription.getReceiveBatchSize();
      const longPollingTimeout = subscription.getLongPollingTimeout();
      if (longPollingTimeout) {
        this.#longPollingTimeout = longPollingTimeout.getSeconds() * 1000 +
          Math.floor(longPollingTimeout.getNanos() / 1000000);
      }
    }
    const backoffPolicy = settings.getBackoffPolicy();
    if (backoffPolicy) {
      switch (backoffPolicy.getStrategyCase()) {
        case RetryPolicyPB.StrategyCase.EXPONENTIAL_BACKOFF: {
          const exponential = backoffPolicy.getExponentialBackoff()!.toObject();
          this.retryPolicy = new ExponentialBackoffRetryPolicy(
            backoffPolicy.getMaxAttempts(),
            exponential.initial?.seconds,
            exponential.max?.seconds,
            exponential.multiplier,
          );
          break;
        }
        case RetryPolicyPB.StrategyCase.CUSTOMIZED_BACKOFF:
          // CustomizedBackoffRetryPolicy not yet implemented in Node.js
          break;
        default:
          break;
      }
    }
  }
}
