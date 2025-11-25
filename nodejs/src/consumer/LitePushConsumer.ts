/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { PushConsumer, MessageListener } from './PushConsumer';
import { ConsumerOptions } from './Consumer';
import { SubscriptionEntry } from './SubscriptionEntry';
import { FilterType } from '../../proto/apache/rocketmq/v2/definition_pb';
import { createResource } from '../util';

/**
 * LitePushConsumer is a lightweight push consumer.
 *
 * @export
 * @class LitePushConsumer
 * @extends {PushConsumer}
 */
export class LitePushConsumer extends PushConsumer {
  private readonly subscriptions: Map<string, SubscriptionEntry> = new Map();

  constructor(options: ConsumerOptions, messageListener: MessageListener) {
    super(options, messageListener);
  }

  /**
   * Subscribe to a topic with a sub expression.
   *
   * @param {string} topic The topic to subscribe to.
   * @param {string} subExpression The subscription expression.
   * @memberof LitePushConsumer
   */
  async subscribeLite(topic: string, subExpression: string) {
    const entry = new SubscriptionEntry(topic, subExpression, FilterType.TAG);
    this.subscriptions.set(topic, entry);
    await this.syncLiteSubscription();
  }

  /**
   * Unsubscribe from a topic.
   *
   * @param {string} topic The topic to unsubscribe from.
   * @memberof LitePushConsumer
   */
  async unsubscribeLite(topic: string) {
    this.subscriptions.delete(topic);
    await this.syncLiteSubscription();
  }

  public async syncLiteSubscription() {
    this.logger.info('Syncing lite subscriptions...');
    await super.syncLiteSubscription(createResource(this.consumerGroup), this.subscriptions);
  }
}
