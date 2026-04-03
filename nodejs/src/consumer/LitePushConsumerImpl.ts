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
import { NotifyUnsubscribeLiteCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Resource } from '../route/Resource';
import { Endpoints } from '../route';
import { PushConsumer, PushConsumerOptions } from './PushConsumer';
import { LitePushConsumer } from './LitePushConsumer';
import { OffsetOption } from './OffsetOption';
import { LiteSubscriptionManager } from './LiteSubscriptionManager';
import { FilterExpression } from './FilterExpression';

export interface LitePushConsumerOptions extends PushConsumerOptions {
  bindTopic: string;
}

/**
 * Implementation of LitePushConsumer.
 *
 * <p>LitePushConsumer extends PushConsumer to provide lightweight message consumption
 * with reduced metadata and storage overhead. It supports dynamic subscription
 * management for lite topics.</p>
 */
export class LitePushConsumerImpl extends PushConsumer implements LitePushConsumer {
  private readonly liteSubscriptionManager: LiteSubscriptionManager;
  private readonly bindTopic: Resource;

  constructor(options: LitePushConsumerOptions) {
    // Create subscription expressions with bind topic
    const subscriptions = new Map<string, FilterExpression>();
    subscriptions.set(options.bindTopic, FilterExpression.SUB_ALL);

    super({
      ...options,
      subscriptions,
    });

    this.bindTopic = new Resource(options.namespace, options.bindTopic);

    const groupResource = new Resource(options.namespace, options.consumerGroup);
    this.liteSubscriptionManager = new LiteSubscriptionManager(
      this,
      this.bindTopic,
      groupResource,
    );
  }

  /**
   * Get the client type.
   */
  protected getClientType(): ClientType {
    return ClientType.LITE_PUSH_CONSUMER;
  }

  /**
   * Start up the consumer.
   */
  async startup() {
    await super.startup();
    this.liteSubscriptionManager.startUp();
  }

  /**
   * Shutdown the consumer.
   */
  async shutdown() {
    this.liteSubscriptionManager.shutdown();
    await super.shutdown();
  }

  /**
   * Subscribe to a lite topic.
   */
  async subscribeLite(liteTopic: string): Promise<void>;
  async subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;
  async subscribeLite(liteTopic: string, offsetOption?: OffsetOption): Promise<void> {
    await this.liteSubscriptionManager.subscribeLite(liteTopic, offsetOption ?? null);
  }

  /**
   * Unsubscribe from a lite topic.
   */
  async unsubscribeLite(liteTopic: string): Promise<void> {
    await this.liteSubscriptionManager.unsubscribeLite(liteTopic);
  }

  /**
   * Get the lite topic set.
   */
  getLiteTopicSet(): Set<string> {
    return this.liteSubscriptionManager.getLiteTopicSet();
  }

  /**
   * Get the load balancing group for the consumer.
   */
  getConsumerGroup(): string {
    return this.liteSubscriptionManager.getConsumerGroupName();
  }

  /**
   * Handle notify unsubscribe lite command from server.
   */
  onNotifyUnsubscribeLiteCommand(command: NotifyUnsubscribeLiteCommand) {
    this.liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);
  }

  /**
   * Handle settings command from server.
   */
  onSettingsCommand(endpoints: any, settings: any) {
    super.onSettingsCommand(endpoints, settings);
    this.liteSubscriptionManager.sync(settings);
  }

  /**
   * Close the consumer.
   */
  async close(): Promise<void> {
    await this.shutdown();
  }

  /**
   * Get the logger.
   */
  getLogger() {
    return this.logger;
  }

  /**
   * Get the RPC client manager (protected access for internal use).
   *
   * @internal
   */
  getRpcClientManager() {
    return this.rpcClientManager;
  }

  /**
   * Get the endpoints (protected access for internal use).
   *
   * @internal
   */
  getEndpoints(): Endpoints {
    return this.endpoints;
  }

  /**
   * Get the request timeout (protected access for internal use).
   *
   * @internal
   */
  getRequestTimeout(): number {
    return this.requestTimeout;
  }
}
