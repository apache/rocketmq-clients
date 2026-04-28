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
import { Endpoints, TopicRouteData } from '../route';
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
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Dynamic lite topic subscription/unsubscription</li>
 *   <li>Quota management for lite subscriptions</li>
 *   <li>Reduced resource consumption compared to standard PushConsumer</li>
 *   <li>Automatic synchronization with server settings</li>
 * </ul>
 *
 * <p>Note: Unlike LiteSimpleConsumer, LitePushConsumer does not perform route optimization
 * because it uses the assignment-based message delivery mechanism managed by the server.</p>
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
   *
   * @return The client type identifier for lite push consumer
   */
  protected getClientType(): ClientType {
    return ClientType.LITE_PUSH_CONSUMER;
  }

  /**
   * Override to disable standard assignment scanning for lite consumers.
   * LitePushConsumer uses Lite Subscription mechanism instead of assignments.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected onTopicRouteDataUpdate(_topic: string, _topicRouteData: TopicRouteData) {
    // Lite consumers do not use standard assignment mechanism
    // They rely on LiteSubscriptionManager for message delivery
  }

  /**
   * Start up the consumer.
   *
   * <p>This method initializes the consumer and starts the lite subscription manager.
   * It must be called before the consumer can receive messages.</p>
   */
  async startup() {
    await super.startup();
    this.liteSubscriptionManager.startUp();
  }

  /**
   * Shutdown the consumer.
   *
   * <p>This method gracefully shuts down the consumer, releasing all resources
   * and stopping the lite subscription manager.</p>
   */
  async shutdown() {
    this.liteSubscriptionManager.shutdown();
    await super.shutdown();
  }

  /**
   * Subscribe to a lite topic.
   *
   * <p>The subscribeLite() method initiates network requests and performs quota verification,
   * so it may fail. It's important to handle potential errors. Possible failure scenarios include:</p>
   * <ul>
   *   <li>Network request errors, which can be retried.</li>
   *   <li>Quota verification failures (LiteSubscriptionQuotaExceededException).
   *       In this case, evaluate whether the quota is insufficient and promptly unsubscribe
   *       from unused subscriptions using unsubscribeLite() to free up resources.</li>
   * </ul>
   *
   * @param liteTopic - The name of the lite topic to subscribe
   */
  async subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with offset option to specify the consume from offset.
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @param offsetOption - The consume from offset option
   */
  async subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;

  async subscribeLite(liteTopic: string, offsetOption?: OffsetOption): Promise<void> {
    await this.liteSubscriptionManager.subscribeLite(liteTopic, offsetOption ?? null);
  }

  /**
   * Unsubscribe from a lite topic.
   *
   * <p>This method removes the subscription and notifies the server.
   * After unsubscribing, the consumer will no longer receive messages from this topic.</p>
   *
   * @param liteTopic - The name of the lite topic to unsubscribe from
   */
  async unsubscribeLite(liteTopic: string): Promise<void> {
    await this.liteSubscriptionManager.unsubscribeLite(liteTopic);
  }

  /**
   * Get the lite topic set.
   *
   * <p>Returns an immutable set of currently subscribed lite topics.
   * This set reflects the current state of subscriptions managed by the lite subscription manager.</p>
   *
   * @return Set of lite topic names
   */
  getLiteTopicSet(): Set<string> {
    return this.liteSubscriptionManager.getLiteTopicSet();
  }

  /**
   * Get the load balancing group for the consumer.
   *
   * <p>The consumer group is used for load balancing across multiple consumer instances.
   * All consumers in the same group will share the message load.</p>
   *
   * @return Consumer group name
   */
  getConsumerGroup(): string {
    return this.liteSubscriptionManager.getConsumerGroupName();
  }

  /**
   * Handle notify unsubscribe lite command from server.
   *
   * <p>This method is called when the server sends a notification to unsubscribe
   * from a lite topic, typically due to quota violations or administrative actions.</p>
   *
   * @param command - The unsubscribe command from the server
   */
  onNotifyUnsubscribeLiteCommand(command: NotifyUnsubscribeLiteCommand) {
    this.liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);
  }

  /**
   * Handle settings command from server.
   *
   * <p>This method processes configuration updates from the server and synchronizes
   * the lite subscription manager with the latest settings.</p>
   *
   * @param endpoints - The server endpoints
   * @param settings - The settings configuration
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
