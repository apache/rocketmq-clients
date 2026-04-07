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
import { TopicRouteData } from '../route';
import { MessageView } from '../message';
import { Endpoints } from '../route';
import { SimpleConsumer, SimpleConsumerOptions } from './SimpleConsumer';
import { LiteSimpleConsumer } from './LiteSimpleConsumer';
import { OffsetOption } from './OffsetOption';
import { LiteSubscriptionManager } from './LiteSubscriptionManager';
import { FilterExpression } from './FilterExpression';

export interface LiteSimpleConsumerOptions extends SimpleConsumerOptions {
  bindTopic: string;
}

/**
 * Implementation of LiteSimpleConsumer.
 *
 * <p>LiteSimpleConsumer extends SimpleConsumer to provide lightweight message consumption
 * with reduced metadata and storage overhead. It supports dynamic subscription
 * management for lite topics.</p>
 *
 * <p>Key differences from standard SimpleConsumer:</p>
 * <ul>
 *   <li>Uses lite topic subscription mechanism</li>
 *   <li>Optimized routing - only uses first readable master queue</li>
 *   <li>Reduced resource consumption</li>
 * </ul>
 */
export class LiteSimpleConsumerImpl extends SimpleConsumer implements LiteSimpleConsumer {
  private readonly liteSubscriptionManager: LiteSubscriptionManager;
  private readonly bindTopic: Resource;

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(options: LiteSimpleConsumerOptions) {
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
      this as any, // Type assertion needed due to protected methods
      this.bindTopic,
      groupResource,
    );
  }

  /**
   * Get the client type.
   */
  protected getClientType(): ClientType {
    return ClientType.LITE_SIMPLE_CONSUMER;
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
  onSettingsCommand(endpoints: Endpoints, settings: any) {
    super.onSettingsCommand(endpoints, settings);
    this.liteSubscriptionManager.sync(settings);
  }

  /**
   * Override topic route data update to optimize routing for lite consumer.
   * For lite consumers, we only need routes to brokers, so keep only the first readable queue.
   * This reduces memory usage and network overhead significantly.
   */
  protected onTopicRouteDataUpdate(topic: string, topicRouteData: TopicRouteData) {
    // Optimize: only use the first readable master queue for lite consumers
    const originalQueueCount = topicRouteData.getQueueCount();
    const optimizedRouteData = this.optimizeRouteDataForLite(topicRouteData);
    const optimizedQueueCount = optimizedRouteData.getQueueCount();

    if (originalQueueCount > 1 && optimizedQueueCount === 1) {
      this.logger.info(
        'Lite consumer route optimization applied: topic=%s, original queues=%d, optimized queues=%d',
        topic,
        originalQueueCount,
        optimizedQueueCount,
      );
    }

    super.onTopicRouteDataUpdate(topic, optimizedRouteData);
  }

  /**
   * Optimize route data for lite consumer by keeping only the first readable master queue.
   * This reduces memory usage and network overhead.
   *
   * Note: Similar to Java implementation, we filter for readable master queues and take the first one.
   */
  private optimizeRouteDataForLite(topicRouteData: TopicRouteData): TopicRouteData {
    // Find the first readable master queue (similar to Java's isReadableMasterQueue)
    const readableMasterQueue = topicRouteData.messageQueues.find(mq => {
      // Check if it's a master broker (id > 0 typically indicates master)
      // and has read permission
      return mq.broker.id >= 0 && mq.permission === 1; // Permission.READ = 1
    });

    if (readableMasterQueue) {
      const firstQueuePb = readableMasterQueue.toProtobuf();
      return new TopicRouteData([ firstQueuePb ]);
    }

    // If no readable master queue found, fall back to first available queue
    if (topicRouteData.messageQueues.length > 0) {
      const firstQueuePb = topicRouteData.messageQueues[ 0 ].toProtobuf();
      return new TopicRouteData([ firstQueuePb ]);
    }

    // If no queues available, return original (will handle error downstream)
    return topicRouteData;
  }

  /**
   * Receive messages synchronously.
   *
   * @param maxMessageNum max message num of server returned
   * @param invisibleDuration set the invisible duration of messages to return from the server
   * @return list of message view
   */
  async receive(maxMessageNum: number, invisibleDuration: number): Promise<MessageView[]> {
    return super.receive(maxMessageNum, invisibleDuration);
  }

  /**
   * Acknowledge a message synchronously.
   *
   * @param messageView message view to ack
   */
  async ack(messageView: MessageView): Promise<void> {
    return super.ack(messageView);
  }

  /**
   * Change invisible duration synchronously.
   *
   * @param messageView message view to change invisible duration
   * @param invisibleDuration new invisible duration
   */
  async changeInvisibleDuration(messageView: MessageView, invisibleDuration: number): Promise<void> {
    return super.changeInvisibleDuration(messageView, invisibleDuration);
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
