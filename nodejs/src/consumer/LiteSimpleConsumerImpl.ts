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

import { ClientType, Permission, Settings as SettingsPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import { NotifyUnsubscribeLiteCommand } from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints, TopicRouteData } from '../route';
import { Resource } from '../route/Resource';
import { MASTER_BROKER_ID } from '../util';
import { ILogger } from '../client/Logger';
import { RpcClientManager } from '../client/RpcClientManager';
import { SimpleConsumer, SimpleConsumerOptions } from './SimpleConsumer';
import { FilterExpression } from './FilterExpression';
import { LiteSubscriptionManager, LiteSubscriptionHost } from './LiteSubscriptionManager';
import { OffsetOption } from './OffsetOption';
import { LiteSimpleConsumer, LiteSimpleConsumerOptions } from './LiteSimpleConsumer';

/**
 * Keep only the first readable master queue of the route. Lite consumers talk
 * to brokers only through the bound parent topic, so a single queue is enough
 * and it avoids useless route bookkeeping for the other queues.
 */
export function firstReadableMasterQueue(topicRouteData: TopicRouteData): TopicRouteData {
  const readable = topicRouteData.messageQueues.find(
    mq => mq.broker.id === MASTER_BROKER_ID &&
      (mq.permission === Permission.READ || mq.permission === Permission.READ_WRITE),
  );
  return new TopicRouteData(readable ? [ readable.toProtobuf() ] : []);
}

/**
 * Implementation of LiteSimpleConsumer.
 *
 * <p>LiteSimpleConsumer extends SimpleConsumer to provide explicit receive/ack
 * control over lite topics. It binds to a single parent topic and manages lite
 * topic subscriptions dynamically via the lite subscription sync protocol.</p>
 */
export class LiteSimpleConsumerImpl extends SimpleConsumer
  implements LiteSimpleConsumer, LiteSubscriptionHost {
  readonly #bindTopic: Resource;
  readonly #liteSubscriptionManager: LiteSubscriptionManager;

  constructor(options: LiteSimpleConsumerOptions) {
    if (!options.bindTopic || options.bindTopic.trim().length === 0) {
      throw new TypeError('bindTopic should not be blank');
    }
    // Default subscription: (bindTopic, *) for code reuse.
    const subscriptions = new Map<string, FilterExpression | string>()
      .set(options.bindTopic, FilterExpression.SUB_ALL);

    super({
      ...options,
      subscriptions,
    } as SimpleConsumerOptions);

    this.#bindTopic = new Resource(options.namespace, options.bindTopic);
    const groupResource = new Resource(options.namespace, options.consumerGroup);
    this.#liteSubscriptionManager = new LiteSubscriptionManager(this, this.#bindTopic, groupResource);
  }

  /**
   * Get the client type.
   *
   * @return The client type identifier for lite simple consumer
   */
  protected getClientType(): ClientType {
    return ClientType.LITE_SIMPLE_CONSUMER;
  }

  protected onTopicRouteDataUpdate(topic: string, topicRouteData: TopicRouteData) {
    super.onTopicRouteDataUpdate(topic, firstReadableMasterQueue(topicRouteData));
  }

  /**
   * Start up the consumer.
   *
   * <p>This method initializes the consumer and starts the lite subscription
   * manager. It must be called before the consumer can receive messages.</p>
   */
  async startup() {
    await super.startup();
    this.#liteSubscriptionManager.startUp();
  }

  /**
   * Shutdown the consumer.
   *
   * <p>This method gracefully shuts down the consumer, releasing all resources
   * and stopping the lite subscription manager.</p>
   */
  async shutdown() {
    this.#liteSubscriptionManager.shutdown();
    await super.shutdown();
  }

  async subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with an offset option to specify the consume
   * from offset.
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @param offsetOption - The consume from offset option
   */
  async subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;

  async subscribeLite(liteTopic: string, offsetOption?: OffsetOption): Promise<void> {
    if (!liteTopic || liteTopic.trim().length === 0) {
      throw new Error('liteTopic should not be blank');
    }
    await this.#liteSubscriptionManager.subscribeLite(liteTopic, offsetOption ?? null);
  }

  /**
   * Unsubscribe from a lite topic.
   *
   * @param liteTopic - The name of the lite topic to unsubscribe from
   */
  async unsubscribeLite(liteTopic: string): Promise<void> {
    if (!liteTopic || liteTopic.trim().length === 0) {
      throw new Error('liteTopic should not be blank');
    }
    await this.#liteSubscriptionManager.unsubscribeLite(liteTopic);
  }

  /**
   * Get the lite topic set.
   *
   * @return Set of currently subscribed lite topic names
   */
  getLiteTopicSet(): Set<string> {
    return this.#liteSubscriptionManager.getLiteTopicSet();
  }

  /**
   * Get the load balancing group for the consumer.
   *
   * @return Consumer group name
   */
  getConsumerGroup(): string {
    return this.#liteSubscriptionManager.getConsumerGroupName();
  }

  /**
   * Handle notify unsubscribe lite command from server.
   *
   * @param _endpoints - The server endpoints
   * @param command - The unsubscribe command from the server
   */
  onNotifyUnsubscribeLiteCommand(_endpoints: Endpoints, command: NotifyUnsubscribeLiteCommand) {
    this.#liteSubscriptionManager.onNotifyUnsubscribeLiteCommand(command);
  }

  /**
   * Handle settings command from server.
   *
   * @param endpoints - The server endpoints
   * @param settings - The settings configuration
   */
  onSettingsCommand(endpoints: Endpoints, settings: SettingsPB) {
    super.onSettingsCommand(endpoints, settings);
    this.#liteSubscriptionManager.sync(settings);
  }

  /**
   * Close the consumer.
   */
  async close(): Promise<void> {
    await this.shutdown();
  }

  /**
   * Get the logger.
   *
   * @internal
   */
  getLogger(): ILogger {
    return this.logger;
  }

  /**
   * Get the RPC client manager (protected access for internal use).
   *
   * @internal
   */
  getRpcClientManager(): RpcClientManager {
    return this.rpcClientManager;
  }

  /**
   * Get the request timeout (protected access for internal use).
   *
   * @internal
   */
  getRequestTimeout(): number {
    return this.requestTimeout;
  }

  /**
   * Endpoints the lite subscription manager should sync to: every endpoint
   * present in the consumed routes.
   *
   * @internal
   */
  getSyncEndpoints(): Endpoints[] {
    return this.getTotalRouteEndpoints();
  }
}
