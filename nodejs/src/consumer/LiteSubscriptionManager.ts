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

import { Code, LiteSubscriptionAction } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  NotifyUnsubscribeLiteCommand,
  SyncLiteSubscriptionRequest,
  SyncLiteSubscriptionResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ClientException } from '../exception';
import { Resource } from '../route';
import { LitePushConsumerImpl } from './LitePushConsumerImpl';
import { OffsetOption } from './OffsetOption';

const SYNC_LITE_SUBSCRIPTION_INTERVAL = 30000; // 30 seconds

/**
 * Manages lite topic subscriptions for LitePushConsumer.
 *
 * <p>LiteSubscriptionManager handles:
 * - Maintaining the set of subscribed lite topics
 * - Periodic synchronization of lite subscriptions with server
 * - Quota management and verification
 * - Handling unsubscribe commands from server</p>
 */
export class LiteSubscriptionManager {
  private readonly consumerImpl: LitePushConsumerImpl;
  private readonly bindTopic: Resource;
  private readonly group: Resource;
  private readonly liteTopicSet = new Set<string>();
  private liteSubscriptionQuota: number;
  private maxLiteTopicSize: number;
  private syncTimer?: NodeJS.Timeout;

  constructor(
    consumerImpl: LitePushConsumerImpl,
    bindTopic: Resource,
    group: Resource,
  ) {
    this.consumerImpl = consumerImpl;
    this.bindTopic = bindTopic;
    this.group = group;
    this.liteSubscriptionQuota = 100; // Default quota
    this.maxLiteTopicSize = 64; // Default max length
  }

  /**
   * Start periodic sync of lite subscriptions.
   */
  public startUp() {
    this.syncTimer = setInterval(
      () => this.syncAllLiteSubscription(),
      SYNC_LITE_SUBSCRIPTION_INTERVAL,
    );
  }

  /**
   * Stop the sync timer.
   */
  public shutdown() {
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = undefined;
    }
  }

  /**
   * Get the bind topic name.
   */
  public getBindTopicName(): string {
    return this.bindTopic.getName();
  }

  /**
   * Get the consumer group name.
   */
  public getConsumerGroupName(): string {
    return this.group.getName();
  }

  /**
   * Get a copy of the lite topic set.
   */
  public getLiteTopicSet(): Set<string> {
    return new Set(this.liteTopicSet);
  }

  /**
   * Sync settings from server.
   */
  public sync(settings: any) {
    if (!settings?.hasSubscription?.()) {
      return;
    }

    const subscription = settings.getSubscription();
    if (subscription.hasLiteSubscriptionQuota?.()) {
      this.liteSubscriptionQuota = subscription.getLiteSubscriptionQuota();
    }
    if (subscription.hasMaxLiteTopicSize?.()) {
      this.maxLiteTopicSize = subscription.getMaxLiteTopicSize();
    }
  }

  /**
   * Subscribe to a lite topic.
   */
  public async subscribeLite(
    liteTopic: string,
    offsetOption?: OffsetOption | null,
  ): Promise<void> {
    // Check if consumer is running
    if (!this.consumerImpl.isRunning()) {
      throw new ClientException(500, 'Consumer is not running');
    }

    // Already subscribed
    if (this.liteTopicSet.has(liteTopic)) {
      return;
    }

    // Validate lite topic
    this.validateLiteTopic(liteTopic);

    // Check quota
    this.checkLiteSubscriptionQuota(1);

    try {
      await this.syncLiteSubscription(
        LiteSubscriptionAction.PARTIAL_ADD,
        [ liteTopic ],
        offsetOption ?? null,
      );

      this.liteTopicSet.add(liteTopic);
      this.consumerImpl.getLogger().info(
        'SubscribeLite %s, topic=%s, group=%s, clientId=%s',
        liteTopic,
        this.getBindTopicName(),
        this.getConsumerGroupName(),
        this.consumerImpl.clientId,
      );
    } catch (error) {
      this.consumerImpl.getLogger().error(
        'Failed to subscribeLite %s, error=%s',
        liteTopic,
        error,
      );
      throw error;
    }
  }

  /**
   * Unsubscribe from a lite topic.
   */
  public async unsubscribeLite(liteTopic: string): Promise<void> {
    // Check if consumer is running
    if (!this.consumerImpl.isRunning()) {
      throw new ClientException(500, 'Consumer is not running');
    }

    // Not subscribed
    if (!this.liteTopicSet.has(liteTopic)) {
      return;
    }

    try {
      await this.syncLiteSubscription(
        LiteSubscriptionAction.PARTIAL_REMOVE,
        [ liteTopic ],
        null,
      );

      this.liteTopicSet.delete(liteTopic);
      this.consumerImpl.getLogger().info(
        'UnsubscribeLite %s, topic=%s, group=%s, clientId=%s',
        liteTopic,
        this.getBindTopicName(),
        this.getConsumerGroupName(),
        this.consumerImpl.clientId,
      );
    } catch (error) {
      this.consumerImpl.getLogger().error(
        'Failed to unsubscribeLite %s, error=%s',
        liteTopic,
        error,
      );
      throw error;
    }
  }

  /**
   * Periodic sync of all lite subscriptions.
   */
  private async syncAllLiteSubscription() {
    try {
      this.checkLiteSubscriptionQuota(0);
      await this.syncLiteSubscription(
        LiteSubscriptionAction.COMPLETE_ADD,
        Array.from(this.liteTopicSet),
        null,
      );
    } catch (error) {
      this.consumerImpl.getLogger().error(
        'Schedule syncAllLiteSubscription error, clientId=%s, error=%s',
        this.consumerImpl.clientId,
        error,
      );
    }
  }

  /**
   * Sync lite subscription with server.
   */
  private async syncLiteSubscription(
    action: LiteSubscriptionAction,
    liteTopics: string[],
    offsetOption: OffsetOption | null,
  ): Promise<void> {
    const logger = this.consumerImpl.getLogger();
    if (logger.debug) {
      logger.debug(
        'SyncLiteSubscription: action=%s, liteTopics=[%s], offsetOption=%s',
        LiteSubscriptionAction[action],
        liteTopics.join(', '),
        offsetOption?.toString() ?? 'null',
      );
    }

    // Build the request
    const request = new SyncLiteSubscriptionRequest()
      .setAction(action)
      .setTopic(this.bindTopic.toProtobuf())
      .setGroup(this.group.toProtobuf())
      .setLiteTopicSetList(liteTopics);

    // Set offset option if provided
    if (offsetOption) {
      request.setOffsetOption(offsetOption.toProtobuf());
    }

    try {
      // Call RPC client using public methods from LitePushConsumerImpl
      const response: SyncLiteSubscriptionResponse = await this.consumerImpl.getRpcClientManager().syncLiteSubscription(
        this.consumerImpl.getEndpoints(),
        request,
        this.consumerImpl.getRequestTimeout(),
      );

      // Handle response status
      const status = response.getStatus();
      if (status && status.getCode() !== Code.OK) {
        throw new ClientException(
          status.getCode(),
          `Failed to sync lite subscription: ${status.getMessage()}`,
        );
      }

      if (logger.info) {
        logger.info(
          'SyncLiteSubscription success: action=%s, liteTopics=[%s], clientId=%s',
          LiteSubscriptionAction[action],
          liteTopics.join(', '),
          this.consumerImpl.clientId,
        );
      }
    } catch (error) {
      if (logger.error) {
        logger.error(
          'SyncLiteSubscription failed: action=%s, liteTopics=[%s], clientId=%s, error=%s',
          LiteSubscriptionAction[action],
          liteTopics.join(', '),
          this.consumerImpl.clientId,
          error,
        );
      }
      throw error;
    }
  }

  /**
   * Handle notify unsubscribe lite command from server.
   */
  public onNotifyUnsubscribeLiteCommand(command: NotifyUnsubscribeLiteCommand) {
    const liteTopic = command.getLiteTopic();
    this.consumerImpl.getLogger().info(
      'Notify unsubscribe lite: liteTopic=%s, group=%s, bindTopic=%s',
      liteTopic,
      this.getConsumerGroupName(),
      this.getBindTopicName(),
    );

    if (liteTopic && liteTopic.trim().length > 0) {
      this.liteTopicSet.delete(liteTopic);
    }
  }

  /**
   * Validate lite topic name.
   */
  private validateLiteTopic(liteTopic: string, maxLength?: number) {
    const maxLen = maxLength ?? this.maxLiteTopicSize;

    if (!liteTopic || liteTopic.trim().length === 0) {
      throw new Error('liteTopic is blank');
    }

    if (liteTopic.length > maxLen) {
      throw new Error(
        `liteTopic length exceeded max length ${maxLen}, liteTopic: ${liteTopic}`,
      );
    }
  }

  /**
   * Check lite subscription quota.
   */
  private checkLiteSubscriptionQuota(delta: number) {
    if (this.liteTopicSet.size + delta > this.liteSubscriptionQuota) {
      throw new ClientException(
        429,
        `Lite subscription quota exceeded ${this.liteSubscriptionQuota}`,
      );
    }
  }

  /**
   * Returns a string representation of this LiteSubscriptionManager.
   *
   * @return String representation
   */
  toString(): string {
    return `LiteSubscriptionManager{bindTopic=${this.bindTopic.getName()}, group=${this.group.getName()}, liteTopicSet=[${Array.from(this.liteTopicSet).join(', ')}], liteSubscriptionQuota=${this.liteSubscriptionQuota}, maxLiteTopicSize=${this.maxLiteTopicSize}}`;
  }
}
