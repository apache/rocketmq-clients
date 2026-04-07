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

import { BaseClientOptions } from '../client';
import { MessageListener } from './MessageListener';
import { OffsetOption } from './OffsetOption';
import { LitePushConsumerImpl } from './LitePushConsumerImpl';

/**
 * LitePushConsumer interface for consuming messages from lite topics.
 *
 * <p>LitePushConsumer is a specialized consumer designed for lightweight scenarios
 * with reduced metadata and storage overhead. It supports dynamic subscription
 * management for lite topics.</p>
 */
export interface LitePushConsumer {
  /**
   * Subscribe to a lite topic.
   *
   * <p>The subscribeLite() method initiates network requests and performs quota verification,
   * so it may fail. It's important to check the result of this call to ensure that the
   * subscription was successfully added. Possible failure scenarios include:</p>
   * <ul>
   *   <li>Network request errors, which can be retried.</li>
   *   <li>Quota verification failures, indicated by LiteSubscriptionQuotaExceededException.
   *       In this case, evaluate whether the quota is insufficient and promptly unsubscribe
   *       from unused subscriptions using unsubscribeLite() to free up resources.</li>
   * </ul>
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with consumeFromOption to specify the consume from offset.
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @param offsetOption - The consume from offset option
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;

  /**
   * Unsubscribe from a lite topic.
   *
   * @param liteTopic - The name of the lite topic to unsubscribe from
   * @throws ClientException if an error occurs during unsubscription
   */
  unsubscribeLite(liteTopic: string): Promise<void>;

  /**
   * Get the lite topic immutable set.
   *
   * @return Lite topic immutable set
   */
  getLiteTopicSet(): Set<string>;

  /**
   * Get the load balancing group for the consumer.
   *
   * @return Consumer load balancing group
   */
  getConsumerGroup(): string;

  /**
   * Close the consumer and release all related resources.
   *
   * <p>Once the consumer is closed, <strong>it could not be started once again.</strong>
   * We maintain an FSM (finite-state machine) to record the different states for each
   * push consumer.</p>
   */
  close(): Promise<void>;
}

export interface LitePushConsumerOptions extends BaseClientOptions {
  consumerGroup: string;
  bindTopic: string;
  messageListener: MessageListener;
}

const CONSUMER_GROUP_PATTERN = /^[a-zA-Z0-9_-]+$/;

/**
 * LitePushConsumer builder class.
 *
 * <p>This class provides a fluent API for configuring and creating
 * lite push consumers with reduced overhead for lightweight scenarios.</p>
 */
export class LitePushConsumerBuilder {
  private options: Partial<LitePushConsumerOptions> = {};

  /**
   * Set the bind topic for the lite push consumer.
   *
   * @param bindTopic - The parent topic to bind
   * @return This builder instance
   * @throws Error if bindTopic is blank
   */
  bindTopic(bindTopic: string): LitePushConsumerBuilder {
    if (!bindTopic || bindTopic.trim().length === 0) {
      throw new Error('bindTopic should not be blank');
    }
    this.options.bindTopic = bindTopic;
    return this;
  }

  /**
   * Set the client configuration.
   *
   * @param options - Client configuration options
   * @return This builder instance
   * @throws Error if options is null/undefined
   */
  setClientConfiguration(options: BaseClientOptions): LitePushConsumerBuilder {
    if (!options) {
      throw new Error('clientConfiguration should not be null');
    }
    Object.assign(this.options, options);
    return this;
  }

  /**
   * Set the consumer group.
   *
   * @param consumerGroup - Consumer group name
   * @return This builder instance
   * @throws Error if consumerGroup is null, doesn't match the pattern, or starts with 'GID-'
   */
  setConsumerGroup(consumerGroup: string): LitePushConsumerBuilder {
    if (!consumerGroup) {
      throw new Error('consumerGroup should not be null');
    }
    if (!CONSUMER_GROUP_PATTERN.test(consumerGroup)) {
      throw new Error(`consumerGroup does not match the pattern ${CONSUMER_GROUP_PATTERN.source}`);
    }
    this.options.consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Set the message listener.
   *
   * @param messageListener - Message listener implementation
   * @return This builder instance
   * @throws Error if messageListener is null/undefined
   */
  setMessageListener(messageListener: MessageListener): LitePushConsumerBuilder {
    if (!messageListener) {
      throw new Error('messageListener should not be null');
    }
    this.options.messageListener = messageListener;
    return this;
  }

  /**
   * Set the maximum cache message count.
   *
   * @param maxCacheMessageCount - Maximum number of messages to cache
   * @return This builder instance
   * @throws Error if maxCacheMessageCount is not positive
   */
  setMaxCacheMessageCount(maxCacheMessageCount: number): LitePushConsumerBuilder {
    if (maxCacheMessageCount <= 0) {
      throw new Error('maxCacheMessageCount should be positive');
    }
    // TODO: Will be used when implementing full consumer
    return this;
  }

  /**
   * Set the maximum cache message size in bytes.
   *
   * @param maxCacheMessageSizeInBytes - Maximum cache size in bytes
   * @return This builder instance
   * @throws Error if maxCacheMessageSizeInBytes is not positive
   */
  setMaxCacheMessageSizeInBytes(maxCacheMessageSizeInBytes: number): LitePushConsumerBuilder {
    if (maxCacheMessageSizeInBytes <= 0) {
      throw new Error('maxCacheMessageSizeInBytes should be positive');
    }
    // TODO: Will be used when implementing full consumer
    return this;
  }

  /**
   * Set the consumption thread count.
   *
   * @param consumptionThreadCount - Number of threads for consumption
   * @return This builder instance
   * @throws Error if consumptionThreadCount is not positive
   */
  setConsumptionThreadCount(consumptionThreadCount: number): LitePushConsumerBuilder {
    if (consumptionThreadCount <= 0) {
      throw new Error('consumptionThreadCount should be positive');
    }
    // TODO: Will be used when implementing full consumer
    return this;
  }

  /**
   * Start up the LitePushConsumer instance.
   *
   * @return Promise resolving to started LitePushConsumer instance
   * @throws Error if required parameters are not set
   */
  async startup(): Promise<LitePushConsumer> {
    if (!this.options.endpoints) {
      throw new Error('clientConfiguration has not been set yet');
    }
    if (!this.options.consumerGroup) {
      throw new Error('consumerGroup has not been set yet');
    }
    if (!this.options.messageListener) {
      throw new Error('messageListener has not been set yet');
    }
    if (!this.options.bindTopic) {
      throw new Error('bindTopic has not been set yet');
    }

    // Build LitePushConsumerImpl
    const options: any = {
      endpoints: this.options.endpoints,
      namespace: this.options.namespace ?? '',
      consumerGroup: this.options.consumerGroup,
      bindTopic: this.options.bindTopic,
      messageListener: this.options.messageListener,
      sslEnabled: this.options.sslEnabled,
      sessionCredentials: this.options.sessionCredentials,
      requestTimeout: this.options.requestTimeout,
      logger: this.options.logger,
    };

    const litePushConsumer = new LitePushConsumerImpl(options);
    await litePushConsumer.startup();
    return litePushConsumer;
  }
}
