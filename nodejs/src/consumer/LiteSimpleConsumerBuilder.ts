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
import { LiteSimpleConsumerImpl, LiteSimpleConsumerOptions } from './LiteSimpleConsumerImpl';

/**
 * Builder for creating LiteSimpleConsumer instances.
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * const consumer = await new LiteSimpleConsumerBuilder()
 *   .setClientConfiguration(clientConfig)
 *   .setConsumerGroup('your-consumer-group')
 *   .bindTopic('your-parent-topic')
 *   .setAwaitDuration(30000)
 *   .build();
 * }</pre>
 */
export class LiteSimpleConsumerBuilder {
  private options: Partial<LiteSimpleConsumerOptions> = {};

  /**
   * Set the bind topic for the lite simple consumer.
   *
   * @param bindTopic the parent topic that lite topics belong to
   * @return this builder
   */
  bindTopic(bindTopic: string): LiteSimpleConsumerBuilder {
    if (!bindTopic || bindTopic.trim().length === 0) {
      throw new Error('bindTopic should not be blank');
    }
    this.options.bindTopic = bindTopic;
    return this;
  }

  /**
   * Set the client configuration.
   *
   * @param options the client configuration options
   * @return this builder
   */
  setClientConfiguration(options: BaseClientOptions): LiteSimpleConsumerBuilder {
    if (!options) {
      throw new Error('clientConfiguration should not be null');
    }
    Object.assign(this.options, options);
    return this;
  }

  /**
   * Set the consumer group.
   *
   * <p>Note: For lite consumers, the consumer group should NOT start with 'GID-' prefix.</p>
   *
   * @param consumerGroup the consumer group name
   * @return this builder
   */
  setConsumerGroup(consumerGroup: string): LiteSimpleConsumerBuilder {
    if (!consumerGroup) {
      throw new Error('consumerGroup should not be null');
    }
    // Validate consumer group format (should not start with GID-)
    if (consumerGroup.startsWith('GID-')) {
      throw new Error(
        'Lite consumer group should not start with "GID-" prefix. ' +
        'Please use a different group name without the prefix.');
    }
    this.options.consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Set the await duration for long-polling.
   *
   * @param awaitDuration the await duration in milliseconds
   * @return this builder
   */
  setAwaitDuration(awaitDuration: number): LiteSimpleConsumerBuilder {
    if (awaitDuration <= 0) {
      throw new Error('awaitDuration should be greater than 0');
    }
    this.options.awaitDuration = awaitDuration;
    return this;
  }

  /**
   * Build and start the LiteSimpleConsumer.
   *
   * @return the started LiteSimpleConsumer instance
   * @throws Error if required parameters are not set
   */
  async build(): Promise<LiteSimpleConsumerImpl> {
    // Validate required parameters
    if (!this.options.endpoints) {
      throw new Error('clientConfiguration has not been set yet');
    }
    if (!this.options.consumerGroup) {
      throw new Error('consumerGroup has not been set yet');
    }
    if (!this.options.awaitDuration) {
      throw new Error('awaitDuration has not been set yet');
    }
    if (!this.options.bindTopic) {
      throw new Error('bindTopic has not been set yet');
    }

    // Create options
    const options: any = {
      endpoints: this.options.endpoints,
      namespace: this.options.namespace ?? '',
      consumerGroup: this.options.consumerGroup,
      bindTopic: this.options.bindTopic,
      awaitDuration: this.options.awaitDuration,
      subscriptions: new Map(), // Will be set in constructor
      sslEnabled: this.options.sslEnabled,
      sessionCredentials: this.options.sessionCredentials,
      requestTimeout: this.options.requestTimeout,
      logger: this.options.logger,
    };

    // Create and start the consumer
    const consumer = new LiteSimpleConsumerImpl(options);
    await consumer.startup();

    return consumer;
  }
}
