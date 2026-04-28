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

export interface MessageOptions {
  topic: string;
  body: Buffer;
  tag?: string;
  messageGroup?: string;
  keys?: string[];
  properties?: Map<string, string>;
  deliveryTimestamp?: Date;
  priority?: number;
  liteTopic?: string;
}

export class Message {
  topic: string;
  body: Buffer;
  tag?: string;
  messageGroup?: string;
  keys: string[];
  properties?: Map<string, string>;
  deliveryTimestamp?: Date;
  priority?: number;
  liteTopic?: string;

  constructor(options: MessageOptions) {
    // Validate mutual exclusivity - priority
    if (options.priority !== undefined) {
      if (options.priority < 0) {
        throw new Error('priority must be greater than or equal to 0');
      }
      if (options.deliveryTimestamp) {
        throw new Error('priority and deliveryTimestamp should not be set at same time');
      }
      if (options.messageGroup) {
        throw new Error('priority and messageGroup should not be set at same time');
      }
      if (options.liteTopic) {
        throw new Error('priority and liteTopic should not be set at same time');
      }
    }

    // Validate mutual exclusivity - liteTopic
    if (options.liteTopic !== undefined) {
      if (options.deliveryTimestamp) {
        throw new Error('liteTopic and deliveryTimestamp should not be set at same time');
      }
      if (options.messageGroup) {
        throw new Error('liteTopic and messageGroup should not be set at same time');
      }
    }

    // Validate mutual exclusivity - deliveryTimestamp
    if (options.deliveryTimestamp !== undefined) {
      if (options.messageGroup) {
        throw new Error('deliveryTimestamp and messageGroup should not be set at same time');
      }
    }

    this.topic = options.topic;
    this.body = options.body;
    this.tag = options.tag;
    this.messageGroup = options.messageGroup;
    this.keys = options.keys ?? [];
    this.properties = options.properties;
    this.deliveryTimestamp = options.deliveryTimestamp;
    this.priority = options.priority;
    this.liteTopic = options.liteTopic;
  }

  /**
   * Returns a string representation of the message.
   */
  toString(): string {
    return `Message{topic='${this.topic}', tag=${this.tag}, messageGroup=${this.messageGroup}, ` +
      `liteTopic=${this.liteTopic}, priority=${this.priority}, deliveryTimestamp=${this.deliveryTimestamp}, ` +
      `keys=[${this.keys.join(', ')}], properties=${JSON.stringify(this.properties)}}`;
  }
}
