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
  delay?: number;
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
    let finalDeliveryTimestamp = options.deliveryTimestamp;
    if (options.delay && !finalDeliveryTimestamp) {
      finalDeliveryTimestamp = new Date(Date.now() + options.delay);
    }

    const hasPriority = options.priority !== undefined;
    const hasLiteTopic = options.liteTopic !== undefined;
    const hasTimedDelivery = finalDeliveryTimestamp !== undefined;
    const hasGroup = options.messageGroup !== undefined;

    if (hasPriority) {
      if (options.priority === undefined || options.priority < 0) {
        throw new Error('priority must be greater than or equal to 0');
      }
      if (hasTimedDelivery || hasGroup || hasLiteTopic) {
        throw new Error('priority is mutually exclusive with delay, deliveryTimestamp, messageGroup, and liteTopic');
      }
    }

    if (hasLiteTopic) {
      if (hasTimedDelivery || hasGroup) {
        throw new Error('liteTopic is mutually exclusive with delay, deliveryTimestamp, and messageGroup');
      }
    }

    if (hasTimedDelivery && hasGroup) {
      throw new Error('delay/deliveryTimestamp is mutually exclusive with messageGroup');
    }

    // 4. Assign properties
    this.topic = options.topic;
    this.body = options.body;
    this.tag = options.tag;
    this.messageGroup = options.messageGroup;
    this.keys = options.keys ?? [];
    this.properties = options.properties;
    this.deliveryTimestamp = finalDeliveryTimestamp;
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
