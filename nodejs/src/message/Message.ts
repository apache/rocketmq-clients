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
}

export class Message {
  topic: string;
  body: Buffer;
  tag?: string;
  messageGroup?: string;
  keys: string[];
  properties?: Map<string, string>;
  deliveryTimestamp?: Date;

  constructor(options: MessageOptions) {
    this.topic = options.topic;
    this.body = options.body;
    this.tag = options.tag;
    this.messageGroup = options.messageGroup;
    this.keys = options.keys ?? [];
    this.properties = options.properties;
    let deliveryTimestamp = options.deliveryTimestamp;
    if (options.delay && !deliveryTimestamp) {
      deliveryTimestamp = new Date(Date.now() + options.delay);
    }
    this.deliveryTimestamp = deliveryTimestamp;
  }
}
