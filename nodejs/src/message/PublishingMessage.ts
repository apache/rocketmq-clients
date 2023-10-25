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

import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import {
  MessageType, Message as MessagePB, SystemProperties, Encoding,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { PublishingSettings } from '../producer';
import { createResource } from '../util';
import { MessageQueue } from '../route';
import { UserAgent } from '../client';
import { Message, MessageOptions } from './Message';
import { MessageIdFactory } from './MessageId';

export class PublishingMessage extends Message {
  readonly messageId: string;
  readonly messageType: MessageType;

  constructor(options: MessageOptions, publishingSettings: PublishingSettings, txEnabled: boolean) {
    super(options);
    const length = this.body.length;
    const maxBodySizeBytes = publishingSettings.maxBodySizeBytes;
    if (length > maxBodySizeBytes) {
      throw new TypeError(`Message body size exceeds the threshold, max size=${maxBodySizeBytes} bytes`);
    }
    // Generate message id.
    this.messageId = MessageIdFactory.create().toString();
    // Normal message.
    if (!this.messageGroup && !this.deliveryTimestamp && !txEnabled) {
      this.messageType = MessageType.NORMAL;
      return;
    }
    // Fifo message.
    if (this.messageGroup && !txEnabled) {
      this.messageType = MessageType.FIFO;
      return;
    }
    // Delay message.
    if (this.deliveryTimestamp && !txEnabled) {
      this.messageType = MessageType.DELAY;
      return;
    }
    // Transaction message.
    if (!this.messageGroup &&
        !this.deliveryTimestamp && txEnabled) {
      this.messageType = MessageType.TRANSACTION;
      return;
    }
    // Transaction semantics is conflicted with fifo/delay.
    throw new TypeError('Transactional message should not set messageGroup or deliveryTimestamp');
  }

  /**
   * This method should be invoked before each message sending, because the born time is reset before each
   * invocation, which means that it should not be invoked ahead of time.
   */
  toProtobuf(mq: MessageQueue) {
    const systemProperties = new SystemProperties()
      .setKeysList(this.keys)
      .setMessageId(this.messageId)
      .setBornTimestamp(Timestamp.fromDate(new Date()))
      .setBornHost(UserAgent.INSTANCE.hostname)
      .setBodyEncoding(Encoding.IDENTITY)
      .setQueueId(mq.queueId)
      .setMessageType(this.messageType);
    if (this.tag) {
      systemProperties.setTag(this.tag);
    }
    if (this.deliveryTimestamp) {
      systemProperties.setDeliveryTimestamp(Timestamp.fromDate(this.deliveryTimestamp));
    }
    if (this.messageGroup) {
      systemProperties.setMessageGroup(this.messageGroup);
    }

    const message = new MessagePB()
      .setTopic(createResource(this.topic))
      .setBody(this.body)
      .setSystemProperties(systemProperties);
    if (this.properties) {
      const userProperties = message.getUserPropertiesMap();
      for (const [ key, value ] of this.properties.entries()) {
        userProperties.set(key, value);
      }
    }
    return message;
  }
}
