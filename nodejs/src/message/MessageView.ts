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

import { gunzipSync } from 'node:zlib';
import {
  DigestType,
  Encoding,
  Message as MessagePB,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { crc32CheckSum, md5CheckSum, sha1CheckSum } from '../util';
import { Endpoints, MessageQueue } from '../route';

export class MessageView {
  readonly messageId: string;
  readonly topic: string;
  readonly body: Buffer;
  readonly corrupted: boolean;
  readonly transportDeliveryTimestamp?: Date;
  readonly tag?: string;
  readonly messageGroup?: string;
  readonly deliveryTimestamp?: Date;
  readonly keys: string[];
  readonly bornHost: string;
  readonly bornTimestamp?: Date;
  readonly deliveryAttempt?: number;
  readonly endpoints: Endpoints;
  readonly receiptHandle: string;
  readonly offset?: number;
  readonly decodeTimestamp: Date;
  readonly properties = new Map<string, string>();
  readonly liteTopic?: string;
  readonly priority?: number;

  constructor(message: MessagePB, messageQueue?: MessageQueue, transportDeliveryTimestamp?: Date) {
    const systemProperties = message.getSystemProperties()!;
    const bodyDigest = systemProperties.getBodyDigest()!.toObject();
    const digestType = bodyDigest.type;
    const checksum = bodyDigest.checksum;
    let expectedChecksum = '';
    let bodyBytes = Buffer.from(message.getBody_asU8());
    switch (digestType) {
      case DigestType.CRC32:
        expectedChecksum = crc32CheckSum(bodyBytes);
        break;
      case DigestType.MD5:
        expectedChecksum = md5CheckSum(bodyBytes);
        break;
      case DigestType.SHA1:
        expectedChecksum = sha1CheckSum(bodyBytes);
        break;
      default:
        // log.error("Unsupported message body digest algorithm, digestType={}, topic={}, messageId={}",
        //             digestType, topic, messageId);
    }
    if (expectedChecksum && expectedChecksum !== checksum) {
      this.corrupted = true;
    }
    if (systemProperties.getBodyEncoding() === Encoding.GZIP) {
      bodyBytes = gunzipSync(bodyBytes);
    }

    for (const [ key, value ] of message.getUserPropertiesMap().entries()) {
      this.properties.set(key, value);
    }
    this.messageId = systemProperties.getMessageId();
    this.topic = message.getTopic()!.getName();
    this.tag = systemProperties.getTag();
    this.messageGroup = systemProperties.getMessageGroup();
    this.deliveryTimestamp = systemProperties.getDeliveryTimestamp()?.toDate();
    this.keys = systemProperties.getKeysList();
    this.bornHost = systemProperties.getBornHost();
    this.bornTimestamp = systemProperties.getBornTimestamp()?.toDate();
    this.deliveryAttempt = systemProperties.getDeliveryAttempt();
    this.offset = systemProperties.getQueueOffset();
    this.receiptHandle = systemProperties.getReceiptHandle()!;
    this.transportDeliveryTimestamp = transportDeliveryTimestamp;
    this.liteTopic = systemProperties.getLiteTopic();
    this.priority = systemProperties.getPriority();
    if (messageQueue) {
      this.endpoints = messageQueue.broker.endpoints;
    }
    this.body = bodyBytes;
  }

  /**
   * Get the lite topic, which makes sense only when the topic type is LITE.
   *
   * @return lite topic, which is optional, undefined means lite topic is not specified.
   */
  getLiteTopic(): string | undefined {
    return this.liteTopic;
  }

  /**
   * Get the priority of the message, which makes sense only when topic type is priority.
   *
   * @return message priority, which is optional, undefined means priority is not specified.
   */
  getPriority(): number | undefined {
    return this.priority;
  }

  /**
   * Get the unique id of the message.
   *
   * @return unique id.
   */
  getMessageId(): string {
    return this.messageId;
  }

  /**
   * Get the topic of the message, which is the first classifier for the message.
   *
   * @return topic of the message.
   */
  getTopic(): string {
    return this.topic;
  }

  /**
   * Get the deep copy of the message body, which means any modification of the return value does not
   * affect the built-in message body.
   *
   * @return the deep copy of message body.
   */
  getBody(): Buffer {
    return Buffer.from(this.body);
  }

  /**
   * Get the deep copy of message properties, which makes the modifies of return value does
   * not affect the message itself.
   *
   * @return the deep copy of message properties.
   */
  getProperties(): Map<string, string> {
    return new Map(this.properties);
  }

  /**
   * Get the tag of the message, which is the second classifier besides the topic.
   *
   * @return the tag of message, which is optional, undefined means tag does not exist.
   */
  getTag(): string | undefined {
    return this.tag;
  }

  /**
   * Get the key collection of the message, which means any modification of the return value does not affect the
   * built-in message key collection.
   *
   * @return copy of the key collection of the message, empty array means message key is not specified.
   */
  getKeys(): string[] {
    return [ ...this.keys ];
  }

  /**
   * Get the message group, which makes sense only when the topic type is FIFO(First In, First Out).
   *
   * @return message group, which is optional, undefined means message group is not specified.
   */
  getMessageGroup(): string | undefined {
    return this.messageGroup;
  }

  /**
   * Get the expected delivery timestamp, which makes sense only when the topic type is delay.
   *
   * @return message expected delivery timestamp, which is optional, undefined means delivery timestamp is not specified.
   */
  getDeliveryTimestamp(): Date | undefined {
    return this.deliveryTimestamp;
  }

  /**
   * Get the born host of the message.
   *
   * @return born host of the message.
   */
  getBornHost(): string {
    return this.bornHost;
  }

  /**
   * Get the born timestamp of the message.
   *
   * <p>Born time means the timestamp that the message is prepared to send rather than the timestamp the
   * Message was built.
   *
   * @return born timestamp of the message.
   */
  getBornTimestamp(): Date | undefined {
    return this.bornTimestamp;
  }

  /**
   * Get the delivery attempt for the message.
   *
   * @return delivery attempt.
   */
  getDeliveryAttempt(): number | undefined {
    return this.deliveryAttempt;
  }
}
