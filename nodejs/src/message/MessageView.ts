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
    if (messageQueue) {
      this.endpoints = messageQueue.broker.endpoints;
    }
    this.body = bodyBytes;
  }
}
