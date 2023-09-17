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

import address from 'address';

export enum MESSAGE_VERSION {
  V0 = 0x00,
  V1 = 0x01,
}

export class MessageId {
  id: string;
  /**
   * e.g.: 0x01: fixed 1 byte for current version
   * offset = 0
   */
  version: MESSAGE_VERSION;
  /**
   * e.g.: 0x56F7E71C361B: lower 6 bytes of local mac address
   * offset = 1
   */
  macAddress: string;
  /**
   * e.g.: 0x21BC: lower 2 bytes of process id
   * offset = 7
   */
  processId: number;
  /**
   * e.g: 0x024CCDBE: seconds since 2021-01-01 00:00:00(UTC+0, lower 4 bytes)
   * offset = 9
   */
  timestamp: number;
  /**
   * e.g.: 0x00000000: sequence number(4 bytes)
   * offset = 13
   */
  sequence: number;

  toString() {
    return this.id;
  }
}

const MAX_UINT32 = 0xFFFFFFFF;
const MAX_UINT16 = 0xFFFF;

/**
 * Message Identifier
 * https://github.com/apache/rocketmq-clients/blob/master/docs/message_id.md
 */
export class MessageIdFactory {
  // static #hostname = hostname();
  static #sequence = 0;
  static #buf = Buffer.alloc(1 + 6 + 2 + 4 + 4);
  // 2021-01-01 00:00:00(UTC+0), 1609459200000
  static #sinceTimestamp = new Date('2021-01-01T00:00:00Z').getTime() / 1000;
  // lower 2 bytes of process id
  static #processId = process.pid % MAX_UINT16;
  static MAC = '000000000000';

  static create() {
    const messageId = new MessageId();
    messageId.version = MESSAGE_VERSION.V1;
    messageId.macAddress = this.MAC;
    messageId.processId = this.#processId;
    messageId.timestamp = this.#getCurrentTimestamp();
    messageId.sequence = this.#sequence++;
    if (this.#sequence > MAX_UINT32) {
      this.#sequence = 0;
    }
    this.#buf.writeUInt8(messageId.version, 0);
    this.#buf.write(messageId.macAddress, 1, 'hex');
    this.#buf.writeUInt16BE(messageId.processId, 7);
    this.#buf.writeUInt32BE(messageId.timestamp, 9);
    this.#buf.writeUInt32BE(messageId.sequence, 13);
    messageId.id = this.#buf.toString('hex').toUpperCase();
    return messageId;
  }

  static decode(id: string) {
    const messageId = new MessageId();
    messageId.id = id;
    this.#buf.write(id, 0, 'hex');
    messageId.version = this.#buf.readUInt8(0);
    messageId.macAddress = this.#buf.subarray(1, 7).toString('hex');
    messageId.processId = this.#buf.readUInt16BE(7);
    messageId.timestamp = this.#buf.readUInt32BE(9);
    messageId.sequence = this.#buf.readUInt32BE(13);
    return messageId;
  }

  static #getCurrentTimestamp() {
    // use lower 4 bytes
    return Math.floor(Date.now() / 1000 - this.#sinceTimestamp) % MAX_UINT32;
  }
}

// set current mac address
address.mac((err, mac) => {
  if (err) {
    console.warn('[rocketmq-client-nodejs] can\'t get mac address, %s', err.message);
    return;
  }
  if (!mac) {
    console.warn('[rocketmq-client-nodejs] can\'t get mac address');
    return;
  }
  MessageIdFactory.MAC = mac.replaceAll(':', '');
});
