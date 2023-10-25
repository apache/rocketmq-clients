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

import { TransactionResolution } from '../../proto/apache/rocketmq/v2/definition_pb';
import { MessageOptions, PublishingMessage } from '../message';
import type { Producer } from './Producer';
import { SendReceipt } from './SendReceipt';

export class Transaction {
  static readonly MAX_MESSAGE_NUM = 1;
  readonly #producer: Producer;
  readonly #messageMap = new Map<string, PublishingMessage>();
  readonly #messageSendReceiptMap = new Map<string, SendReceipt>();

  constructor(producer: Producer) {
    this.#producer = producer;
  }

  tryAddMessage(message: MessageOptions) {
    if (this.#messageMap.size >= Transaction.MAX_MESSAGE_NUM) {
      throw new TypeError(`Message in transaction has exceeded the threshold=${Transaction.MAX_MESSAGE_NUM}`);
    }
    const publishingMessage = new PublishingMessage(message, this.#producer.publishingSettings, true);
    this.#messageMap.set(publishingMessage.messageId, publishingMessage);
    return publishingMessage;
  }

  tryAddReceipt(publishingMessage: PublishingMessage, sendReceipt: SendReceipt) {
    if (!this.#messageMap.has(publishingMessage.messageId)) {
      throw new TypeError('Message not in transaction');
    }
    this.#messageSendReceiptMap.set(publishingMessage.messageId, sendReceipt);
  }

  async commit() {
    if (this.#messageSendReceiptMap.size === 0) {
      throw new TypeError('Transactional message has not been sent yet');
    }
    for (const [ messageId, sendReceipt ] of this.#messageSendReceiptMap.entries()) {
      const publishingMessage = this.#messageMap.get(messageId)!;
      await this.#producer.endTransaction(sendReceipt.endpoints, publishingMessage,
        sendReceipt.messageId, sendReceipt.transactionId, TransactionResolution.COMMIT);
    }
  }

  async rollback() {
    if (this.#messageSendReceiptMap.size === 0) {
      throw new TypeError('Transactional message has not been sent yet');
    }
    for (const [ messageId, sendReceipt ] of this.#messageSendReceiptMap.entries()) {
      const publishingMessage = this.#messageMap.get(messageId)!;
      await this.#producer.endTransaction(sendReceipt.endpoints, publishingMessage,
        sendReceipt.messageId, sendReceipt.transactionId, TransactionResolution.ROLLBACK);
    }
  }
}
