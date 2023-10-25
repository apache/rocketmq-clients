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

import { Code } from '../../proto/apache/rocketmq/v2/definition_pb';
import { SendMessageResponse } from '../../proto/apache/rocketmq/v2/service_pb';
import { MessageQueue } from '../route';
import { StatusChecker } from '../exception';

export class SendReceipt {
  readonly messageId: string;
  readonly transactionId: string;
  readonly offset: number;
  readonly #messageQueue: MessageQueue;

  constructor(messageId: string, transactionId: string, messageQueue: MessageQueue, offset: number) {
    this.messageId = messageId;
    this.transactionId = transactionId;
    this.offset = offset;
    this.#messageQueue = messageQueue;
  }

  get messageQueue() {
    return this.#messageQueue;
  }

  get endpoints() {
    return this.#messageQueue.broker.endpoints;
  }

  static processResponseInvocation(mq: MessageQueue, response: SendMessageResponse) {
    const responseObj = response.toObject();
    // Filter abnormal status.
    const abnormalStatus = responseObj.entriesList.map(e => e.status).find(s => s?.code !== Code.OK);
    const status = abnormalStatus ?? responseObj.status;
    StatusChecker.check(status);
    const sendReceipts: SendReceipt[] = [];
    for (const entry of responseObj.entriesList) {
      const messageId = entry.messageId;
      const transactionId = entry.transactionId;
      const offset = entry.offset;
      const sendReceipt = new SendReceipt(messageId, transactionId, mq, offset);
      sendReceipts.push(sendReceipt);
    }
    return sendReceipts;
  }
}
