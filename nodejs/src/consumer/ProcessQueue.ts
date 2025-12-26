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

import { FilterExpression } from './FilterExpression';
import { MessageQueue } from '../route';
import { ConsumeService } from './ConsumeService';
import { ConsumeResult } from './Consumer';
import { PushConsumer } from './PushConsumer';

export class ProcessQueue {
  private readonly consumer: PushConsumer;
  private readonly messageQueue: MessageQueue;
  private readonly filterExpression: FilterExpression;
  private readonly consumeService: ConsumeService;
  public dropped = false;

  constructor(consumer: PushConsumer, messageQueue: MessageQueue, filterExpression: FilterExpression, consumeService: ConsumeService) {
    this.consumer = consumer;
    this.messageQueue = messageQueue;
    this.filterExpression = filterExpression;
    this.consumeService = consumeService;
  }

  async fetchMessage() {
    if (this.dropped) {
      return;
    }
    const request = this.consumer.wrapReceiveMessageRequest(16, this.messageQueue, this.filterExpression, 15000, 15000);
    try {
      const messages = await this.consumer.receiveMessage(request, this.messageQueue, 3000 + 15000);
      for (const message of messages) {
        const result = await this.consumeService.consume(message);
        if (result === ConsumeResult.SUCCESS) {
          await this.consumer.ackMessage(message);
        } else {
            await this.consumer.changeInvisibleDuration(message, 15000);
        }
      }
    } catch (error) {
      this.consumer.logger.error('Failed to receive message', error);
      // Wait for a while before retrying
      await new Promise(resolve => setTimeout(resolve, 1000));
    } finally {
      this.fetchMessage();
    }
  }
}
