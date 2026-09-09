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

import { MessageView } from '../message';
import {
  Attribute,
  AttributeKey,
  MessageHookPoints,
  MessageHookPointsStatus,
  MessageInterceptor,
  MessageInterceptorContextImpl,
} from '../hook';
import { ConsumeResult } from './ConsumeResult';
import { MessageListener } from './MessageListener';

// Context keys exposed to interceptors, mirroring the Java ConsumeTask keys.
export const REMOTE_ADDR_CONTEXT_KEY = AttributeKey.create<string>('remote_address');
export const MESSAGE_VIEW_CONTEXT_KEY = AttributeKey.create<MessageView>('message_view');
export const CONSUMER_GROUP_CONTEXT_KEY = AttributeKey.create<string>('consumer_group');
export const CONSUME_ERROR_CONTEXT_KEY = AttributeKey.create<unknown>('consume_error');

export class ConsumeTask {
  readonly #messageListener: MessageListener;
  readonly #messageView: MessageView;
  readonly #consumerGroup?: string;
  readonly #messageInterceptor?: MessageInterceptor;

  constructor(_clientId: string, messageListener: MessageListener, messageView: MessageView,
    messageInterceptor?: MessageInterceptor, consumerGroup?: string) {
    this.#messageListener = messageListener;
    this.#messageView = messageView;
    this.#messageInterceptor = messageInterceptor;
    this.#consumerGroup = consumerGroup;
  }

  async call(): Promise<ConsumeResult> {
    const generalMessages = [ this.#messageView ];
    let context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
    context.putAttribute(MESSAGE_VIEW_CONTEXT_KEY, Attribute.create(this.#messageView));
    if (this.#consumerGroup) {
      context.putAttribute(CONSUMER_GROUP_CONTEXT_KEY, Attribute.create(this.#consumerGroup));
    }
    let throwable: unknown;
    let consumeResult: ConsumeResult;
    if (this.#messageInterceptor) {
      this.#messageInterceptor.doBefore(context, generalMessages);
    }
    try {
      consumeResult = await this.#messageListener.consume(this.#messageView);
    } catch (e) {
      // Message listener raised an exception while consuming messages
      throwable = e;
      consumeResult = ConsumeResult.FAILURE;
    }
    const status = consumeResult === ConsumeResult.SUCCESS ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
    context = MessageInterceptorContextImpl.withStatus(context, status);
    if (throwable) {
      context.putAttribute(CONSUME_ERROR_CONTEXT_KEY, Attribute.create(throwable));
    }
    if (this.#messageInterceptor) {
      this.#messageInterceptor.doAfter(context, generalMessages);
    }
    return consumeResult;
  }
}
