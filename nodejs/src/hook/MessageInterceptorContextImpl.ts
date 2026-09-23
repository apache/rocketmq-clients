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

import { Attribute } from './Attribute';
import { AttributeKey } from './AttributeKey';
import { MessageHookPoints } from './MessageHookPoints';
import { MessageHookPointsStatus } from './MessageHookPointsStatus';
import { MessageInterceptorContext } from './MessageInterceptorContext';

/**
 * Default {@link MessageInterceptorContext} implementation, mirroring
 * org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl.
 */
export class MessageInterceptorContextImpl implements MessageInterceptorContext {
  readonly #messageHookPoints: MessageHookPoints;
  readonly #status: MessageHookPointsStatus | undefined;
  readonly #attributes: Map<AttributeKey<any>, Attribute<any>>;

  constructor(messageHookPoints: MessageHookPoints,
    status?: MessageHookPointsStatus,
    attributes?: Map<AttributeKey<any>, Attribute<any>>) {
    this.#messageHookPoints = messageHookPoints;
    this.#status = status;
    this.#attributes = new Map(attributes ?? []);
  }

  /**
   * Derive a new context from an existing one with the given status, e.g. once the
   * outcome of the intercepted operation is known.
   */
  static withStatus(context: MessageInterceptorContext, status: MessageHookPointsStatus) {
    return new MessageInterceptorContextImpl(context.getMessageHookPoints(), status, context.getAttributes());
  }

  getMessageHookPoints(): MessageHookPoints {
    return this.#messageHookPoints;
  }

  getStatus(): MessageHookPointsStatus | undefined {
    return this.#status;
  }

  putAttribute<T>(key: AttributeKey<T>, value: Attribute<T>) {
    this.#attributes.set(key, value);
  }

  getAttribute<T>(key: AttributeKey<T>): Attribute<T> | undefined {
    return this.#attributes.get(key) as Attribute<T> | undefined;
  }

  getAttributes(): Map<AttributeKey<any>, Attribute<any>> {
    return this.#attributes;
  }
}
