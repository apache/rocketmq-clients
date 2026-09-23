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
import { MessageHookPointsStatus } from './MessageHookPointsStatus';
import { MessageInterceptorContext } from './MessageInterceptorContext';
import { MessageInterceptorContextImpl } from './MessageInterceptorContextImpl';
import { GeneralMessage, MessageInterceptor } from './MessageInterceptor';

// Key under which per-interceptor attributes are stashed between doBefore and doAfter.
const INTERCEPTOR_ATTRIBUTES_KEY = AttributeKey
  .create<Map<number, Map<AttributeKey<any>, Attribute<any>>>>('composited_interceptor_attributes');

/**
 * Composite interceptor which delegates to a chain of interceptors: doBefore runs
 * in registration order while doAfter runs in reverse order, mirroring
 * org.apache.rocketmq.client.java.hook.CompositedMessageInterceptor.
 */
export class CompositedMessageInterceptor implements MessageInterceptor {
  readonly #interceptors: MessageInterceptor[] = [];

  constructor(interceptors: MessageInterceptor[] = []) {
    this.#interceptors.push(...interceptors);
  }

  addInterceptor(interceptor: MessageInterceptor) {
    this.#interceptors.push(interceptor);
  }

  get size() {
    return this.#interceptors.length;
  }

  doBefore(context0: MessageInterceptorContext, messages: GeneralMessage[]) {
    const attributeMap = new Map<number, Map<AttributeKey<any>, Attribute<any>>>();
    for (let index = 0; index < this.#interceptors.length; index++) {
      const context = new MessageInterceptorContextImpl(context0.getMessageHookPoints(), context0.getStatus());
      // Share the attributes accumulated by the caller and previous interceptors.
      for (const [ key, value ] of context0.getAttributes()) {
        context.putAttribute(key, value);
      }
      try {
        this.#interceptors[index].doBefore(context, messages);
      } catch (t) {
        // Interceptors must never break the message pipeline.
        // eslint-disable-next-line no-console
        console.error(
          '[CompositedMessageInterceptor] Exception raised while handling messages before hook point=%s, error=%s',
          context0.getMessageHookPoints(), t);
      }
      attributeMap.set(index, context.getAttributes());
    }
    context0.putAttribute(INTERCEPTOR_ATTRIBUTES_KEY, Attribute.create(attributeMap));
    // Propagate attributes collected by the first interceptor to the caller context.
    const firstAttributes = attributeMap.get(0);
    if (firstAttributes) {
      for (const [ key, value ] of firstAttributes) {
        context0.putAttribute(key, value);
      }
    }
  }

  doAfter(context0: MessageInterceptorContext, messages: GeneralMessage[]) {
    const attributeMap = context0.getAttribute(INTERCEPTOR_ATTRIBUTES_KEY)?.get()
      ?? new Map<number, Map<AttributeKey<any>, Attribute<any>>>();
    for (let index = this.#interceptors.length - 1; index >= 0; index--) {
      const attributes = attributeMap.get(index);
      const context = new MessageInterceptorContextImpl(
        context0.getMessageHookPoints(), context0.getStatus() ?? MessageHookPointsStatus.OK, attributes);
      for (const [ key, value ] of context0.getAttributes()) {
        context.putAttribute(key, value);
      }
      try {
        this.#interceptors[index].doAfter(context, messages);
      } catch (t) {
        // eslint-disable-next-line no-console
        console.error(
          '[CompositedMessageInterceptor] Exception raised while handling messages after hook point=%s, error=%s',
          context0.getMessageHookPoints(), t);
      }
    }
  }
}
