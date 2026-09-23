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

/**
 * Context carried through the message interceptor chain, mirroring
 * org.apache.rocketmq.client.java.hook.MessageInterceptorContext.
 */
export interface MessageInterceptorContext {
  getMessageHookPoints(): MessageHookPoints;
  getStatus(): MessageHookPointsStatus | undefined;
  putAttribute<T>(key: AttributeKey<T>, value: Attribute<T>): void;
  getAttribute<T>(key: AttributeKey<T>): Attribute<T> | undefined;
  getAttributes(): Map<AttributeKey<any>, Attribute<any>>;
}
