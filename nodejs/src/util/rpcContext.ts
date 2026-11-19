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

import type { Metadata } from '@grpc/grpc-js';

// Associate request IDs with response objects to avoid mixing concurrent RPCs without changing protobufs or public return types.
const requestIds = new WeakMap<object, string>();

export function setRequestId<T extends object>(response: T, requestId?: string): T {
  if (requestId !== undefined) requestIds.set(response, requestId);
  return response;
}

export function withRequestId<T extends object>(response: T, metadata: Metadata): T {
  return setRequestId(response, metadata.get('x-mq-request-id')[0]?.toString());
}

export function getRequestId(response?: object): string | undefined {
  return response ? requestIds.get(response) : undefined;
}
