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

/**
 * Typed key of a context attribute, mirroring
 * org.apache.rocketmq.client.java.hook.AttributeKey.
 *
 * The type parameter is only used for compile-time typing of the associated value.
 */
export class AttributeKey<T> {
  readonly name: string;
  /**
   * Never assigned at runtime; it only carries the value type for compile-time typing.
   */
  readonly typeHint?: T;

  constructor(name: string) {
    this.name = name;
  }

  static create<T>(name: string): AttributeKey<T> {
    return new AttributeKey<T>(name);
  }

  toString() {
    return `AttributeKey(${this.name})`;
  }
}
