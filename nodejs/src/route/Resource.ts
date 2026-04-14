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

import { Resource as ProtoResource } from '../../proto/apache/rocketmq/v2/definition_pb';

/**
 * Represents a resource with namespace and name.
 */
export class Resource {
  private readonly namespace: string;
  private readonly name: string;

  constructor(namespace: string, name: string);
  constructor(name: string);
  constructor(namespaceOrName: string, name?: string) {
    if (name !== undefined) {
      this.namespace = namespaceOrName;
      this.name = name;
    } else {
      this.namespace = '';
      this.name = namespaceOrName;
    }
  }

  /**
   * Get the namespace.
   */
  getNamespace(): string {
    return this.namespace;
  }

  /**
   * Get the name.
   */
  getName(): string {
    return this.name;
  }

  /**
   * Convert to protobuf Resource.
   */
  toProtobuf(): ProtoResource {
    const resource = new ProtoResource();
    resource.setResourceNamespace(this.namespace);
    resource.setName(this.name);
    return resource;
  }

  /**
   * Create from protobuf Resource.
   */
  static fromProtobuf(proto: ProtoResource): Resource {
    const namespace = proto.getResourceNamespace();
    const name = proto.getName();
    return new Resource(namespace, name);
  }

  /**
   * Returns a string representation of the resource.
   */
  toString(): string {
    if (this.namespace && this.namespace.length > 0) {
      return `Resource{name='${this.name}', namespace='${this.namespace}'}`;
    }
    return this.name;
  }

  /**
   * Check equality with another Resource.
   */
  equals(other: Resource): boolean {
    if (this === other) return true;
    if (!other) return false;
    return this.namespace === other.namespace &&
      this.name === other.name;
  }

  /**
   * Calculate hash code.
   */
  hashCode(): number {
    let hash = 17;
    hash = hash * 31 + this.hashCodeOfString(this.namespace);
    hash = hash * 31 + this.hashCodeOfString(this.name);
    return hash;
  }

  private hashCodeOfString(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      // eslint-disable-next-line no-bitwise
      hash = ((hash << 5) - hash) + char;
      // eslint-disable-next-line no-bitwise
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
  }
}
