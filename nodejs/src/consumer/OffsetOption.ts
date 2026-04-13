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
 * OffsetOption type enum.
 */
export enum OffsetType {
  POLICY = 0,
  OFFSET = 1,
  TAIL_N = 2,
  TIMESTAMP = 3,
}

/**
 * ProtoBuf OffsetOption type.
 */
import { OffsetOption as ProtoOffsetOption } from '../../proto/apache/rocketmq/v2/definition_pb';

/**
 * OffsetOption for specifying consume from offset.
 *
 * <p>OffsetOption is used to specify the starting point for message consumption.
 * It supports various policies and custom offsets.</p>
 */
export class OffsetOption {
  /** Policy constant: last offset */
  static readonly POLICY_LAST_VALUE = BigInt(0);

  /** Policy constant: min offset */
  static readonly POLICY_MIN_VALUE = BigInt(1);

  /** Policy constant: max offset */
  static readonly POLICY_MAX_VALUE = BigInt(2);

  /** Predefined option: LAST_OFFSET */
  static readonly LAST_OFFSET = new OffsetOption(OffsetType.POLICY, OffsetOption.POLICY_LAST_VALUE);

  /** Predefined option: MIN_OFFSET */
  static readonly MIN_OFFSET = new OffsetOption(OffsetType.POLICY, OffsetOption.POLICY_MIN_VALUE);

  /** Predefined option: MAX_OFFSET */
  static readonly MAX_OFFSET = new OffsetOption(OffsetType.POLICY, OffsetOption.POLICY_MAX_VALUE);

  private readonly type: OffsetType;
  private readonly value: bigint;

  private constructor(type: OffsetType, value: bigint) {
    this.type = type;
    this.value = value;
  }

  /**
   * Create an OffsetOption from a specific offset.
   *
   * @param offset - The offset value (must be >= 0)
   * @return OffsetOption instance
   * @throws Error if offset is negative
   */
  static ofOffset(offset: number | bigint): OffsetOption {
    const value = typeof offset === 'number' ? BigInt(offset) : offset;
    if (value < BigInt(0)) {
      throw new Error('offset must be greater than or equal to 0');
    }
    return new OffsetOption(OffsetType.OFFSET, value);
  }

  /**
   * Create an OffsetOption from tail N messages.
   *
   * @param tailN - Number of messages from the tail (must be >= 0)
   * @return OffsetOption instance
   * @throws Error if tailN is negative
   */
  static ofTailN(tailN: number | bigint): OffsetOption {
    const value = typeof tailN === 'number' ? BigInt(tailN) : tailN;
    if (value < BigInt(0)) {
      throw new Error('tailN must be greater than or equal to 0');
    }
    return new OffsetOption(OffsetType.TAIL_N, value);
  }

  /**
   * Create an OffsetOption from a timestamp.
   *
   * @param timestamp - Unix timestamp in milliseconds (must be >= 0)
   * @return OffsetOption instance
   * @throws Error if timestamp is negative
   */
  static ofTimestamp(timestamp: number | bigint): OffsetOption {
    const value = typeof timestamp === 'number' ? BigInt(timestamp) : timestamp;
    if (value < BigInt(0)) {
      throw new Error('timestamp must be greater than or equal to 0');
    }
    return new OffsetOption(OffsetType.TIMESTAMP, value);
  }

  /**
   * Get the type of this OffsetOption.
   *
   * @return OffsetType
   */
  getType(): OffsetType {
    return this.type;
  }

  /**
   * Get the value of this OffsetOption.
   *
   * @return Value as bigint
   */
  getValue(): bigint {
    return this.value;
  }

  /**
   * Returns a string representation of this OffsetOption.
   *
   * @return String representation
   */
  toString(): string {
    return `OffsetOption{type=${OffsetType[this.type]}, value=${this.value}}`;
  }

  /**
   * Validate that a bigint value is within the safe number range.
   *
   * @param value - The bigint value to validate
   * @throws Error if value is out of safe number range
   */
  private validateSafeNumberRange(value: bigint): void {
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new Error(`Value ${value} is out of safe number range`);
    }
  }

  /**
   * Convert this OffsetOption to ProtoBuf OffsetOption.
   *
   * @return ProtoBuf OffsetOption instance
   */
  toProtobuf(): ProtoOffsetOption {
    const proto = new ProtoOffsetOption();
    switch (this.type) {
      case OffsetType.POLICY: {
        // Map our policy values to ProtoBuf Policy enum
        // POLICY_LAST_VALUE (0) -> Policy.LAST (0)
        // POLICY_MIN_VALUE (1) -> Policy.MIN (1)
        // POLICY_MAX_VALUE (2) -> Policy.MAX (2)
        this.validateSafeNumberRange(this.value);
        proto.setPolicy(Number(this.value));
        break;
      }
      case OffsetType.OFFSET: {
        this.validateSafeNumberRange(this.value);
        proto.setOffset(Number(this.value));
        break;
      }
      case OffsetType.TAIL_N: {
        this.validateSafeNumberRange(this.value);
        proto.setTailN(Number(this.value));
        break;
      }
      case OffsetType.TIMESTAMP: {
        this.validateSafeNumberRange(this.value);
        proto.setTimestamp(Number(this.value));
        break;
      }
      default:
        throw new Error(`Unknown OffsetType: ${this.type}`);
    }
    return proto;
  }
}
