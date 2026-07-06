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
 * Consume result for push consumer.
 *
 * <p>Designed for push consumer specifically. This is a class-based result
 * (matching the Java client) so that it can be extended with suspend results.</p>
 */
export class ConsumeResult {
  /**
   * Consume message successfully.
   */
  static readonly SUCCESS = new ConsumeResult('SUCCESS');

  /**
   * Failed to consume message.
   */
  static readonly FAILURE = new ConsumeResult('FAILURE');

  readonly name: string;

  protected constructor(name: string) {
    this.name = name;
  }

  toString(): string {
    return this.name;
  }
}

const MIN_SUSPEND_TIME_MS = 50;

/**
 * Suspend consume result.
 *
 * <p>When returned by the message listener, the consumer will suspend
 * consumption of the current message (and related messages in FIFO mode)
 * for the specified duration by changing the invisible duration.</p>
 */
export class ConsumeResultSuspend extends ConsumeResult {
  readonly suspendTimeMs: number;

  private constructor(suspendTimeMs: number) {
    super('SUSPEND');
    if (suspendTimeMs < MIN_SUSPEND_TIME_MS) {
      throw new Error(`suspend time cannot be less than ${MIN_SUSPEND_TIME_MS}ms, got ${suspendTimeMs}ms`);
    }
    this.suspendTimeMs = suspendTimeMs;
  }

  /**
   * Create a suspend result with the given suspend time in milliseconds.
   *
   * @param suspendTimeMs - Suspend time in milliseconds
   * @return {ConsumeResultSuspend} ConsumeResultSuspend instance
   * @throws {Error} if suspendTimeMs is less than 50ms
   */
  static of(suspendTimeMs: number): ConsumeResultSuspend {
    return new ConsumeResultSuspend(suspendTimeMs);
  }

  toString(): string {
    return `SUSPEND(${this.suspendTimeMs}ms)`;
  }
}
