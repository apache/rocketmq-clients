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

import assert from 'node:assert';
import { Duration } from 'google-protobuf/google/protobuf/duration_pb';
import {
  RetryPolicy as RetryPolicyPB,
  CustomizedBackoff,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { createDuration } from '../util';
import { RetryPolicy } from './RetryPolicy';

/**
 * Backoff policy whose durations are customized by the server side,
 * mirroring Java's CustomizedBackoffRetryPolicy: the Nth attempt waits
 * durations[N - 1], and once attempts run past the list, the last
 * duration applies.
 */
export class CustomizedBackoffRetryPolicy implements RetryPolicy {
  #maxAttempts: number;
  // Backoff durations in milliseconds.
  #durations: number[];

  constructor(durations: number[], maxAttempts: number) {
    assert(Array.isArray(durations) && durations.length > 0, 'durations must not be empty');
    this.#durations = durations;
    this.#maxAttempts = maxAttempts;
  }

  getMaxAttempts(): number {
    return this.#maxAttempts;
  }

  getNextAttemptDelay(attempt: number): number {
    assert(attempt > 0, 'attempt must be positive');
    return attempt > this.#durations.length
      ? this.#durations[this.#durations.length - 1]
      : this.#durations[attempt - 1];
  }

  inheritBackoff(retryPolicy: RetryPolicyPB): RetryPolicy {
    assert(retryPolicy.getStrategyCase() === RetryPolicyPB.StrategyCase.CUSTOMIZED_BACKOFF,
      'strategy must be customized backoff');
    return new CustomizedBackoffRetryPolicy(
      CustomizedBackoffRetryPolicy.durationsToMillis(retryPolicy.getCustomizedBackoff()!),
      this.#maxAttempts);
  }

  toProtobuf(): RetryPolicyPB {
    const customizedBackoff = new CustomizedBackoff();
    for (const duration of this.#durations) {
      customizedBackoff.addNext(createDuration(duration));
    }
    return new RetryPolicyPB()
      .setMaxAttempts(this.#maxAttempts)
      .setCustomizedBackoff(customizedBackoff);
  }

  /**
   * Convert the protobuf CustomizedBackoff durations (seconds + nanos) to
   * milliseconds without losing sub-second precision.
   */
  static durationsToMillis(customizedBackoff: CustomizedBackoff): number[] {
    return customizedBackoff.getNextList().map((duration: Duration) =>
      duration.getSeconds() * 1000 + duration.getNanos() / 1e6);
  }
}
