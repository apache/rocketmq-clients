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
import {
  RetryPolicy as RetryPolicyPB,
  ExponentialBackoff,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { createDuration } from '../util';
import { RetryPolicy } from './RetryPolicy';

export class ExponentialBackoffRetryPolicy implements RetryPolicy {
  #maxAttempts: number;
  // milliseconds
  #initialBackoff: number;
  #maxBackoff: number;
  #backoffMultiplier: number;

  constructor(maxAttempts: number, initialBackoff = 0, maxBackoff = 0, backoffMultiplier = 1) {
    this.#maxAttempts = maxAttempts;
    this.#initialBackoff = initialBackoff;
    this.#maxBackoff = maxBackoff;
    this.#backoffMultiplier = backoffMultiplier;
  }

  static immediatelyRetryPolicy(maxAttempts: number) {
    return new ExponentialBackoffRetryPolicy(maxAttempts, 0, 0, 1);
  }

  getMaxAttempts(): number {
    return this.#maxAttempts;
  }

  getNextAttemptDelay(attempt: number): number {
    assert(attempt > 0, 'attempt must be positive');
    const delay = Math.min(this.#initialBackoff * Math.pow(this.#backoffMultiplier, 1.0 * (attempt - 1)), this.#maxBackoff);
    if (delay <= 0) {
      return 0;
    }
    return delay;
  }

  inheritBackoff(retryPolicy: RetryPolicyPB): RetryPolicy {
    assert(retryPolicy.getStrategyCase() === RetryPolicyPB.StrategyCase.EXPONENTIAL_BACKOFF,
      'strategy must be exponential backoff');
    const backoff = retryPolicy.getExponentialBackoff()!.toObject();
    // Convert protobuf Duration (seconds + nanos) to milliseconds without
    // losing sub-second precision (e.g. an initial backoff of 0.5s used to
    // be truncated to plain 0, causing immediate retries).
    const toMillis = (duration?: { seconds?: number; nanos?: number }) =>
      duration ? (duration.seconds ?? 0) * 1000 + (duration.nanos ?? 0) / 1e6 : 0;
    return new ExponentialBackoffRetryPolicy(this.#maxAttempts,
      toMillis(backoff.initial),
      toMillis(backoff.max),
      backoff.multiplier);
  }

  toProtobuf(): RetryPolicyPB {
    return new RetryPolicyPB()
      .setMaxAttempts(this.#maxAttempts)
      .setExponentialBackoff(
        new ExponentialBackoff()
          .setInitial(createDuration(this.#initialBackoff))
          .setMax(createDuration(this.#maxBackoff))
          .setMultiplier(this.#backoffMultiplier));
  }
}
