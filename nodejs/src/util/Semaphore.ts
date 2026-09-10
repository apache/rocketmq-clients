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
 * A counting semaphore used to throttle the number of messages that can be
 * in-flight (fetched but not yet settled) on a single {@link ProcessQueue}.
 *
 * This mirrors the permit model of the Java client's
 * {@code ProcessQueueImpl#acquirePermit()}/{@code releasePermit()}, where the
 * permit count is bounded by {@code consumeConcurrentlyMax} and exhaustion
 * applies backpressure by stopping further message fetches until permits are
 * released (i.e. messages are acked / nacked).
 */
export class Semaphore {
  #permits: number;
  readonly #waiters: Array<() => void> = [];

  constructor(permits: number) {
    if (!Number.isInteger(permits) || permits < 0) {
      throw new RangeError(`permits must be a non-negative integer, got ${permits}`);
    }
    this.#permits = permits;
  }

  /** Current number of available permits. */
  get availablePermits(): number {
    return this.#permits;
  }

  /**
   * Non-blocking acquisition: takes one permit if available and returns true,
   * otherwise leaves the semaphore unchanged and returns false.
   */
  tryAcquire(): boolean {
    if (this.#permits > 0) {
      this.#permits--;
      return true;
    }
    return false;
  }

  /**
   * Acquire a single permit. Resolves immediately when one is available;
   * otherwise queues the caller until a permit is released.
   */
  acquire(): Promise<void> {
    if (this.#permits > 0) {
      this.#permits--;
      return Promise.resolve();
    }
    return new Promise<void>(resolve => {
      this.#waiters.push(resolve);
    });
  }

  /**
   * Release a single permit. Hands it to a pending acquirer if any, otherwise
   * increments the available count.
   */
  release(): void {
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter();
    } else {
      this.#permits++;
    }
  }
}
