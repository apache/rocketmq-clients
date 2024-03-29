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

import { RetryPolicy as RetryPolicyPB } from '../../proto/apache/rocketmq/v2/definition_pb';

/**
 * Internal interface for retry policy.
 */
export interface RetryPolicy {
  /**
   * Get the max attempt times for retry.
   *
   * @return max attempt times.
   */
  getMaxAttempts(): number;

  /**
   * Get await time after current attempts, the attempt index starts at 1.
   *
   * @param attempt current attempt.
   * @return await time in seconds.
   */
  getNextAttemptDelay(attempt: number): number;

  /**
   * Update the retry backoff strategy and generate a new one.
   *
   * @param retryPolicy retry policy which contains the backoff strategy.
   * @return the new retry policy.
   */
  inheritBackoff(retryPolicy: RetryPolicyPB): RetryPolicy;

  /**
   * Convert to RetryPolicyPB
   */
  toProtobuf(): RetryPolicyPB;
}
