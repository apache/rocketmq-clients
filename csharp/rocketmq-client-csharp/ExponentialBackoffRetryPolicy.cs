/*
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

using System;
using Proto = Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;

namespace Org.Apache.Rocketmq
{
    public class ExponentialBackoffRetryPolicy : IRetryPolicy
    {
        private readonly int _maxAttempts;

        private ExponentialBackoffRetryPolicy(int maxAttempts, TimeSpan initialBackoff, TimeSpan maxBackoff,
            double backoffMultiplier)
        {
            _maxAttempts = maxAttempts;
            InitialBackoff = initialBackoff;
            MaxBackoff = maxBackoff;
            BackoffMultiplier = backoffMultiplier;
        }

        public int GetMaxAttempts()
        {
            return _maxAttempts;
        }

        private TimeSpan InitialBackoff { get; }

        private TimeSpan MaxBackoff { get; }

        private double BackoffMultiplier { get; }

        public IRetryPolicy InheritBackoff(Proto.RetryPolicy retryPolicy)
        {
            if (retryPolicy.StrategyCase != Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff)
            {
                throw new InvalidOperationException("Strategy must be exponential backoff");
            }

            return InheritBackoff(retryPolicy.ExponentialBackoff);
        }

        private IRetryPolicy InheritBackoff(Proto.ExponentialBackoff retryPolicy)
        {
            return new ExponentialBackoffRetryPolicy(_maxAttempts, retryPolicy.Initial.ToTimeSpan(),
                retryPolicy.Max.ToTimeSpan(), retryPolicy.Multiplier);
        }

        public TimeSpan GetNextAttemptDelay(int attempt)
        {
            var delayMillis = Math.Min(
                InitialBackoff.TotalMilliseconds * Math.Pow(BackoffMultiplier, 1.0 * (attempt - 1)),
                MaxBackoff.TotalMilliseconds);
            return delayMillis < 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(delayMillis);
        }

        public static ExponentialBackoffRetryPolicy ImmediatelyRetryPolicy(int maxAttempts)
        {
            return new ExponentialBackoffRetryPolicy(maxAttempts, TimeSpan.Zero, TimeSpan.Zero, 1);
        }

        public global::Apache.Rocketmq.V2.RetryPolicy ToProtobuf()
        {
            var exponentialBackoff = new Proto.ExponentialBackoff
            {
                Multiplier = (float)BackoffMultiplier,
                Max = Duration.FromTimeSpan(MaxBackoff),
                Initial = Duration.FromTimeSpan(InitialBackoff)
            };
            return new global::Apache.Rocketmq.V2.RetryPolicy
            {
                MaxAttempts = _maxAttempts,
                ExponentialBackoff = exponentialBackoff
            };
        }
    }
}