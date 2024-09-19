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
using System.Collections.Generic;
using System.Linq;
using Apache.Rocketmq.V2;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class CustomizedBackoffRetryPolicy : IRetryPolicy
    {
        private readonly int _maxAttempts;
        private readonly List<TimeSpan> _durations;

        public CustomizedBackoffRetryPolicy(List<TimeSpan> durations, int maxAttempts)
        {
            if (durations == null || !durations.Any())
            {
                throw new ArgumentException("durations must not be empty", nameof(durations));
            }
            _durations = durations;
            _maxAttempts = maxAttempts;
        }

        public int GetMaxAttempts()
        {
            return _maxAttempts;
        }

        public List<TimeSpan> GetDurations()
        {
            return _durations;
        }

        public TimeSpan GetNextAttemptDelay(int attempt)
        {
            if (attempt <= 0)
            {
                throw new ArgumentException("attempt must be positive", nameof(attempt));
            }
            return attempt > _durations.Count ? _durations.Last() : _durations[attempt - 1];
        }

        public static CustomizedBackoffRetryPolicy FromProtobuf(RetryPolicy retryPolicy)
        {
            if (!retryPolicy.StrategyCase.Equals(RetryPolicy.StrategyOneofCase.CustomizedBackoff))
            {
                throw new ArgumentException("Illegal retry policy");
            }
            var customizedBackoff = retryPolicy.CustomizedBackoff;
            var durations = customizedBackoff.Next.Select(duration => duration.ToTimeSpan()).ToList();
            return new CustomizedBackoffRetryPolicy(durations, retryPolicy.MaxAttempts);
        }

        public RetryPolicy ToProtobuf()
        {
            var customizedBackoff = new CustomizedBackoff
            {
                Next = { _durations.Select(Duration.FromTimeSpan) }
            };
            return new RetryPolicy
            {
                MaxAttempts = _maxAttempts,
                CustomizedBackoff = customizedBackoff
            };
        }

        public IRetryPolicy InheritBackoff(Proto.RetryPolicy retryPolicy)
        {
            if (!retryPolicy.StrategyCase.Equals(RetryPolicy.StrategyOneofCase.CustomizedBackoff))
            {
                throw new InvalidOperationException("Strategy must be customized backoff");
            }

            return InheritBackoff(retryPolicy.CustomizedBackoff);
        }

        private IRetryPolicy InheritBackoff(CustomizedBackoff retryPolicy)
        {
            var durations = retryPolicy.Next.Select(duration => duration.ToTimeSpan()).ToList();
            return new CustomizedBackoffRetryPolicy(durations, _maxAttempts);
        }
    }
}