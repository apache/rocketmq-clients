using System;
using Apache.Rocketmq.V2;
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

        public TimeSpan InitialBackoff { get; }

        public TimeSpan MaxBackoff { get; }

        public double BackoffMultiplier { get; }

        public TimeSpan GetNextAttemptDelay(int attempt)
        {
            return TimeSpan.Zero;
        }

        public static ExponentialBackoffRetryPolicy ImmediatelyRetryPolicy(int maxAttempts)
        {
            return new ExponentialBackoffRetryPolicy(maxAttempts, TimeSpan.Zero, TimeSpan.Zero, 1);
        }

        public global::Apache.Rocketmq.V2.RetryPolicy ToProtobuf()
        {
            var exponentialBackoff = new ExponentialBackoff
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