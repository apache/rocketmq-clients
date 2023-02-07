using System;

namespace Org.Apache.Rocketmq
{
    public interface IRetryPolicy
    {
        int GetMaxAttempts();

        TimeSpan GetNextAttemptDelay(int attempt);

        global::Apache.Rocketmq.V2.RetryPolicy ToProtobuf();
    }
}