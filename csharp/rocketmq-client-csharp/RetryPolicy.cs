using System;

namespace Org.Apache.Rocketmq
{
    public interface RetryPolicy
    {
        int getMaxAttempts();

        TimeSpan getNextAttemptDelay(int attempt);

        global::Apache.Rocketmq.V2.RetryPolicy toProtobuf();
    }
}