using System;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public abstract class Settings
    {
        protected readonly string ClientId;
        protected readonly ClientType ClientType;
        protected readonly Endpoints AccessPoint;
        protected volatile RetryPolicy RetryPolicy;
        protected readonly TimeSpan RequestTimeout;

        public Settings(string clientId, ClientType clientType, Endpoints accessPoint, RetryPolicy retryPolicy,
            TimeSpan requestTimeout)
        {
            ClientId = clientId;
            ClientType = clientType;
            AccessPoint = accessPoint;
            RetryPolicy = retryPolicy;
            RequestTimeout = requestTimeout;
        }

        public Settings(string clientId, ClientType clientType, Endpoints accessPoint, TimeSpan requestTimeout)
        {
            ClientId = clientId;
            ClientType = clientType;
            AccessPoint = accessPoint;
            RetryPolicy = null;
            RequestTimeout = requestTimeout;
        }

        public abstract rmq::Settings ToProtobuf();

        public abstract void Sync(rmq::Settings settings);

        public RetryPolicy GetRetryPolicy()
        {
            return RetryPolicy;
        }
    }
}