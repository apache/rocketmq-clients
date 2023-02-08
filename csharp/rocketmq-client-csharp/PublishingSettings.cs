using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Google.Protobuf.WellKnownTypes;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class PublishingSettings : Settings
    {
        private volatile int _maxBodySizeBytes = 4 * 1024 * 1024;
        private volatile bool _validateMessageType = true;

        public PublishingSettings(string clientId, Endpoints accessPoint, ExponentialBackoffRetryPolicy retryPolicy,
            TimeSpan requestTimeout, ConcurrentDictionary<string, bool> topics) : base(clientId, ClientType.Producer, accessPoint,
            retryPolicy, requestTimeout)
        {
            Topics = topics;
        }

        public ConcurrentDictionary<string, bool> Topics { get; }

        public int GetMaxBodySizeBytes()
        {
            return _maxBodySizeBytes;
        }

        public bool IsValidateMessageType()
        {
            return _validateMessageType;
        }

        public override void Sync(Proto::Settings settings)
        {
            // TODO
        }

        public override Proto.Settings ToProtobuf()
        {
            List<Proto.Resource> topics = new List<Proto.Resource>();
            foreach (var topic in Topics)
            {
                topics.Add(new Proto.Resource
                {
                    Name = topic.Key
                });
            }

            var publishing = new Proto.Publishing();
            publishing.Topics.Add(topics);
            publishing.ValidateMessageType = _validateMessageType;
            return new Proto.Settings
            {
                Publishing = publishing,
                AccessPoint = AccessPoint.ToProtobuf(),
                ClientType = ClientTypeHelper.ToProtobuf(ClientType),
                RequestTimeout = Duration.FromTimeSpan(RequestTimeout),
                BackoffPolicy = RetryPolicy.toProtobuf(),
                UserAgent = UserAgent.Instance.ToProtobuf()
            };
        }
    }
}