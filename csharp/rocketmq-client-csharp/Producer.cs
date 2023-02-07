using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Proto = Apache.Rocketmq.V2;
using NLog;

namespace Org.Apache.Rocketmq
{
    public class Producer : Client, IProducer
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private readonly ConcurrentDictionary<string /* topic */, PublishingLoadBalancer> _publishingRouteDataCache;
        private readonly PublishingSettings _publishingSettings;


        public Producer(ClientConfig clientConfig) : this(clientConfig, new ConcurrentDictionary<string, bool>(), 3)
        {
        }

        public Producer(ClientConfig clientConfig, int maxAttempts) : this(clientConfig,
            new ConcurrentDictionary<string, bool>(), maxAttempts)
        {
        }

        private Producer(ClientConfig clientConfig, ConcurrentDictionary<string, bool> topics, int maxAttempts) :
            base(clientConfig, topics)
        {
            var retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(maxAttempts);
            _publishingSettings = new PublishingSettings(ClientId, clientConfig.Endpoints, retryPolicy,
                clientConfig.RequestTimeout, topics);
            _publishingRouteDataCache = new ConcurrentDictionary<string, PublishingLoadBalancer>();
        }

        public void SetTopics(params string[] topics)
        {
            foreach (var topic in topics)
            {
                Topics[topic] = false;
            }
        }

        public override async Task Start()
        {
            Logger.Info($"Begin to start the rocketmq producer, clientId={ClientId}");
            await base.Start();
            Logger.Info($"The rocketmq producer starts successfully, clientId={ClientId}");
        }

        public override async Task Shutdown()
        {
            Logger.Info($"Begin to shutdown the rocketmq producer, clientId={ClientId}");
            await base.Shutdown();
            Logger.Info($"Shutdown the rocketmq producer successfully, clientId={ClientId}");
        }

        protected override Proto::HeartbeatRequest WrapHeartbeatRequest()
        {
            return new Proto::HeartbeatRequest
            {
                ClientType = Proto.ClientType.Producer
            };
        }

        protected override void OnTopicDataFetched0(string topic, TopicRouteData topicRouteData)
        {
        }

        private RetryPolicy GetRetryPolicy()
        {
            return _publishingSettings.GetRetryPolicy();
        }

        public async Task<SendReceipt> Send(Message message)
        {
            if (!_publishingRouteDataCache.TryGetValue(message.Topic, out var publishingLoadBalancer))
            {
                var topicRouteData = await FetchTopicRoute(message.Topic);
                publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
                _publishingRouteDataCache.TryAdd(message.Topic, publishingLoadBalancer);
            }

            var publishingMessage = new PublishingMessage(message, _publishingSettings, false);
            var retryPolicy = GetRetryPolicy();
            var maxAttempts = retryPolicy.getMaxAttempts();
            var candidates = publishingLoadBalancer.TakeMessageQueues(publishingMessage.MessageGroup, maxAttempts);
            Exception exception = null;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    var sendReceipt = await Send0(publishingMessage, candidates, attempt, maxAttempts);
                    return sendReceipt;
                }
                catch (Exception e)
                {
                    exception = e;
                }
            }

            throw exception!;
        }

        private Proto.SendMessageRequest WrapSendMessageRequest(PublishingMessage message, MessageQueue mq)
        {
            return new Proto.SendMessageRequest
            {
                Messages = { message.ToProtobuf(mq.QueueId) }
            };
        }

        private async Task<SendReceipt> Send0(PublishingMessage message, List<MessageQueue> candidates, int attempt,
            int maxAttempts)
        {
            var candidateIndex = (attempt - 1) % candidates.Count;
            var mq = candidates[candidateIndex];
            if (_publishingSettings.IsValidateMessageType() &&
                !mq.AcceptMessageTypes.Contains(message.MessageType))
            {
                throw new ArgumentException($"Current message type does not match with the accept message types," +
                                            $" topic={message.Topic}, actualMessageType={message.MessageType}" +
                                            $" acceptMessageType={string.Join(",", mq.AcceptMessageTypes)}");
            }

            var sendMessageRequest = WrapSendMessageRequest(message, mq);
            var endpoints = mq.Broker.Endpoints;
            Proto.SendMessageResponse response =
                await Manager.SendMessage(endpoints, sendMessageRequest, ClientConfig.RequestTimeout);
            try
            {
                var sendReceipts = SendReceipt.processSendMessageResponse(response);

                var sendReceipt = sendReceipts.First();
                if (attempt > 1)
                {
                    Logger.Info(
                        $"Re-send message successfully, topic={message.Topic}, messageId={sendReceipt.MessageId}," +
                        $" maxAttempts={maxAttempts}, endpoints={endpoints}, clientId={ClientId}");
                }

                return sendReceipt;
            }
            catch (Exception e)
            {
                Logger.Warn(e, $"Failed to send message, topic={message.Topic}, maxAttempts={maxAttempts}, " +
                               $"endpoints={endpoints}, clientId={ClientId}");
                throw;
            }
        }

        public override Proto.Settings GetSettings()
        {
            return _publishingSettings.ToProtobuf();
        }

        public override void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
        {
            _publishingSettings.Sync(settings);
        }
    }
}