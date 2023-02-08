using System.Collections.Generic;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class MessageQueue
    {
        public MessageQueue(rmq::MessageQueue messageQueue)
        {
            TopicResource = new Resource(messageQueue.Topic);
            QueueId = messageQueue.Id;
            Permission = PermissionHelper.FromProtobuf(messageQueue.Permission);
            var messageTypes = new List<MessageType>();
            foreach (var acceptMessageType in messageQueue.AcceptMessageTypes)
            {
                var messageType = MessageTypeHelper.FromProtobuf(acceptMessageType);
                messageTypes.Add(messageType);
            }

            AcceptMessageTypes = messageTypes;
            Broker = new Broker(messageQueue.Broker);
        }

        public Broker Broker { get; }

        public Resource TopicResource { get; }

        public Permission Permission { get; }

        public int QueueId { get; }

        public List<MessageType> AcceptMessageTypes { get; }

        public string Topic
        {
            get { return TopicResource.Name; }
        }
        
        
    }
}