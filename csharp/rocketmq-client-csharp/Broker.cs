using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Broker
    {
        public Broker(rmq.Broker broker)
        {
            Name = broker.Name;
            Id = broker.Id;
            Endpoints = new Endpoints(broker.Endpoints);
        }

        public string Name { get; }
        public int Id { get; }
        public Endpoints Endpoints { get; }
    }
}