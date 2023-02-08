using System;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Address : IEquatable<Address>
    {
        public Address(rmq.Address address)
        {
            Host = address.Host;
            Port = address.Port;
        }

        public Address(string host, int port)
        {
            Host = host;
            Port = port;
        }

        public string Host { get; }

        public int Port { get; }

        public rmq.Address ToProtobuf()
        {
            return new rmq.Address
            {
                Host = Host,
                Port = Port
            };
        }

        public bool Equals(Address other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Host == other.Host && Port == other.Port;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType()) return false;
            return Equals((Address)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Host, Port);
        }
    }
}