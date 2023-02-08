using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public enum AddressScheme
    {
        DomainName,
        Ipv4,
        Ipv6,
    }

    public static class AddressSchemeHelper
    {
        public static rmq.AddressScheme ToProtobuf(AddressScheme scheme)
        {
            switch (scheme)
            {
                case AddressScheme.Ipv4:
                    return rmq.AddressScheme.Ipv4;
                case AddressScheme.Ipv6:
                    return rmq.AddressScheme.Ipv6;
                case AddressScheme.DomainName:
                default:
                    return rmq.AddressScheme.DomainName;
            }
        }
    }
}