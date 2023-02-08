using System;
using System.Collections.Generic;
using System.Linq;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Endpoints : IEquatable<Endpoints>
    {
        private static readonly AddressListEqualityComparer AddressListComparer = new();
        private static readonly string EndpointSeparator = ":";
        private List<Address> Addresses { get; }
        private AddressScheme Scheme { get; }
        private readonly int _hashCode;

        public Endpoints(global::Apache.Rocketmq.V2.Endpoints endpoints)
        {
            Addresses = new List<Address>();
            foreach (var address in endpoints.Addresses)
            {
                Addresses.Add(new Address(address));
            }

            if (!Addresses.Any())
            {
                throw new NotSupportedException("No available address");
            }

            switch (endpoints.Scheme)
            {
                case rmq.AddressScheme.Ipv4:
                    Scheme = AddressScheme.Ipv4;
                    break;
                case rmq.AddressScheme.Ipv6:
                    Scheme = AddressScheme.Ipv6;
                    break;
                case rmq.AddressScheme.DomainName:
                default:
                    Scheme = AddressScheme.DomainName;
                    if (Addresses.Count > 1)
                    {
                        throw new NotSupportedException("Multiple addresses are not allowed in domain scheme");
                    }

                    break;
            }

            unchecked
            {
                var hash = 17;
                hash = (hash * 31) + AddressListComparer.GetHashCode(Addresses);
                hash = (hash * 31) + (int)Scheme;
                _hashCode = hash;
            }
        }

        public Endpoints(string endpoints)
        {
            // TODO
            var strs = endpoints.Split(EndpointSeparator);
            Scheme = AddressScheme.DomainName;
            string host = strs[0];
            int port = int.Parse(strs[1]);
            Address address = new Address(host, port);
            var addresses = new List<Address>();
            addresses.Add(address);
            Addresses = addresses;
        }

        public string GrpcTarget
        {
            // TODO
            get
            {
                foreach (var address in Addresses)
                {
                    var target = "https://" + address.Host + ":" + address.Port;
                    // Console.WriteLine(target);
                    return "https://" + address.Host + ":" + address.Port;
                }

                return "";
            }
        }

        public bool Equals(Endpoints other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Addresses.SequenceEqual(other.Addresses) && Scheme == other.Scheme;
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

            return obj.GetType() == GetType() && Equals((Endpoints)obj);
        }

        public override int GetHashCode()
        {
            return _hashCode;
        }

        public rmq.Endpoints ToProtobuf()
        {
            var endpoints = new rmq.Endpoints();
            foreach (var address in Addresses)
            {
                endpoints.Addresses.Add(address.ToProtobuf());
            }

            endpoints.Scheme = AddressSchemeHelper.ToProtobuf(Scheme);
            return endpoints;
        }
    }
}