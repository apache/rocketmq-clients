using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.Rocketmq
{
    internal class AddressListEqualityComparer : IEqualityComparer<List<Address>>
    {
        public bool Equals(List<Address> a1, List<Address> a2)
        {
            if (ReferenceEquals(a1, a2))
            {
                return true;
            }

            if (ReferenceEquals(a1, null) || ReferenceEquals(a2, null))
            {
                return false;
            }

            return a1.Count == a2.Count && a1.SequenceEqual(a2);
        }

        public int GetHashCode(List<Address> s1)
        {
            var hash = 17;
            unchecked
            {
                hash = s1.Aggregate(hash, (current, s) => (current * 31) + s.GetHashCode());
            }

            return hash;
        }
    }
}