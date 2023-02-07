/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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