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

using System;

namespace Org.Apache.Rocketmq
{
    public class Topic : IComparable<Topic>, IEquatable<Topic>
    {
        public Topic(string resourceNamespace, string name)
        {
            ResourceNamespace = resourceNamespace;
            Name = name;
        }

        public string ResourceNamespace { get; }
        public string Name { get; }

        public bool Equals(Topic other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return ResourceNamespace == other.ResourceNamespace && Name == other.Name;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj) || obj.GetType() != GetType())
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return Equals((Topic)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(ResourceNamespace, Name);
        }

        public int CompareTo(Topic other)
        {
            if (ReferenceEquals(null, other))
            {
                return -1;
            }

            var compareTo = String.CompareOrdinal(ResourceNamespace, other.ResourceNamespace);
            if (0 == compareTo)
            {
                compareTo = String.CompareOrdinal(Name, other.Name);
            }

            return compareTo;
        }
    }
}