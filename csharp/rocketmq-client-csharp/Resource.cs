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
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Resource : IEquatable<Resource>
    {
        public Resource(string namespaceName, string name)
        {
            Namespace = namespaceName;
            Name = name;
        }

        public Resource(Proto.Resource resource)
        {
            Namespace = resource.ResourceNamespace;
            Name = resource.Name;
        }

        public Resource(string name)
        {
            Namespace = "";
            Name = name;
        }

        public string Namespace { get; }
        public string Name { get; }

        public Proto.Resource ToProtobuf()
        {
            return new Proto.Resource
            {
                ResourceNamespace = Namespace,
                Name = Name
            };
        }

        public bool Equals(Resource other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return Name == other.Name && Namespace == other.Namespace;
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

            return obj.GetType() == GetType() && Equals((Resource)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Namespace, Name);
        }

        public override string ToString()
        {
            return string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        }


    }
}