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

namespace org.apache.rocketmq {
    public class Broker : IComparable<Broker>, IEquatable<Broker> {

        public Broker(string name, int id, ServiceAddress address) {
            this.name = name;
            this.id = id;
            this.address = address;
        }

        private string name;
        public string Name {
            get { return name; }
        }

        private int id;
        public int Id {
            get { return id; }
        }

        private ServiceAddress address;
        public ServiceAddress Address {
            get { return address; }
        }

        public int CompareTo(Broker other) {
            if (0 != name.CompareTo(other.name)) {
                return name.CompareTo(other.name);
            }

            return id.CompareTo(other.id);
        }

        public bool Equals(Broker other) {
            return name.Equals(other.name) && id.Equals(other.id);
        }

        public override bool Equals(Object other) {
            if (!(other is Broker)) {
                return false;
            }
            return Equals(other as Broker);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(name, id);
        }
    }
}