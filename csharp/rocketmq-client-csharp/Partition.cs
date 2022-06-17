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
    
    public class Partition : IEquatable<Partition>, IComparable<Partition> {

        public Partition(Topic topic, Broker broker, int id, Permission permission) {
            this.topic = topic;
            this.broker = broker;
            this.id = id;
            this.permission = permission;
        }

        private Topic topic;
        public Topic Topic{
            get { return topic; }
        }

        private Broker broker;
        public Broker Broker {
            get { return broker; }
        }

        private int id;
        public int Id {
            get { return id; }
        }

        Permission permission;
        public Permission Permission {
            get { return permission; }
        }

        public bool Equals(Partition other) {
            return topic.Equals(other.topic) 
                && broker.Equals(other.broker) 
                && id.Equals(other.id) 
                && permission == other.permission;
        }

        public override bool Equals(Object other) {
            if (!(other is Partition)) {
                return false;
            }
            return Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(topic, broker, id, permission);
        }

        public int CompareTo(Partition other) {
            if (0 != topic.CompareTo(other.topic)) {
                return topic.CompareTo(other.topic);
            }

            if (0 != broker.CompareTo(other.broker)) {
                return broker.CompareTo(other.broker);
            }

            if (0 != id.CompareTo(other.id)) {
                return id.CompareTo(other.id);
            }

            return permission.CompareTo(other.permission);
        }
    }
}