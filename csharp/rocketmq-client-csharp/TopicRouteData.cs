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
using System.Collections.Generic;

namespace org.apache.rocketmq {

    public class TopicRouteData : IEquatable<TopicRouteData> {

        public TopicRouteData(List<Partition> partitions) {
            this.partitions = partitions;
            this.partitions.Sort();
        }

        private List<Partition> partitions;

        public List<Partition> Partitions {
            get { return partitions; }
        }

        public bool Equals(TopicRouteData other) {
            return partitions.Equals(other.partitions);
        }

        public override bool Equals(object other)
        {

            if (!(other is TopicRouteData)) {
                return false;
            }

            return Equals(other as TopicRouteData);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(partitions);
        }

    }

}