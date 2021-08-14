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

package org.apache.rocketmq.client.route;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TopicRouteData {
    /**
     * Partitions of topic route
     */
    final List<Partition> partitions;


    /**
     * Construct topic route by partition list.
     *
     * @param partitionList partition list, should never be empty.
     */
    public TopicRouteData(List<apache.rocketmq.v1.Partition> partitionList) {
        this.partitions = new ArrayList<Partition>();
        for (apache.rocketmq.v1.Partition partition : partitionList) {
            this.partitions.add(new Partition(partition));
        }
    }

    public Set<Endpoints> allEndpoints() {
        Set<Endpoints> endpointsSet = new HashSet<Endpoints>();
        for (Partition partition : partitions) {
            endpointsSet.add(partition.getBroker().getEndpoints());
        }
        return endpointsSet;
    }

    public List<Partition> getPartitions() {
        return this.partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteData that = (TopicRouteData) o;
        return Objects.equal(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitions);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("partitions", partitions)
                          .toString();
    }
}
