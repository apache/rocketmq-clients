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
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.exception.ServerException;
import org.apache.rocketmq.client.misc.MixAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class TopicRouteData {
    public static final TopicRouteData EMPTY =
            new TopicRouteData(Collections.<apache.rocketmq.v1.Partition>emptyList());

    private static final Logger log = LoggerFactory.getLogger(TopicRouteData.class);

    private final AtomicInteger index;
    /**
     * Partitions of topic route
     */
    private final ImmutableList<Partition> partitions;

    /**
     * Construct topic route by partition list.
     *
     * @param partitionList partition list, should never be empty.
     */
    public TopicRouteData(List<apache.rocketmq.v1.Partition> partitionList) {
        this.index = new AtomicInteger(RandomUtils.nextInt());
        final ImmutableList.Builder<Partition> builder = ImmutableList.builder();
        for (apache.rocketmq.v1.Partition partition : partitionList) {
            builder.add(new Partition(partition));
        }
        this.partitions = builder.build();
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

    public Endpoints pickEndpointsToQueryAssignments() throws ServerException {
        int nextIndex = index.getAndIncrement();
        for (int i = 0; i < partitions.size(); i++) {
            final Partition partition = partitions.get(IntMath.mod(nextIndex++, partitions.size()));
            final Broker broker = partition.getBroker();
            if (MixAll.MASTER_BROKER_ID != broker.getId()) {
                continue;
            }
            if (Permission.NONE == partition.getPermission()) {
                continue;
            }
            return broker.getEndpoints();
        }
        log.error("No available endpoints, topicRouteData={}", this);
        throw new ServerException(ErrorCode.NO_PERMISSION, "No available endpoints to pick for query assignments");
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
