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
import javax.annotation.concurrent.Immutable;
import org.apache.rocketmq.client.message.protocol.Resource;

@Immutable
public class Partition {
    private final Resource topicResource;
    private final Broker broker;
    private final int id;

    private final Permission permission;

    public Partition(apache.rocketmq.v1.Partition partition) {
        final apache.rocketmq.v1.Resource resource = partition.getTopic();
        this.topicResource = new Resource(resource.getArn(), resource.getName());
        this.id = partition.getId();
        final apache.rocketmq.v1.Permission perm = partition.getPermission();
        switch (perm) {
            case READ:
                this.permission = Permission.READ;
                break;
            case WRITE:
                this.permission = Permission.WRITE;
                break;
            case READ_WRITE:
                this.permission = Permission.READ_WRITE;
                break;
            case NONE:
            default:
                this.permission = Permission.NONE;
                break;
        }

        final String brokerName = partition.getBroker().getName();
        final int brokerId = partition.getBroker().getId();

        final apache.rocketmq.v1.Endpoints endpoints = partition.getBroker().getEndpoints();
        this.broker = new Broker(brokerName, brokerId, new Endpoints(endpoints));
    }

    public Resource getTopicResource() {
        return this.topicResource;
    }

    public Broker getBroker() {
        return this.broker;
    }

    public int getId() {
        return this.id;
    }

    public Permission getPermission() {
        return this.permission;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Partition partition = (Partition) o;
        return id == partition.id && Objects.equal(topicResource, partition.topicResource) &&
               Objects.equal(broker, partition.broker) && permission == partition.permission;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicResource, broker, id, permission);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("topicResource", topicResource)
                          .add("broker", broker)
                          .add("id", id)
                          .add("permission", permission)
                          .toString();
    }
}
