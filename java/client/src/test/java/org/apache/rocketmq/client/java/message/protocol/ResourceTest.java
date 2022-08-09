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

package org.apache.rocketmq.client.java.message.protocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;

public class ResourceTest extends TestBase {

    @Test
    public void testGetterAndSetter() {
        Resource resource = new Resource("foobar");
        Assert.assertEquals(resource.getName(), "foobar");
        Assert.assertEquals(resource.getNamespace(), StringUtils.EMPTY);

        resource = new Resource("foo", "bar");
        Assert.assertEquals(resource.getName(), "bar");
        Assert.assertEquals(resource.getNamespace(), "foo");
    }

    @Test
    public void testToProtobuf() {
        Resource resource = new Resource("foo", "bar");
        final apache.rocketmq.v2.Resource protobuf = resource.toProtobuf();
        Assert.assertEquals(protobuf.getResourceNamespace(), "foo");
        Assert.assertEquals(protobuf.getName(), "bar");
    }

    @Test
    public void testEqual() {
        Resource resource0 = new Resource("foo", "bar");
        Resource resource1 = new Resource("foo", "bar");
        Assert.assertEquals(resource0, resource1);
        Resource resource2 = new Resource("foo0", "bar");
        Assert.assertNotEquals(resource0, resource2);
    }
}