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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PermissionTest {

    @Test
    public void testIsWritable() {
        assertFalse(Permission.NONE.isWritable());
        assertTrue(Permission.WRITE.isWritable());
        assertTrue(Permission.READ_WRITE.isWritable());
        assertFalse(Permission.READ.isWritable());
    }

    @Test
    public void testIsReadable() {
        assertFalse(Permission.NONE.isReadable());
        assertFalse(Permission.WRITE.isReadable());
        assertTrue(Permission.READ_WRITE.isReadable());
        assertTrue(Permission.READ.isReadable());
    }
}