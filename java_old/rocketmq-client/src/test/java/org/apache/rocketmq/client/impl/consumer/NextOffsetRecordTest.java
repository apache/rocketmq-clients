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

package org.apache.rocketmq.client.impl.consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class NextOffsetRecordTest {

    @Test
    public void testAdd() {
        final NextOffsetRecord nextOffsetRecord = new NextOffsetRecord();

        assertFalse(nextOffsetRecord.next().isPresent());
        List<Long> offsetsToAdd0 = new ArrayList<Long>();
        offsetsToAdd0.add(3L);
        nextOffsetRecord.add(offsetsToAdd0);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 3L);

        List<Long> offsetsToAdd1 = new ArrayList<Long>();
        offsetsToAdd1.add(3L);
        offsetsToAdd1.add(4L);
        offsetsToAdd1.add(5L);
        nextOffsetRecord.add(offsetsToAdd1);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 3L);

        List<Long> offsetsToAdd2 = new ArrayList<Long>();
        offsetsToAdd2.add(4L);
        offsetsToAdd2.add(5L);
        nextOffsetRecord.add(offsetsToAdd2);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 3L);

        List<Long> offsetsToAdd3 = new ArrayList<Long>();
        offsetsToAdd3.add(9L);
        offsetsToAdd3.add(10L);
        nextOffsetRecord.add(offsetsToAdd3);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 3L);

        List<Long> offsetsToAdd4 = new ArrayList<Long>();
        offsetsToAdd4.add(1L);
        nextOffsetRecord.add(offsetsToAdd4);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 1L);
    }

    @Test
    public void testRemove() {
        final NextOffsetRecord nextOffsetRecord = new NextOffsetRecord();

        assertFalse(nextOffsetRecord.next().isPresent());
        List<Long> offsetsToAdd0 = new ArrayList<Long>();
        offsetsToAdd0.add(1L);
        offsetsToAdd0.add(2L);
        offsetsToAdd0.add(3L);
        nextOffsetRecord.add(offsetsToAdd0);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 1L);

        List<Long> offsetsToRemove0 = new ArrayList<Long>();
        offsetsToRemove0.add(1L);
        nextOffsetRecord.remove(offsetsToRemove0);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 2L);

        List<Long> offsetsToRemove1 = new ArrayList<Long>();
        offsetsToRemove1.add(3L);
        nextOffsetRecord.remove(offsetsToRemove1);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 2L);

        List<Long> offsetsToRemove2 = new ArrayList<Long>();
        offsetsToRemove2.add(2L);
        nextOffsetRecord.remove(offsetsToRemove2);
        assertTrue(nextOffsetRecord.next().isPresent());
        assertEquals((long) nextOffsetRecord.next().get(), 3L);
    }
}