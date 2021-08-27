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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NextOffsetRecord {
    @GuardedBy("offsetRecordsLock")
    private final TreeSet<OffsetRecord> offsetRecords;
    private final ReadWriteLock offsetRecordsLock;

    public NextOffsetRecord() {
        this.offsetRecords = new TreeSet<OffsetRecord>();
        this.offsetRecordsLock = new ReentrantReadWriteLock();
    }

    /**
     * Add new offsets to {@link #offsetRecords}, release the old pilot if possible.
     *
     * @param offsetList list of offset.
     */
    public void add(List<Long> offsetList) {
        offsetRecordsLock.writeLock().lock();
        try {
            for (long offset : offsetList) {
                if (1 == offsetRecords.size()) {
                    final OffsetRecord record = offsetRecords.iterator().next();
                    if (record.isReleased() && record.getOffset() < offset) {
                        offsetRecords.remove(record);
                    }
                }
                offsetRecords.add(new OffsetRecord(offset));
            }
        } finally {
            offsetRecordsLock.writeLock().unlock();
        }
    }

    /**
     * Get the next offset of records.
     *
     * @return next offset. or null if not offset exists.
     */
    public Long next() {
        offsetRecordsLock.readLock().lock();
        try {
            if (offsetRecords.isEmpty()) {
                return null;
            }
            final OffsetRecord record = offsetRecords.iterator().next();
            if (record.isReleased()) {
                return record.getOffset() + 1;
            }
            return record.getOffset();
        } finally {
            offsetRecordsLock.readLock().unlock();
        }
    }

    public void remove(List<Long> offsetList) {
        offsetRecordsLock.writeLock().lock();
        try {
            for (Long offset : offsetList) {
                final OffsetRecord record = new OffsetRecord(offset);
                offsetRecords.remove(record);
                if (offsetRecords.isEmpty()) {
                    record.setReleased(true);
                    offsetRecords.add(record);
                }
            }
        } finally {
            offsetRecordsLock.writeLock().unlock();
        }
    }
}
