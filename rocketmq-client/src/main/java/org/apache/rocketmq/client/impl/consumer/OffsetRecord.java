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

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class OffsetRecord implements Comparable<OffsetRecord> {
    private final long offset;

    @EqualsAndHashCode.Exclude
    private boolean release = false;

    public OffsetRecord(long offset) {
        this.offset = offset;
    }

    @Override
    public int compareTo(OffsetRecord o) {
        if (offset == o.getOffset()) {
            return 0;
        }
        if (offset > o.getOffset()) {
            return 1;
        }
        return -1;
    }
}
