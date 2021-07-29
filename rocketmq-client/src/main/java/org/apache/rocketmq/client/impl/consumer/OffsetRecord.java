package org.apache.rocketmq.client.impl.consumer;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class OffsetRecord implements Comparable<OffsetRecord> {
    private final long offset;
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
