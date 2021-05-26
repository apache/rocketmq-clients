package org.apache.rocketmq.client.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Permission {
    NONE(0),
    READ(1),
    WRITE(2),
    READ_WRITE(3);

    private final int value;

    public boolean isWritable() {
        switch (this) {
            case WRITE:
            case READ_WRITE:
                return true;
            case NONE:
            case READ:
            default:
                return false;
        }
    }

    public boolean isReadable() {
        switch (this) {
            case READ:
            case READ_WRITE:
                return true;
            case NONE:
            case WRITE:
            default:
                return false;
        }
    }
}
