package org.apache.rocketmq.client.message.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum MessageType {
    NORMAL("normal"),
    FIFO("fifo"),
    DELAY("delay"),
    TRANSACTION("transaction");


    private final String name;
}
