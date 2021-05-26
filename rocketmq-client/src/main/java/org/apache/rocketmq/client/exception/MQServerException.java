package org.apache.rocketmq.client.exception;

import java.io.IOException;
import lombok.Getter;

@Getter
public class MQServerException extends IOException {
    public MQServerException(String msg) {
        super(msg);
    }

}
