package org.apache.rocketmq.client.exception;

import java.io.IOException;
import lombok.Getter;
import org.apache.rocketmq.proto.ResponseCode;

@Getter
public class MQServerException extends IOException {
    private ResponseCode code;

    public MQServerException(String msg) {
        super(msg);
    }

    public MQServerException(ResponseCode code, String msg) {
        super(msg);
        this.code = code;
    }
}
