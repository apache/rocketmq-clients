package org.apache.rocketmq.client.exception;

import java.io.IOException;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Getter
@ToString
@Slf4j
public class MQServerException extends IOException {

    @Deprecated
    public MQServerException(String errorMessage) {
        super("Code: " + ErrorCode.OTHER + ", " + errorMessage);
    }

    public MQServerException(ErrorCode errorCode) {
        super("Code: " + errorCode);
    }

    public MQServerException(ErrorCode errorCode, String errorMessage) {
        super("Code: " + errorCode + ", " + errorMessage);
    }

    public MQServerException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("Code: " + errorCode + ", " + errorMessage, cause);
    }

    public MQServerException(ErrorCode errorCode, Throwable cause) {
        super("Code: " + errorCode, cause);
    }
}
