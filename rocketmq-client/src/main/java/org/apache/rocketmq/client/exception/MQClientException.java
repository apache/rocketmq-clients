package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class MQClientException extends Exception {

    @Deprecated
    public MQClientException(String errorMessage) {
        super("Code: " + ErrorCode.OTHER + ", " + errorMessage);
    }

    public MQClientException(Throwable cause) {
        super("Code: " + ErrorCode.OTHER, cause);
    }

    public MQClientException(ErrorCode errorCode) {
        super("Code: " + errorCode);
    }

    public MQClientException(ErrorCode errorCode, String errorMessage) {
        super("Code: " + errorCode + ", " + errorMessage);
    }

    public MQClientException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("Code: " + errorCode + ", " + errorMessage, cause);
    }

    public MQClientException(ErrorCode errorCode, Throwable cause) {
        super("Code: " + errorCode, cause);
    }
}
