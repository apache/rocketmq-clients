package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class ClientException extends Exception {

    @Deprecated
    public ClientException(String errorMessage) {
        super("Code: " + ErrorCode.OTHER + ", " + errorMessage);
    }

    public ClientException(Throwable cause) {
        super("Code: " + ErrorCode.OTHER, cause);
    }

    public ClientException(ErrorCode errorCode) {
        super("Code: " + errorCode);
    }

    public ClientException(ErrorCode errorCode, String errorMessage) {
        super("Code: " + errorCode + ", " + errorMessage);
    }

    public ClientException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("Code: " + errorCode + ", " + errorMessage, cause);
    }

    public ClientException(ErrorCode errorCode, Throwable cause) {
        super("Code: " + errorCode, cause);
    }
}
