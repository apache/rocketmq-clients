package org.apache.rocketmq.client.exception;

import java.io.IOException;
import lombok.Getter;

@Getter
public class ServerException extends IOException {

    @Deprecated
    public ServerException(String errorMessage) {
        super("Code: " + ErrorCode.OTHER + ", " + errorMessage);
    }

    public ServerException(ErrorCode errorCode) {
        super("Code: " + errorCode);
    }

    public ServerException(ErrorCode errorCode, String errorMessage) {
        super("Code: " + errorCode + ", " + errorMessage);
    }

    public ServerException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("Code: " + errorCode + ", " + errorMessage, cause);
    }

    public ServerException(ErrorCode errorCode, Throwable cause) {
        super("Code: " + errorCode, cause);
    }
}
