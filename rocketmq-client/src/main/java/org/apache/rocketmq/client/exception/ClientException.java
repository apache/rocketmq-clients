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

package org.apache.rocketmq.client.exception;

public class ClientException extends Exception {
    private final ErrorCode errorCode;

    @Deprecated
    public ClientException(String errorMessage) {
        super("ErrorCode: " + ErrorCode.OTHER + ", " + errorMessage);
        this.errorCode = ErrorCode.OTHER;
    }

    @Deprecated
    public ClientException(Throwable cause) {
        super("ErrorCode: " + ErrorCode.OTHER, cause);
        this.errorCode = ErrorCode.OTHER;
    }

    public ClientException(ErrorCode errorCode) {
        super("ErrorCode: " + errorCode);
        this.errorCode = errorCode;
    }

    public ClientException(ErrorCode errorCode, String errorMessage) {
        super("ErrorCode: " + errorCode + ", " + errorMessage);
        this.errorCode = errorCode;
    }

    public ClientException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("ErrorCode: " + errorCode + ", " + errorMessage, cause);
        this.errorCode = errorCode;
    }

    public ClientException(ErrorCode errorCode, Throwable cause) {
        super("ErrorCode: " + errorCode, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }
}
