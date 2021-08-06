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

import lombok.Getter;

@Getter
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
