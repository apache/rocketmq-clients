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

import java.io.IOException;

public class ServerException extends IOException {

    @Deprecated
    public ServerException(String errorMessage) {
        super("ErrorCode: " + ErrorCode.OTHER + ", " + errorMessage);
    }

    public ServerException(ErrorCode errorCode) {
        super("ErrorCode: " + errorCode);
    }

    public ServerException(ErrorCode errorCode, String errorMessage) {
        super("ErrorCode: " + errorCode + ", " + errorMessage);
    }

    public ServerException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super("ErrorCode: " + errorCode + ", " + errorMessage, cause);
    }

    public ServerException(ErrorCode errorCode, Throwable cause) {
        super("ErrorCode: " + errorCode, cause);
    }
}
