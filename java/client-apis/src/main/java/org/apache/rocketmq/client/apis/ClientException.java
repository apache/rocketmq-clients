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

package org.apache.rocketmq.client.apis;

/**
 * Base exception for all exceptions raised in client, each exception should derive from the current class.
 * It should throw exception which is derived from {@link ClientException} rather than {@link ClientException} itself.
 */
public class ClientException extends Exception {
    /**
     * For those {@link ClientException} along with a remote procedure call, request-id could be used to track the
     * request.
     */
    protected static final String REQUEST_ID_KEY = "request-id";
    protected static final String RESPONSE_CODE_KEY = "response-code";

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClientException(String message) {
        super(message);
    }

    public ClientException(Throwable t) {
        super(t);
    }

    public ClientException(int responseCode, String requestId, String message) {
        this("[" + REQUEST_ID_KEY + "=" + requestId + ", " + RESPONSE_CODE_KEY + "=" + responseCode + "] " + message);
    }
}
