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

package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ForbiddenException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnauthorizedException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;

public class ReceiveMessageResult {
    private final Endpoints endpoints;
    private final String requestId;
    private final ClientException exception;

    private final List<MessageViewImpl> messages;

    public ReceiveMessageResult(Endpoints endpoints, String requestId, Status status, List<MessageViewImpl> messages) {
        this.endpoints = endpoints;
        this.requestId = requestId;
        final Code code = status.getCode();
        switch (code) {
            case OK:
                this.exception = null;
                break;
            case BAD_REQUEST:
            case ILLEGAL_TOPIC:
            case ILLEGAL_CONSUMER_GROUP:
            case ILLEGAL_FILTER_EXPRESSION:
            case CLIENT_ID_REQUIRED:
                this.exception = new BadRequestException(code.getNumber(), status.getMessage());
                break;
            case UNAUTHORIZED:
                this.exception = new UnauthorizedException(code.getNumber(), status.getMessage());
                break;
            case FORBIDDEN:
                this.exception = new ForbiddenException(code.getNumber(), status.getMessage());
                break;
            case MESSAGE_NOT_FOUND:
            case NOT_FOUND:
            case TOPIC_NOT_FOUND:
            case CONSUMER_GROUP_NOT_FOUND:
                this.exception = new NotFoundException(code.getNumber(), status.getMessage());
                break;
            case TOO_MANY_REQUESTS:
                this.exception = new TooManyRequestsException(code.getNumber(), status.getMessage());
                break;
            case INTERNAL_ERROR:
            case INTERNAL_SERVER_ERROR:
                this.exception = new InternalErrorException(code.getNumber(), status.getMessage());
                break;
            case PROXY_TIMEOUT:
                this.exception = new ProxyTimeoutException(code.getNumber(), status.getMessage());
                break;
            default:
                this.exception = new UnsupportedException(code.getNumber(), status.getMessage());
        }
        this.messages = messages;
    }

    /**
     * Indicates that the result is ok or not.
     *
     * <p>The result is ok if the status code is {@link Code#OK} or {@link Code#MESSAGE_NOT_FOUND}.
     *
     * @return true if the result is ok, false otherwise.
     */
    public boolean ok() {
        return null == exception || (exception.getResponseCode().isPresent() && Code.OK.getNumber() ==
            exception.getResponseCode().get());
    }

    public List<MessageView> checkAndGetMessages() throws ClientException {
        if (null != exception) {
            throw exception;
        }
        return new ArrayList<>(messages);
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public List<MessageViewImpl> getMessages() {
        return messages;
    }

    public String getRequestId() {
        return requestId;
    }
}
