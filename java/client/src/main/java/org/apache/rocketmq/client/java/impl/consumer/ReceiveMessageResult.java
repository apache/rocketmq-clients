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
    private final List<MessageViewImpl> messages;

    public ReceiveMessageResult(Endpoints endpoints, String requestId, Status status, List<MessageViewImpl> messages)
        throws ClientException {
        this.endpoints = endpoints;
        final Code code = status.getCode();
        final int codeNumber = code.getNumber();
        final String statusMessage = status.getMessage();
        switch (code) {
            case OK:
            case MESSAGE_NOT_FOUND:
                break;
            case BAD_REQUEST:
            case ILLEGAL_TOPIC:
            case ILLEGAL_CONSUMER_GROUP:
            case ILLEGAL_FILTER_EXPRESSION:
            case ILLEGAL_INVISIBLE_TIME:
            case CLIENT_ID_REQUIRED:
                throw new BadRequestException(codeNumber, requestId, statusMessage);
            case UNAUTHORIZED:
                throw new UnauthorizedException(codeNumber, requestId, statusMessage);
            case FORBIDDEN:
                throw new ForbiddenException(codeNumber, requestId, statusMessage);
            case NOT_FOUND:
            case TOPIC_NOT_FOUND:
            case CONSUMER_GROUP_NOT_FOUND:
                throw new NotFoundException(codeNumber, requestId, statusMessage);
            case TOO_MANY_REQUESTS:
                throw new TooManyRequestsException(codeNumber, requestId, statusMessage);
            case INTERNAL_ERROR:
            case INTERNAL_SERVER_ERROR:
                throw new InternalErrorException(codeNumber, requestId, statusMessage);
            case PROXY_TIMEOUT:
                throw new ProxyTimeoutException(codeNumber, requestId, statusMessage);
            default:
                throw new UnsupportedException(codeNumber, requestId, statusMessage);
        }
        this.messages = messages;
    }

    public List<MessageView> getMessageViews() {
        return new ArrayList<>(messages);
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public List<MessageViewImpl> getMessageViewImpls() {
        return messages;
    }
}
