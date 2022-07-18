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

package org.apache.rocketmq.client.java.route;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Status;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;
import javax.annotation.concurrent.Immutable;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;

/**
 * Result topic route data fetched from remote.
 */
@Immutable
public class TopicRouteDataResult {
    private final TopicRouteData topicRouteData;
    private final ClientException exception;

    public TopicRouteDataResult(RpcInvocation<QueryRouteResponse> invocation) {
        final QueryRouteResponse response = invocation.getResponse();
        final String requestId = invocation.getContext().getRequestId();
        final List<MessageQueue> messageQueuesList = response.getMessageQueuesList();
        final TopicRouteData topicRouteData = new TopicRouteData(messageQueuesList);
        final Status status = response.getStatus();
        this.topicRouteData = topicRouteData;
        final Code code = status.getCode();
        final int codeNumber = code.getNumber();
        final String statusMessage = status.getMessage();
        switch (code) {
            case OK:
                this.exception = null;
                break;
            case BAD_REQUEST:
            case ILLEGAL_ACCESS_POINT:
            case ILLEGAL_TOPIC:
            case CLIENT_ID_REQUIRED:
                this.exception = new BadRequestException(codeNumber, requestId, statusMessage);
                break;
            case NOT_FOUND:
            case TOPIC_NOT_FOUND:
                this.exception = new NotFoundException(codeNumber, requestId, statusMessage);
                break;
            case TOO_MANY_REQUESTS:
                this.exception = new TooManyRequestsException(codeNumber, requestId, statusMessage);
                break;
            case INTERNAL_ERROR:
            case INTERNAL_SERVER_ERROR:
                this.exception = new InternalErrorException(codeNumber, requestId, statusMessage);
                break;
            case PROXY_TIMEOUT:
                this.exception = new ProxyTimeoutException(codeNumber, requestId, statusMessage);
                break;
            default:
                this.exception = new UnsupportedException(codeNumber, requestId, statusMessage);
        }
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public TopicRouteData checkAndGetTopicRouteData() throws ClientException {
        if (null != exception) {
            throw exception;
        }
        return topicRouteData;
    }

    public boolean ok() {
        return null == exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteDataResult result = (TopicRouteDataResult) o;
        return Objects.equal(topicRouteData, result.topicRouteData) && Objects.equal(exception, result.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicRouteData, exception);
    }

    @Override
    public String toString() {
        final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
            .add("topicRouteData", this.topicRouteData);
        if (null == exception) {
            return helper.toString();
        }
        return helper.add("exception", this.exception).toString();
    }
}
