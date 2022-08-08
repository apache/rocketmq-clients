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

package org.apache.rocketmq.client.java.exception;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Status;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.misc.MetadataUtils;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatusChecker.class);

    private StatusChecker() {
    }

    public static void check(Status status, RpcInvocation<?> invocation) throws ClientException {
        final String requestId = invocation.getContext().getRequestId();
        final Code code = status.getCode();
        final int codeNumber = code.getNumber();
        final String statusMessage = status.getMessage();
        switch (code) {
            case OK:
            case MULTIPLE_RESULTS:
                return;
            case BAD_REQUEST:
            case ILLEGAL_ACCESS_POINT:
            case ILLEGAL_TOPIC:
            case ILLEGAL_CONSUMER_GROUP:
            case ILLEGAL_MESSAGE_TAG:
            case ILLEGAL_MESSAGE_KEY:
            case ILLEGAL_MESSAGE_GROUP:
            case ILLEGAL_MESSAGE_PROPERTY_KEY:
            case INVALID_TRANSACTION_ID:
            case ILLEGAL_MESSAGE_ID:
            case ILLEGAL_FILTER_EXPRESSION:
            case ILLEGAL_INVISIBLE_TIME:
            case ILLEGAL_DELIVERY_TIME:
            case INVALID_RECEIPT_HANDLE:
            case MESSAGE_PROPERTY_CONFLICT_WITH_TYPE:
            case UNRECOGNIZED_CLIENT_TYPE:
            case MESSAGE_CORRUPTED:
            case CLIENT_ID_REQUIRED:
                throw new BadRequestException(codeNumber, requestId, statusMessage);
            case UNAUTHORIZED:
                throw new UnauthorizedException(codeNumber, requestId, statusMessage);
            case PAYMENT_REQUIRED:
                throw new PaymentRequiredException(codeNumber, requestId, statusMessage);
            case FORBIDDEN:
                throw new ForbiddenException(codeNumber, requestId, statusMessage);
            case MESSAGE_NOT_FOUND:
                if (invocation.getResponse() instanceof ReceiveMessageResponse) {
                    return;
                }
                // fall through on purpose.
            case NOT_FOUND:
            case TOPIC_NOT_FOUND:
            case CONSUMER_GROUP_NOT_FOUND:
                throw new NotFoundException(codeNumber, requestId, statusMessage);
            case PAYLOAD_TOO_LARGE:
            case MESSAGE_BODY_TOO_LARGE:
                throw new PayloadTooLargeException(codeNumber, requestId, statusMessage);
            case TOO_MANY_REQUESTS:
                throw new TooManyRequestsException(codeNumber, requestId, statusMessage);
            case REQUEST_HEADER_FIELDS_TOO_LARGE:
            case MESSAGE_PROPERTIES_TOO_LARGE:
                throw new RequestHeaderFieldsTooLargeException(codeNumber, requestId, statusMessage);
            case INTERNAL_ERROR:
            case INTERNAL_SERVER_ERROR:
            case HA_NOT_AVAILABLE:
                throw new InternalErrorException(codeNumber, requestId, statusMessage);
            case PROXY_TIMEOUT:
            case MASTER_PERSISTENCE_TIMEOUT:
            case SLAVE_PERSISTENCE_TIMEOUT:
                throw new ProxyTimeoutException(codeNumber, requestId, statusMessage);
            case UNSUPPORTED:
            case VERSION_UNSUPPORTED:
            case VERIFY_FIFO_MESSAGE_UNSUPPORTED:
                throw new UnsupportedException(codeNumber, requestId, statusMessage);
            default:
                LOGGER.warn("Unrecognized status code={}, requestId={}, statusMessage={}, clientVersion={}",
                    codeNumber, requestId, statusMessage, MetadataUtils.getVersion());
                throw new UnsupportedException(codeNumber, requestId, statusMessage);
        }
    }
}
