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

import static org.junit.Assert.fail;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Status;
import io.grpc.Metadata;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.misc.RequestIdGenerator;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.Context;
import org.apache.rocketmq.client.java.rpc.RpcInvocation;
import org.apache.rocketmq.client.java.rpc.Signature;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Test;

public class StatusCheckerTest extends TestBase {
    private Context generateContext() {
        final Endpoints endpoints = fakeEndpoints();
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(Signature.REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER),
            RequestIdGenerator.getInstance().next());
        return new Context(endpoints, metadata);
    }

    @Test
    public void testOK() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.OK).build();
        Object response = new Object();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        StatusChecker.check(status, invocation);
    }

    @Test
    public void testMultipleResults() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.OK).build();
        Object response = new Object();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        StatusChecker.check(status, invocation);
    }

    @Test
    public void testBadRequest() throws ClientException {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.BAD_REQUEST).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_ACCESS_POINT).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_TOPIC).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_CONSUMER_GROUP).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_TAG).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_KEY).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_GROUP).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_PROPERTY_KEY).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.INVALID_TRANSACTION_ID).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_ID).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_MESSAGE_GROUP).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_FILTER_EXPRESSION).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_INVISIBLE_TIME).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.ILLEGAL_DELIVERY_TIME).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.INVALID_RECEIPT_HANDLE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.UNRECOGNIZED_CLIENT_TYPE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.MESSAGE_CORRUPTED).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.CLIENT_ID_REQUIRED).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (BadRequestException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testUnauthorized() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.UNAUTHORIZED).build();
        Object response = new Object();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        try {
            StatusChecker.check(status, invocation);
            fail();
        } catch (UnauthorizedException ignore) {
            // ignore on purpose
        }
    }

    @Test
    public void testPaymentRequired() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.PAYMENT_REQUIRED).build();
        Object response = new Object();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        try {
            StatusChecker.check(status, invocation);
            fail();
        } catch (PaymentRequiredException ignore) {
            // ignore on purpose
        }
    }

    @Test
    public void testForbidden() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.FORBIDDEN).build();
        Object response = new Object();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        try {
            StatusChecker.check(status, invocation);
            fail();
        } catch (ForbiddenException ignore) {
            // ignore on purpose
        }
    }

    @Test
    public void testMessageNotFoundDuringReceiving() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.MESSAGE_NOT_FOUND).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().build();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        StatusChecker.check(status, invocation);
    }

    @Test
    public void testNotFound() {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.MESSAGE_NOT_FOUND).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.NOT_FOUND).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.TOPIC_NOT_FOUND).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.CONSUMER_GROUP_NOT_FOUND).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testPayloadTooLarge() {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.PAYLOAD_TOO_LARGE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.MESSAGE_BODY_TOO_LARGE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ClientException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testTooManyRequests() throws ClientException {
        Status status = Status.newBuilder().setCode(Code.TOO_MANY_REQUESTS).build();
        ReceiveMessageResponse response = ReceiveMessageResponse.newBuilder().build();
        final Context context = generateContext();
        RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
        try {
            StatusChecker.check(status, invocation);
            fail();
        } catch (TooManyRequestsException ignore) {
            // ignore on purpose
        }
    }

    @Test
    public void testRequestHeaderFieldsTooLarge() throws ClientException {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.REQUEST_HEADER_FIELDS_TOO_LARGE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (RequestHeaderFieldsTooLargeException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.MESSAGE_PROPERTIES_TOO_LARGE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (RequestHeaderFieldsTooLargeException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testInternalError() throws ClientException {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.INTERNAL_ERROR).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (InternalErrorException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.INTERNAL_SERVER_ERROR).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (InternalErrorException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.HA_NOT_AVAILABLE).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (InternalErrorException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testProxyTimeout() throws ClientException {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.PROXY_TIMEOUT).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ProxyTimeoutException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.MASTER_PERSISTENCE_TIMEOUT).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ProxyTimeoutException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.SLAVE_PERSISTENCE_TIMEOUT).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (ProxyTimeoutException ignore) {
                // ignore on purpose
            }
        }
    }

    @Test
    public void testUnsupported() throws ClientException {
        Object response = new Object();
        final Context context = generateContext();

        {
            Status status = Status.newBuilder().setCode(Code.UNSUPPORTED).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (UnsupportedException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.VERSION_UNSUPPORTED).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (UnsupportedException ignore) {
                // ignore on purpose
            }
        }

        {
            Status status = Status.newBuilder().setCode(Code.VERIFY_FIFO_MESSAGE_UNSUPPORTED).build();
            RpcInvocation<Object> invocation = new RpcInvocation<>(response, context);
            try {
                StatusChecker.check(status, invocation);
                fail();
            } catch (UnsupportedException ignore) {
                // ignore on purpose
            }
        }
    }
}