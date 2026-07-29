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

package org.apache.rocketmq.client.java.rpc;

import com.google.common.util.concurrent.Futures;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.junit.Assert;
import org.junit.Test;

public class RpcFutureTest {
    private static final String REQUEST_ID = "fake-request-id";

    private Context createContext() {
        final Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(Signature.REQUEST_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), REQUEST_ID);
        return new Context(null, metadata);
    }

    @Test
    public void testResourceExhaustedIsNormalized() throws Exception {
        final StatusRuntimeException statusRuntimeException =
            Status.RESOURCE_EXHAUSTED.withDescription("flow controlled").asRuntimeException();
        final RpcFuture<Object, Object> future = new RpcFuture<>(createContext(), null,
            Futures.immediateFailedFuture(statusRuntimeException));

        try {
            future.get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TooManyRequestsException);
            Assert.assertTrue(e.getCause().getMessage().contains("response-code=42900"));
            Assert.assertTrue(e.getCause().getMessage().contains("request-id=" + REQUEST_ID));
            Assert.assertTrue(e.getCause().getMessage().contains("flow controlled"));
            Assert.assertSame(statusRuntimeException, e.getCause().getCause());
        }
    }

    @Test
    public void testOtherTransportExceptionIsUnchanged() throws Exception {
        final StatusRuntimeException statusRuntimeException = Status.UNAVAILABLE.asRuntimeException();
        final RpcFuture<Object, Object> future = new RpcFuture<>(createContext(), null,
            Futures.immediateFailedFuture(statusRuntimeException));

        try {
            future.get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertSame(statusRuntimeException, e.getCause());
        }
    }

    @Test
    public void testSuccessfulResponseIsUnchanged() throws Exception {
        final Object response = new Object();
        final RpcFuture<Object, Object> future =
            new RpcFuture<>(createContext(), null, Futures.immediateFuture(response));

        Assert.assertSame(response, future.get());
    }
}
