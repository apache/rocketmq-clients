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

package org.apache.rocketmq.client.remoting;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.impl.Signature;

/**
 * Client auth interceptor for authentication, but only serve for message tracing actually.
 */
@Slf4j
public class AuthInterceptor implements ClientInterceptor {

    private final ClientConfig clientConfig;

    public AuthInterceptor(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    private void customMetadata(Metadata headers) {
        try {
            final Metadata metadata = Signature.sign(clientConfig);
            headers.merge(metadata);
        } catch (Throwable t) {
            log.error("Failed to sign headers", t);
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> listener, Metadata headers) {
                customMetadata(headers);
                super.start(listener, headers);
            }
        };
    }
}

