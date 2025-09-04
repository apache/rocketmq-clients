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

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client log interceptor based on grpc can track any remote procedure call that interacts with the client locally.
 */
public class LoggingInterceptor implements ClientInterceptor {
    private static final Logger log = LoggerFactory.getLogger(LoggingInterceptor.class);

    private static final LoggingInterceptor INSTANCE = new LoggingInterceptor();
    private String remoteAddr = "";

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public static LoggingInterceptor getInstance() {
        return INSTANCE;
    }

    @Override
    public <T, E> ClientCall<T, E> interceptCall(MethodDescriptor<T, E> method,
        CallOptions callOptions, Channel next) {

        final String rpcId = UUID.randomUUID().toString();
        final String authority = next.authority();
        final String serviceName = method.getServiceName();
        final String methodName = method.getBareMethodName();

        return new ForwardingClientCall.SimpleForwardingClientCall<T, E>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<E> responseListener, final Metadata headers) {
                log.trace("gRPC request header, rpcId={}, serviceName={}, methodName={}, authority={}, headers={}",
                    rpcId, serviceName, methodName, authority, headers);
                Listener<E> observabilityListener =
                    new ForwardingClientCallListener.SimpleForwardingClientCallListener<E>(responseListener) {
                        @Override
                        public void onMessage(E response) {
                            log.trace("gRPC response, rpcId={}, serviceName={}, methodName={}, content:\n{}",
                                rpcId, serviceName, methodName, response);
                            super.onMessage(response);
                        }

                        @Override
                        public void onHeaders(Metadata headers) {
                            log.trace("gRPC response header, rpcId={}, serviceName={}, methodName={}, "
                                + "authority={}, headers={}", rpcId, serviceName, methodName, authority, headers);
                            super.onHeaders(headers);
                        }

                        @Override
                        public void onReady() {
                            Attributes attributes = getAttributes();
                            Object address = attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                            String remoteAddrStr = address != null ? address.toString() : "";
                            remoteAddr = remoteAddrStr.startsWith("/") ? remoteAddrStr.substring(1) : remoteAddrStr;
                            super.onReady();
                        }

                    };
                super.start(observabilityListener, headers);
            }

            @Override
            public void sendMessage(T request) {
                log.trace("gRPC request, rpcId={}, serviceName={}, methodName={}, content:\n{}", rpcId,
                    serviceName, methodName, request);
                super.sendMessage(request);
            }
        };
    }
}
