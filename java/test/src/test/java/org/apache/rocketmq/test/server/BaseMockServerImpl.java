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

package org.apache.rocketmq.test.server;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.Duration;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.test.helper.ResponseWriter;

public class BaseMockServerImpl extends MockServer {
    public final String topic;
    public final String broker;
    public final boolean fifo;
    public final ResponseWriter responseWriter = ResponseWriter.getInstance();
    public final Status mockStatus = Status.newBuilder()
        .setCode(Code.OK)
        .setMessage("mock test")
        .build();

    public final Status messageNotFound = Status.newBuilder()
        .setCode(Code.MESSAGE_NOT_FOUND)
        .setMessage("mock test")
        .build();

    public BaseMockServerImpl(String topic) {
        this(topic, "broker", false);
    }

    public BaseMockServerImpl(String topic, String broker, boolean fifo) {
        super();
        this.topic = topic;
        this.broker = broker;
        this.fifo = fifo;
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        responseWriter.write(responseObserver, QueryRouteResponse.newBuilder()
            .setStatus(mockStatus)
            .addMessageQueues(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setName(topic).build())
                .setId(0)
                .setPermission(Permission.READ_WRITE)
                .setBroker(Broker.newBuilder()
                    .setName(broker)
                    .setId(0)
                    .setEndpoints(Endpoints.newBuilder()
                        .addAddresses(Address.newBuilder()
                            .setHost("127.0.0.1")
                            .setPort(getPort())
                            .build())
                        .build())
                    .build())
                .addAcceptMessageTypes(MessageType.NORMAL)
                .build())
            .build());
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        responseWriter.write(responseObserver, HeartbeatResponse.newBuilder().setStatus(mockStatus)
            .build());
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request,
        StreamObserver<QueryAssignmentResponse> responseObserver) {
        responseWriter.write(responseObserver, QueryAssignmentResponse.newBuilder().setStatus(mockStatus)
            .addAssignments(Assignment.newBuilder()
                .setMessageQueue(MessageQueue.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setName(topic).build())
                    .setId(0)
                    .setPermission(Permission.READ_WRITE)
                    .setBroker(Broker.newBuilder()
                        .setName(broker)
                        .setId(0)
                        .setEndpoints(Endpoints.newBuilder()
                            .addAddresses(Address.newBuilder()
                                .setHost("127.0.0.1")
                                .setPort(getPort())
                                .build())
                            .build())
                        .build())
                    .addAcceptMessageTypes(MessageType.NORMAL)
                    .build())
                .build())
            .build());
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                Subscription.Builder subscriptionBuilder = value.getSettings().getSubscription().toBuilder();
                subscriptionBuilder.setFifo(fifo);

                responseObserver.onNext(value.toBuilder().setStatus(mockStatus)
                    .setSettings(value.getSettings().toBuilder()
                        .setSubscription(subscriptionBuilder)
                        .setBackoffPolicy(value.getSettings().getBackoffPolicy().toBuilder()
                            .setMaxAttempts(16)
                            .setExponentialBackoff(ExponentialBackoff.newBuilder()
                                .setInitial(Duration.newBuilder()
                                    .setSeconds(1).build())
                                .setMax(Duration.newBuilder()
                                    .setSeconds(10).build())
                                .setMultiplier(1.5f)
                                .build()))).build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        responseObserver.onNext(AckMessageResponse.newBuilder().setStatus(mockStatus).build());
        responseObserver.onCompleted();
    }
}
