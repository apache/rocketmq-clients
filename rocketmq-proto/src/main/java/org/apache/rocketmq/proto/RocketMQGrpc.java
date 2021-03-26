package org.apache.rocketmq.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.35.0)",
    comments = "Source: rocketmq.proto")
public final class RocketMQGrpc {

  private RocketMQGrpc() {}

  public static final String SERVICE_NAME = "rocketmq.rpc.api.RocketMQ";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.RouteInfoRequest,
      org.apache.rocketmq.proto.RouteInfoResponse> getFetchTopicRouteInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FetchTopicRouteInfo",
      requestType = org.apache.rocketmq.proto.RouteInfoRequest.class,
      responseType = org.apache.rocketmq.proto.RouteInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.RouteInfoRequest,
      org.apache.rocketmq.proto.RouteInfoResponse> getFetchTopicRouteInfoMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.RouteInfoRequest, org.apache.rocketmq.proto.RouteInfoResponse> getFetchTopicRouteInfoMethod;
    if ((getFetchTopicRouteInfoMethod = RocketMQGrpc.getFetchTopicRouteInfoMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getFetchTopicRouteInfoMethod = RocketMQGrpc.getFetchTopicRouteInfoMethod) == null) {
          RocketMQGrpc.getFetchTopicRouteInfoMethod = getFetchTopicRouteInfoMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.RouteInfoRequest, org.apache.rocketmq.proto.RouteInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FetchTopicRouteInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.RouteInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.RouteInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("FetchTopicRouteInfo"))
              .build();
        }
      }
    }
    return getFetchTopicRouteInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.SendMessageRequest,
      org.apache.rocketmq.proto.SendMessageResponse> getSendMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage",
      requestType = org.apache.rocketmq.proto.SendMessageRequest.class,
      responseType = org.apache.rocketmq.proto.SendMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.SendMessageRequest,
      org.apache.rocketmq.proto.SendMessageResponse> getSendMessageMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.SendMessageRequest, org.apache.rocketmq.proto.SendMessageResponse> getSendMessageMethod;
    if ((getSendMessageMethod = RocketMQGrpc.getSendMessageMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getSendMessageMethod = RocketMQGrpc.getSendMessageMethod) == null) {
          RocketMQGrpc.getSendMessageMethod = getSendMessageMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.SendMessageRequest, org.apache.rocketmq.proto.SendMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.SendMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.SendMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("SendMessage"))
              .build();
        }
      }
    }
    return getSendMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HeartbeatRequest,
      org.apache.rocketmq.proto.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = org.apache.rocketmq.proto.HeartbeatRequest.class,
      responseType = org.apache.rocketmq.proto.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HeartbeatRequest,
      org.apache.rocketmq.proto.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HeartbeatRequest, org.apache.rocketmq.proto.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = RocketMQGrpc.getHeartbeatMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getHeartbeatMethod = RocketMQGrpc.getHeartbeatMethod) == null) {
          RocketMQGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.HeartbeatRequest, org.apache.rocketmq.proto.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.QueryAssignmentRequest,
      org.apache.rocketmq.proto.QueryAssignmentResponse> getQueryAssignmentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryAssignment",
      requestType = org.apache.rocketmq.proto.QueryAssignmentRequest.class,
      responseType = org.apache.rocketmq.proto.QueryAssignmentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.QueryAssignmentRequest,
      org.apache.rocketmq.proto.QueryAssignmentResponse> getQueryAssignmentMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.QueryAssignmentRequest, org.apache.rocketmq.proto.QueryAssignmentResponse> getQueryAssignmentMethod;
    if ((getQueryAssignmentMethod = RocketMQGrpc.getQueryAssignmentMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getQueryAssignmentMethod = RocketMQGrpc.getQueryAssignmentMethod) == null) {
          RocketMQGrpc.getQueryAssignmentMethod = getQueryAssignmentMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.QueryAssignmentRequest, org.apache.rocketmq.proto.QueryAssignmentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryAssignment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.QueryAssignmentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.QueryAssignmentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("QueryAssignment"))
              .build();
        }
      }
    }
    return getQueryAssignmentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PopMessageRequest,
      org.apache.rocketmq.proto.PopMessageResponse> getPopMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PopMessage",
      requestType = org.apache.rocketmq.proto.PopMessageRequest.class,
      responseType = org.apache.rocketmq.proto.PopMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PopMessageRequest,
      org.apache.rocketmq.proto.PopMessageResponse> getPopMessageMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PopMessageRequest, org.apache.rocketmq.proto.PopMessageResponse> getPopMessageMethod;
    if ((getPopMessageMethod = RocketMQGrpc.getPopMessageMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getPopMessageMethod = RocketMQGrpc.getPopMessageMethod) == null) {
          RocketMQGrpc.getPopMessageMethod = getPopMessageMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.PopMessageRequest, org.apache.rocketmq.proto.PopMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PopMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.PopMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.PopMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("PopMessage"))
              .build();
        }
      }
    }
    return getPopMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.AckMessageRequest,
      org.apache.rocketmq.proto.AckMessageResponse> getAckMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckMessage",
      requestType = org.apache.rocketmq.proto.AckMessageRequest.class,
      responseType = org.apache.rocketmq.proto.AckMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.AckMessageRequest,
      org.apache.rocketmq.proto.AckMessageResponse> getAckMessageMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.AckMessageRequest, org.apache.rocketmq.proto.AckMessageResponse> getAckMessageMethod;
    if ((getAckMessageMethod = RocketMQGrpc.getAckMessageMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getAckMessageMethod = RocketMQGrpc.getAckMessageMethod) == null) {
          RocketMQGrpc.getAckMessageMethod = getAckMessageMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.AckMessageRequest, org.apache.rocketmq.proto.AckMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.AckMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.AckMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("AckMessage"))
              .build();
        }
      }
    }
    return getAckMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HealthCheckRequest,
      org.apache.rocketmq.proto.HealthCheckResponse> getHealthCheckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HealthCheck",
      requestType = org.apache.rocketmq.proto.HealthCheckRequest.class,
      responseType = org.apache.rocketmq.proto.HealthCheckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HealthCheckRequest,
      org.apache.rocketmq.proto.HealthCheckResponse> getHealthCheckMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.HealthCheckRequest, org.apache.rocketmq.proto.HealthCheckResponse> getHealthCheckMethod;
    if ((getHealthCheckMethod = RocketMQGrpc.getHealthCheckMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getHealthCheckMethod = RocketMQGrpc.getHealthCheckMethod) == null) {
          RocketMQGrpc.getHealthCheckMethod = getHealthCheckMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.HealthCheckRequest, org.apache.rocketmq.proto.HealthCheckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HealthCheck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.HealthCheckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.HealthCheckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("HealthCheck"))
              .build();
        }
      }
    }
    return getHealthCheckMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.ChangeInvisibleTimeRequest,
      org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> getChangeInvisibleTimeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeInvisibleTime",
      requestType = org.apache.rocketmq.proto.ChangeInvisibleTimeRequest.class,
      responseType = org.apache.rocketmq.proto.ChangeInvisibleTimeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.ChangeInvisibleTimeRequest,
      org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> getChangeInvisibleTimeMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.ChangeInvisibleTimeRequest, org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> getChangeInvisibleTimeMethod;
    if ((getChangeInvisibleTimeMethod = RocketMQGrpc.getChangeInvisibleTimeMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getChangeInvisibleTimeMethod = RocketMQGrpc.getChangeInvisibleTimeMethod) == null) {
          RocketMQGrpc.getChangeInvisibleTimeMethod = getChangeInvisibleTimeMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.ChangeInvisibleTimeRequest, org.apache.rocketmq.proto.ChangeInvisibleTimeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeInvisibleTime"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.ChangeInvisibleTimeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.ChangeInvisibleTimeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("ChangeInvisibleTime"))
              .build();
        }
      }
    }
    return getChangeInvisibleTimeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PullMessageRequest,
      org.apache.rocketmq.proto.PullMessageResponse> getPullMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PullMessage",
      requestType = org.apache.rocketmq.proto.PullMessageRequest.class,
      responseType = org.apache.rocketmq.proto.PullMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PullMessageRequest,
      org.apache.rocketmq.proto.PullMessageResponse> getPullMessageMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.PullMessageRequest, org.apache.rocketmq.proto.PullMessageResponse> getPullMessageMethod;
    if ((getPullMessageMethod = RocketMQGrpc.getPullMessageMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getPullMessageMethod = RocketMQGrpc.getPullMessageMethod) == null) {
          RocketMQGrpc.getPullMessageMethod = getPullMessageMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.PullMessageRequest, org.apache.rocketmq.proto.PullMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PullMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.PullMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.PullMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("PullMessage"))
              .build();
        }
      }
    }
    return getPullMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.rocketmq.proto.UpdateConsumerOffsetRequest,
      org.apache.rocketmq.proto.PullMessageResponse> getUpdateConsumerOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateConsumerOffset",
      requestType = org.apache.rocketmq.proto.UpdateConsumerOffsetRequest.class,
      responseType = org.apache.rocketmq.proto.PullMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.rocketmq.proto.UpdateConsumerOffsetRequest,
      org.apache.rocketmq.proto.PullMessageResponse> getUpdateConsumerOffsetMethod() {
    io.grpc.MethodDescriptor<org.apache.rocketmq.proto.UpdateConsumerOffsetRequest, org.apache.rocketmq.proto.PullMessageResponse> getUpdateConsumerOffsetMethod;
    if ((getUpdateConsumerOffsetMethod = RocketMQGrpc.getUpdateConsumerOffsetMethod) == null) {
      synchronized (RocketMQGrpc.class) {
        if ((getUpdateConsumerOffsetMethod = RocketMQGrpc.getUpdateConsumerOffsetMethod) == null) {
          RocketMQGrpc.getUpdateConsumerOffsetMethod = getUpdateConsumerOffsetMethod =
              io.grpc.MethodDescriptor.<org.apache.rocketmq.proto.UpdateConsumerOffsetRequest, org.apache.rocketmq.proto.PullMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateConsumerOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.UpdateConsumerOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.rocketmq.proto.PullMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RocketMQMethodDescriptorSupplier("UpdateConsumerOffset"))
              .build();
        }
      }
    }
    return getUpdateConsumerOffsetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RocketMQStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RocketMQStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RocketMQStub>() {
        @java.lang.Override
        public RocketMQStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RocketMQStub(channel, callOptions);
        }
      };
    return RocketMQStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RocketMQBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RocketMQBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RocketMQBlockingStub>() {
        @java.lang.Override
        public RocketMQBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RocketMQBlockingStub(channel, callOptions);
        }
      };
    return RocketMQBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RocketMQFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RocketMQFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RocketMQFutureStub>() {
        @java.lang.Override
        public RocketMQFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RocketMQFutureStub(channel, callOptions);
        }
      };
    return RocketMQFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class RocketMQImplBase implements io.grpc.BindableService {

    /**
     */
    public void fetchTopicRouteInfo(org.apache.rocketmq.proto.RouteInfoRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.RouteInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFetchTopicRouteInfoMethod(), responseObserver);
    }

    /**
     */
    public void sendMessage(org.apache.rocketmq.proto.SendMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.SendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessageMethod(), responseObserver);
    }

    /**
     */
    public void heartbeat(org.apache.rocketmq.proto.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    public void queryAssignment(org.apache.rocketmq.proto.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryAssignmentMethod(), responseObserver);
    }

    /**
     */
    public void popMessage(org.apache.rocketmq.proto.PopMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PopMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPopMessageMethod(), responseObserver);
    }

    /**
     */
    public void ackMessage(org.apache.rocketmq.proto.AckMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.AckMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMessageMethod(), responseObserver);
    }

    /**
     */
    public void healthCheck(org.apache.rocketmq.proto.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }

    /**
     */
    public void changeInvisibleTime(org.apache.rocketmq.proto.ChangeInvisibleTimeRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChangeInvisibleTimeMethod(), responseObserver);
    }

    /**
     */
    public void pullMessage(org.apache.rocketmq.proto.PullMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPullMessageMethod(), responseObserver);
    }

    /**
     */
    public void updateConsumerOffset(org.apache.rocketmq.proto.UpdateConsumerOffsetRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateConsumerOffsetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getFetchTopicRouteInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.RouteInfoRequest,
                org.apache.rocketmq.proto.RouteInfoResponse>(
                  this, METHODID_FETCH_TOPIC_ROUTE_INFO)))
          .addMethod(
            getSendMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.SendMessageRequest,
                org.apache.rocketmq.proto.SendMessageResponse>(
                  this, METHODID_SEND_MESSAGE)))
          .addMethod(
            getHeartbeatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.HeartbeatRequest,
                org.apache.rocketmq.proto.HeartbeatResponse>(
                  this, METHODID_HEARTBEAT)))
          .addMethod(
            getQueryAssignmentMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.QueryAssignmentRequest,
                org.apache.rocketmq.proto.QueryAssignmentResponse>(
                  this, METHODID_QUERY_ASSIGNMENT)))
          .addMethod(
            getPopMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.PopMessageRequest,
                org.apache.rocketmq.proto.PopMessageResponse>(
                  this, METHODID_POP_MESSAGE)))
          .addMethod(
            getAckMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.AckMessageRequest,
                org.apache.rocketmq.proto.AckMessageResponse>(
                  this, METHODID_ACK_MESSAGE)))
          .addMethod(
            getHealthCheckMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.HealthCheckRequest,
                org.apache.rocketmq.proto.HealthCheckResponse>(
                  this, METHODID_HEALTH_CHECK)))
          .addMethod(
            getChangeInvisibleTimeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.ChangeInvisibleTimeRequest,
                org.apache.rocketmq.proto.ChangeInvisibleTimeResponse>(
                  this, METHODID_CHANGE_INVISIBLE_TIME)))
          .addMethod(
            getPullMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.PullMessageRequest,
                org.apache.rocketmq.proto.PullMessageResponse>(
                  this, METHODID_PULL_MESSAGE)))
          .addMethod(
            getUpdateConsumerOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.rocketmq.proto.UpdateConsumerOffsetRequest,
                org.apache.rocketmq.proto.PullMessageResponse>(
                  this, METHODID_UPDATE_CONSUMER_OFFSET)))
          .build();
    }
  }

  /**
   */
  public static final class RocketMQStub extends io.grpc.stub.AbstractAsyncStub<RocketMQStub> {
    private RocketMQStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RocketMQStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RocketMQStub(channel, callOptions);
    }

    /**
     */
    public void fetchTopicRouteInfo(org.apache.rocketmq.proto.RouteInfoRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.RouteInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFetchTopicRouteInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendMessage(org.apache.rocketmq.proto.SendMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.SendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(org.apache.rocketmq.proto.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queryAssignment(org.apache.rocketmq.proto.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void popMessage(org.apache.rocketmq.proto.PopMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PopMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPopMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ackMessage(org.apache.rocketmq.proto.AckMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.AckMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void healthCheck(org.apache.rocketmq.proto.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void changeInvisibleTime(org.apache.rocketmq.proto.ChangeInvisibleTimeRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChangeInvisibleTimeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pullMessage(org.apache.rocketmq.proto.PullMessageRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPullMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateConsumerOffset(org.apache.rocketmq.proto.UpdateConsumerOffsetRequest request,
        io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateConsumerOffsetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RocketMQBlockingStub extends io.grpc.stub.AbstractBlockingStub<RocketMQBlockingStub> {
    private RocketMQBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RocketMQBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RocketMQBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.apache.rocketmq.proto.RouteInfoResponse fetchTopicRouteInfo(org.apache.rocketmq.proto.RouteInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFetchTopicRouteInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.SendMessageResponse sendMessage(org.apache.rocketmq.proto.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.HeartbeatResponse heartbeat(org.apache.rocketmq.proto.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.QueryAssignmentResponse queryAssignment(org.apache.rocketmq.proto.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryAssignmentMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.PopMessageResponse popMessage(org.apache.rocketmq.proto.PopMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPopMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.AckMessageResponse ackMessage(org.apache.rocketmq.proto.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.HealthCheckResponse healthCheck(org.apache.rocketmq.proto.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.ChangeInvisibleTimeResponse changeInvisibleTime(org.apache.rocketmq.proto.ChangeInvisibleTimeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChangeInvisibleTimeMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.PullMessageResponse pullMessage(org.apache.rocketmq.proto.PullMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPullMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.rocketmq.proto.PullMessageResponse updateConsumerOffset(org.apache.rocketmq.proto.UpdateConsumerOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateConsumerOffsetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RocketMQFutureStub extends io.grpc.stub.AbstractFutureStub<RocketMQFutureStub> {
    private RocketMQFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RocketMQFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RocketMQFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.RouteInfoResponse> fetchTopicRouteInfo(
        org.apache.rocketmq.proto.RouteInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFetchTopicRouteInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.SendMessageResponse> sendMessage(
        org.apache.rocketmq.proto.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.HeartbeatResponse> heartbeat(
        org.apache.rocketmq.proto.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.QueryAssignmentResponse> queryAssignment(
        org.apache.rocketmq.proto.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.PopMessageResponse> popMessage(
        org.apache.rocketmq.proto.PopMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPopMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.AckMessageResponse> ackMessage(
        org.apache.rocketmq.proto.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.HealthCheckResponse> healthCheck(
        org.apache.rocketmq.proto.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.ChangeInvisibleTimeResponse> changeInvisibleTime(
        org.apache.rocketmq.proto.ChangeInvisibleTimeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChangeInvisibleTimeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.PullMessageResponse> pullMessage(
        org.apache.rocketmq.proto.PullMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPullMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.rocketmq.proto.PullMessageResponse> updateConsumerOffset(
        org.apache.rocketmq.proto.UpdateConsumerOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateConsumerOffsetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_FETCH_TOPIC_ROUTE_INFO = 0;
  private static final int METHODID_SEND_MESSAGE = 1;
  private static final int METHODID_HEARTBEAT = 2;
  private static final int METHODID_QUERY_ASSIGNMENT = 3;
  private static final int METHODID_POP_MESSAGE = 4;
  private static final int METHODID_ACK_MESSAGE = 5;
  private static final int METHODID_HEALTH_CHECK = 6;
  private static final int METHODID_CHANGE_INVISIBLE_TIME = 7;
  private static final int METHODID_PULL_MESSAGE = 8;
  private static final int METHODID_UPDATE_CONSUMER_OFFSET = 9;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RocketMQImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RocketMQImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_FETCH_TOPIC_ROUTE_INFO:
          serviceImpl.fetchTopicRouteInfo((org.apache.rocketmq.proto.RouteInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.RouteInfoResponse>) responseObserver);
          break;
        case METHODID_SEND_MESSAGE:
          serviceImpl.sendMessage((org.apache.rocketmq.proto.SendMessageRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.SendMessageResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((org.apache.rocketmq.proto.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_QUERY_ASSIGNMENT:
          serviceImpl.queryAssignment((org.apache.rocketmq.proto.QueryAssignmentRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.QueryAssignmentResponse>) responseObserver);
          break;
        case METHODID_POP_MESSAGE:
          serviceImpl.popMessage((org.apache.rocketmq.proto.PopMessageRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PopMessageResponse>) responseObserver);
          break;
        case METHODID_ACK_MESSAGE:
          serviceImpl.ackMessage((org.apache.rocketmq.proto.AckMessageRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.AckMessageResponse>) responseObserver);
          break;
        case METHODID_HEALTH_CHECK:
          serviceImpl.healthCheck((org.apache.rocketmq.proto.HealthCheckRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.HealthCheckResponse>) responseObserver);
          break;
        case METHODID_CHANGE_INVISIBLE_TIME:
          serviceImpl.changeInvisibleTime((org.apache.rocketmq.proto.ChangeInvisibleTimeRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.ChangeInvisibleTimeResponse>) responseObserver);
          break;
        case METHODID_PULL_MESSAGE:
          serviceImpl.pullMessage((org.apache.rocketmq.proto.PullMessageRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CONSUMER_OFFSET:
          serviceImpl.updateConsumerOffset((org.apache.rocketmq.proto.UpdateConsumerOffsetRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.rocketmq.proto.PullMessageResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class RocketMQBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RocketMQBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.rocketmq.proto.ACS.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RocketMQ");
    }
  }

  private static final class RocketMQFileDescriptorSupplier
      extends RocketMQBaseDescriptorSupplier {
    RocketMQFileDescriptorSupplier() {}
  }

  private static final class RocketMQMethodDescriptorSupplier
      extends RocketMQBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RocketMQMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RocketMQGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RocketMQFileDescriptorSupplier())
              .addMethod(getFetchTopicRouteInfoMethod())
              .addMethod(getSendMessageMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getQueryAssignmentMethod())
              .addMethod(getPopMessageMethod())
              .addMethod(getAckMessageMethod())
              .addMethod(getHealthCheckMethod())
              .addMethod(getChangeInvisibleTimeMethod())
              .addMethod(getPullMessageMethod())
              .addMethod(getUpdateConsumerOffsetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
