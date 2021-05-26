package apache.rocketmq.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.35.0)",
    comments = "Source: apache/rocketmq/v1/admin.proto")
public final class AdminGrpc {

  private AdminGrpc() {}

  public static final String SERVICE_NAME = "apache.rocketmq.v1.Admin";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.ChangeLogLevelRequest,
      apache.rocketmq.v1.ChangeLogLevelResponse> getChangeLogLevelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeLogLevel",
      requestType = apache.rocketmq.v1.ChangeLogLevelRequest.class,
      responseType = apache.rocketmq.v1.ChangeLogLevelResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.ChangeLogLevelRequest,
      apache.rocketmq.v1.ChangeLogLevelResponse> getChangeLogLevelMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.ChangeLogLevelRequest, apache.rocketmq.v1.ChangeLogLevelResponse> getChangeLogLevelMethod;
    if ((getChangeLogLevelMethod = AdminGrpc.getChangeLogLevelMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getChangeLogLevelMethod = AdminGrpc.getChangeLogLevelMethod) == null) {
          AdminGrpc.getChangeLogLevelMethod = getChangeLogLevelMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.ChangeLogLevelRequest, apache.rocketmq.v1.ChangeLogLevelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeLogLevel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ChangeLogLevelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ChangeLogLevelResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("ChangeLogLevel"))
              .build();
        }
      }
    }
    return getChangeLogLevelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.GetTopicRequest,
      apache.rocketmq.v1.Resource> getGetTopicMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTopic",
      requestType = apache.rocketmq.v1.GetTopicRequest.class,
      responseType = apache.rocketmq.v1.Resource.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.GetTopicRequest,
      apache.rocketmq.v1.Resource> getGetTopicMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.GetTopicRequest, apache.rocketmq.v1.Resource> getGetTopicMethod;
    if ((getGetTopicMethod = AdminGrpc.getGetTopicMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getGetTopicMethod = AdminGrpc.getGetTopicMethod) == null) {
          AdminGrpc.getGetTopicMethod = getGetTopicMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.GetTopicRequest, apache.rocketmq.v1.Resource>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTopic"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.GetTopicRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("GetTopic"))
              .build();
        }
      }
    }
    return getGetTopicMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource,
      apache.rocketmq.v1.Resource> getCreateTopicMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTopic",
      requestType = apache.rocketmq.v1.Resource.class,
      responseType = apache.rocketmq.v1.Resource.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource,
      apache.rocketmq.v1.Resource> getCreateTopicMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource> getCreateTopicMethod;
    if ((getCreateTopicMethod = AdminGrpc.getCreateTopicMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getCreateTopicMethod = AdminGrpc.getCreateTopicMethod) == null) {
          AdminGrpc.getCreateTopicMethod = getCreateTopicMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTopic"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("CreateTopic"))
              .build();
        }
      }
    }
    return getCreateTopicMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateTopicRequest,
      apache.rocketmq.v1.Resource> getUpdateTopicMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateTopic",
      requestType = apache.rocketmq.v1.UpdateTopicRequest.class,
      responseType = apache.rocketmq.v1.Resource.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateTopicRequest,
      apache.rocketmq.v1.Resource> getUpdateTopicMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateTopicRequest, apache.rocketmq.v1.Resource> getUpdateTopicMethod;
    if ((getUpdateTopicMethod = AdminGrpc.getUpdateTopicMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getUpdateTopicMethod = AdminGrpc.getUpdateTopicMethod) == null) {
          AdminGrpc.getUpdateTopicMethod = getUpdateTopicMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.UpdateTopicRequest, apache.rocketmq.v1.Resource>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateTopic"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.UpdateTopicRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("UpdateTopic"))
              .build();
        }
      }
    }
    return getUpdateTopicMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteTopicRequest,
      com.google.protobuf.Empty> getDeleteTopicMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteTopic",
      requestType = apache.rocketmq.v1.DeleteTopicRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteTopicRequest,
      com.google.protobuf.Empty> getDeleteTopicMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteTopicRequest, com.google.protobuf.Empty> getDeleteTopicMethod;
    if ((getDeleteTopicMethod = AdminGrpc.getDeleteTopicMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDeleteTopicMethod = AdminGrpc.getDeleteTopicMethod) == null) {
          AdminGrpc.getDeleteTopicMethod = getDeleteTopicMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.DeleteTopicRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteTopic"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.DeleteTopicRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DeleteTopic"))
              .build();
        }
      }
    }
    return getDeleteTopicMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource,
      apache.rocketmq.v1.Resource> getCreateConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateConsumerGroup",
      requestType = apache.rocketmq.v1.Resource.class,
      responseType = apache.rocketmq.v1.Resource.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource,
      apache.rocketmq.v1.Resource> getCreateConsumerGroupMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource> getCreateConsumerGroupMethod;
    if ((getCreateConsumerGroupMethod = AdminGrpc.getCreateConsumerGroupMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getCreateConsumerGroupMethod = AdminGrpc.getCreateConsumerGroupMethod) == null) {
          AdminGrpc.getCreateConsumerGroupMethod = getCreateConsumerGroupMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.Resource, apache.rocketmq.v1.Resource>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("CreateConsumerGroup"))
              .build();
        }
      }
    }
    return getCreateConsumerGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteConsumerGroupRequest,
      com.google.protobuf.Empty> getDeleteConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteConsumerGroup",
      requestType = apache.rocketmq.v1.DeleteConsumerGroupRequest.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteConsumerGroupRequest,
      com.google.protobuf.Empty> getDeleteConsumerGroupMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.DeleteConsumerGroupRequest, com.google.protobuf.Empty> getDeleteConsumerGroupMethod;
    if ((getDeleteConsumerGroupMethod = AdminGrpc.getDeleteConsumerGroupMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDeleteConsumerGroupMethod = AdminGrpc.getDeleteConsumerGroupMethod) == null) {
          AdminGrpc.getDeleteConsumerGroupMethod = getDeleteConsumerGroupMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.DeleteConsumerGroupRequest, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.DeleteConsumerGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DeleteConsumerGroup"))
              .build();
        }
      }
    }
    return getDeleteConsumerGroupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateConsumerGroupRequest,
      apache.rocketmq.v1.Resource> getUpdateConsumerGroupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateConsumerGroup",
      requestType = apache.rocketmq.v1.UpdateConsumerGroupRequest.class,
      responseType = apache.rocketmq.v1.Resource.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateConsumerGroupRequest,
      apache.rocketmq.v1.Resource> getUpdateConsumerGroupMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.UpdateConsumerGroupRequest, apache.rocketmq.v1.Resource> getUpdateConsumerGroupMethod;
    if ((getUpdateConsumerGroupMethod = AdminGrpc.getUpdateConsumerGroupMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getUpdateConsumerGroupMethod = AdminGrpc.getUpdateConsumerGroupMethod) == null) {
          AdminGrpc.getUpdateConsumerGroupMethod = getUpdateConsumerGroupMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.UpdateConsumerGroupRequest, apache.rocketmq.v1.Resource>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateConsumerGroup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.UpdateConsumerGroupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.Resource.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("UpdateConsumerGroup"))
              .build();
        }
      }
    }
    return getUpdateConsumerGroupMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AdminStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminStub>() {
        @java.lang.Override
        public AdminStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminStub(channel, callOptions);
        }
      };
    return AdminStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminBlockingStub>() {
        @java.lang.Override
        public AdminBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminBlockingStub(channel, callOptions);
        }
      };
    return AdminBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminFutureStub>() {
        @java.lang.Override
        public AdminFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminFutureStub(channel, callOptions);
        }
      };
    return AdminFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class AdminImplBase implements io.grpc.BindableService {

    /**
     */
    public void changeLogLevel(apache.rocketmq.v1.ChangeLogLevelRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ChangeLogLevelResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChangeLogLevelMethod(), responseObserver);
    }

    /**
     */
    public void getTopic(apache.rocketmq.v1.GetTopicRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTopicMethod(), responseObserver);
    }

    /**
     */
    public void createTopic(apache.rocketmq.v1.Resource request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTopicMethod(), responseObserver);
    }

    /**
     */
    public void updateTopic(apache.rocketmq.v1.UpdateTopicRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateTopicMethod(), responseObserver);
    }

    /**
     */
    public void deleteTopic(apache.rocketmq.v1.DeleteTopicRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteTopicMethod(), responseObserver);
    }

    /**
     */
    public void createConsumerGroup(apache.rocketmq.v1.Resource request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateConsumerGroupMethod(), responseObserver);
    }

    /**
     */
    public void deleteConsumerGroup(apache.rocketmq.v1.DeleteConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteConsumerGroupMethod(), responseObserver);
    }

    /**
     */
    public void updateConsumerGroup(apache.rocketmq.v1.UpdateConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateConsumerGroupMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getChangeLogLevelMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.ChangeLogLevelRequest,
                apache.rocketmq.v1.ChangeLogLevelResponse>(
                  this, METHODID_CHANGE_LOG_LEVEL)))
          .addMethod(
            getGetTopicMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.GetTopicRequest,
                apache.rocketmq.v1.Resource>(
                  this, METHODID_GET_TOPIC)))
          .addMethod(
            getCreateTopicMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.Resource,
                apache.rocketmq.v1.Resource>(
                  this, METHODID_CREATE_TOPIC)))
          .addMethod(
            getUpdateTopicMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.UpdateTopicRequest,
                apache.rocketmq.v1.Resource>(
                  this, METHODID_UPDATE_TOPIC)))
          .addMethod(
            getDeleteTopicMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.DeleteTopicRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_TOPIC)))
          .addMethod(
            getCreateConsumerGroupMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.Resource,
                apache.rocketmq.v1.Resource>(
                  this, METHODID_CREATE_CONSUMER_GROUP)))
          .addMethod(
            getDeleteConsumerGroupMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.DeleteConsumerGroupRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_DELETE_CONSUMER_GROUP)))
          .addMethod(
            getUpdateConsumerGroupMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.UpdateConsumerGroupRequest,
                apache.rocketmq.v1.Resource>(
                  this, METHODID_UPDATE_CONSUMER_GROUP)))
          .build();
    }
  }

  /**
   */
  public static final class AdminStub extends io.grpc.stub.AbstractAsyncStub<AdminStub> {
    private AdminStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminStub(channel, callOptions);
    }

    /**
     */
    public void changeLogLevel(apache.rocketmq.v1.ChangeLogLevelRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ChangeLogLevelResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChangeLogLevelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTopic(apache.rocketmq.v1.GetTopicRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTopicMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createTopic(apache.rocketmq.v1.Resource request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTopicMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateTopic(apache.rocketmq.v1.UpdateTopicRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateTopicMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteTopic(apache.rocketmq.v1.DeleteTopicRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteTopicMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createConsumerGroup(apache.rocketmq.v1.Resource request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteConsumerGroup(apache.rocketmq.v1.DeleteConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateConsumerGroup(apache.rocketmq.v1.UpdateConsumerGroupRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateConsumerGroupMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class AdminBlockingStub extends io.grpc.stub.AbstractBlockingStub<AdminBlockingStub> {
    private AdminBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminBlockingStub(channel, callOptions);
    }

    /**
     */
    public apache.rocketmq.v1.ChangeLogLevelResponse changeLogLevel(apache.rocketmq.v1.ChangeLogLevelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChangeLogLevelMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.Resource getTopic(apache.rocketmq.v1.GetTopicRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTopicMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.Resource createTopic(apache.rocketmq.v1.Resource request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTopicMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.Resource updateTopic(apache.rocketmq.v1.UpdateTopicRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateTopicMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty deleteTopic(apache.rocketmq.v1.DeleteTopicRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteTopicMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.Resource createConsumerGroup(apache.rocketmq.v1.Resource request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateConsumerGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.google.protobuf.Empty deleteConsumerGroup(apache.rocketmq.v1.DeleteConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteConsumerGroupMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.Resource updateConsumerGroup(apache.rocketmq.v1.UpdateConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateConsumerGroupMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class AdminFutureStub extends io.grpc.stub.AbstractFutureStub<AdminFutureStub> {
    private AdminFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.ChangeLogLevelResponse> changeLogLevel(
        apache.rocketmq.v1.ChangeLogLevelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChangeLogLevelMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.Resource> getTopic(
        apache.rocketmq.v1.GetTopicRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTopicMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.Resource> createTopic(
        apache.rocketmq.v1.Resource request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTopicMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.Resource> updateTopic(
        apache.rocketmq.v1.UpdateTopicRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateTopicMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteTopic(
        apache.rocketmq.v1.DeleteTopicRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteTopicMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.Resource> createConsumerGroup(
        apache.rocketmq.v1.Resource request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateConsumerGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> deleteConsumerGroup(
        apache.rocketmq.v1.DeleteConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteConsumerGroupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.Resource> updateConsumerGroup(
        apache.rocketmq.v1.UpdateConsumerGroupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateConsumerGroupMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CHANGE_LOG_LEVEL = 0;
  private static final int METHODID_GET_TOPIC = 1;
  private static final int METHODID_CREATE_TOPIC = 2;
  private static final int METHODID_UPDATE_TOPIC = 3;
  private static final int METHODID_DELETE_TOPIC = 4;
  private static final int METHODID_CREATE_CONSUMER_GROUP = 5;
  private static final int METHODID_DELETE_CONSUMER_GROUP = 6;
  private static final int METHODID_UPDATE_CONSUMER_GROUP = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AdminImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AdminImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CHANGE_LOG_LEVEL:
          serviceImpl.changeLogLevel((apache.rocketmq.v1.ChangeLogLevelRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.ChangeLogLevelResponse>) responseObserver);
          break;
        case METHODID_GET_TOPIC:
          serviceImpl.getTopic((apache.rocketmq.v1.GetTopicRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource>) responseObserver);
          break;
        case METHODID_CREATE_TOPIC:
          serviceImpl.createTopic((apache.rocketmq.v1.Resource) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource>) responseObserver);
          break;
        case METHODID_UPDATE_TOPIC:
          serviceImpl.updateTopic((apache.rocketmq.v1.UpdateTopicRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource>) responseObserver);
          break;
        case METHODID_DELETE_TOPIC:
          serviceImpl.deleteTopic((apache.rocketmq.v1.DeleteTopicRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CREATE_CONSUMER_GROUP:
          serviceImpl.createConsumerGroup((apache.rocketmq.v1.Resource) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource>) responseObserver);
          break;
        case METHODID_DELETE_CONSUMER_GROUP:
          serviceImpl.deleteConsumerGroup((apache.rocketmq.v1.DeleteConsumerGroupRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_UPDATE_CONSUMER_GROUP:
          serviceImpl.updateConsumerGroup((apache.rocketmq.v1.UpdateConsumerGroupRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.Resource>) responseObserver);
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

  private static abstract class AdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AdminBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return apache.rocketmq.v1.ACS.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Admin");
    }
  }

  private static final class AdminFileDescriptorSupplier
      extends AdminBaseDescriptorSupplier {
    AdminFileDescriptorSupplier() {}
  }

  private static final class AdminMethodDescriptorSupplier
      extends AdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AdminMethodDescriptorSupplier(String methodName) {
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
      synchronized (AdminGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AdminFileDescriptorSupplier())
              .addMethod(getChangeLogLevelMethod())
              .addMethod(getGetTopicMethod())
              .addMethod(getCreateTopicMethod())
              .addMethod(getUpdateTopicMethod())
              .addMethod(getDeleteTopicMethod())
              .addMethod(getCreateConsumerGroupMethod())
              .addMethod(getDeleteConsumerGroupMethod())
              .addMethod(getUpdateConsumerGroupMethod())
              .build();
        }
      }
    }
    return result;
  }
}
