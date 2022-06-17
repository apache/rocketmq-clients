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

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getChangeLogLevelMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.ChangeLogLevelRequest,
                apache.rocketmq.v1.ChangeLogLevelResponse>(
                  this, METHODID_CHANGE_LOG_LEVEL)))
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
  }

  private static final int METHODID_CHANGE_LOG_LEVEL = 0;

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
      return apache.rocketmq.v1.MQAdmin.getDescriptor();
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
              .build();
        }
      }
    }
    return result;
  }
}
