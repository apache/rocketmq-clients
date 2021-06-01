package apache.rocketmq.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.35.0)",
    comments = "Source: apache/rocketmq/v1/service.proto")
public final class MessagingServiceGrpc {

  private MessagingServiceGrpc() {}

  public static final String SERVICE_NAME = "apache.rocketmq.v1.MessagingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest,
      apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryRoute",
      requestType = apache.rocketmq.v1.QueryRouteRequest.class,
      responseType = apache.rocketmq.v1.QueryRouteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest,
      apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryRouteRequest, apache.rocketmq.v1.QueryRouteResponse> getQueryRouteMethod;
    if ((getQueryRouteMethod = MessagingServiceGrpc.getQueryRouteMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getQueryRouteMethod = MessagingServiceGrpc.getQueryRouteMethod) == null) {
          MessagingServiceGrpc.getQueryRouteMethod = getQueryRouteMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.QueryRouteRequest, apache.rocketmq.v1.QueryRouteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryRoute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryRouteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryRouteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("QueryRoute"))
              .build();
        }
      }
    }
    return getQueryRouteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest,
      apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = apache.rocketmq.v1.HeartbeatRequest.class,
      responseType = apache.rocketmq.v1.HeartbeatResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest,
      apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.HeartbeatRequest, apache.rocketmq.v1.HeartbeatResponse> getHeartbeatMethod;
    if ((getHeartbeatMethod = MessagingServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getHeartbeatMethod = MessagingServiceGrpc.getHeartbeatMethod) == null) {
          MessagingServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.HeartbeatRequest, apache.rocketmq.v1.HeartbeatResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HeartbeatRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HeartbeatResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest,
      apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HealthCheck",
      requestType = apache.rocketmq.v1.HealthCheckRequest.class,
      responseType = apache.rocketmq.v1.HealthCheckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest,
      apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.HealthCheckRequest, apache.rocketmq.v1.HealthCheckResponse> getHealthCheckMethod;
    if ((getHealthCheckMethod = MessagingServiceGrpc.getHealthCheckMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getHealthCheckMethod = MessagingServiceGrpc.getHealthCheckMethod) == null) {
          MessagingServiceGrpc.getHealthCheckMethod = getHealthCheckMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.HealthCheckRequest, apache.rocketmq.v1.HealthCheckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HealthCheck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HealthCheckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.HealthCheckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("HealthCheck"))
              .build();
        }
      }
    }
    return getHealthCheckMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest,
      apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage",
      requestType = apache.rocketmq.v1.SendMessageRequest.class,
      responseType = apache.rocketmq.v1.SendMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest,
      apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.SendMessageRequest, apache.rocketmq.v1.SendMessageResponse> getSendMessageMethod;
    if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getSendMessageMethod = MessagingServiceGrpc.getSendMessageMethod) == null) {
          MessagingServiceGrpc.getSendMessageMethod = getSendMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.SendMessageRequest, apache.rocketmq.v1.SendMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SendMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SendMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("SendMessage"))
              .build();
        }
      }
    }
    return getSendMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest,
      apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryAssignment",
      requestType = apache.rocketmq.v1.QueryAssignmentRequest.class,
      responseType = apache.rocketmq.v1.QueryAssignmentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest,
      apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryAssignmentRequest, apache.rocketmq.v1.QueryAssignmentResponse> getQueryAssignmentMethod;
    if ((getQueryAssignmentMethod = MessagingServiceGrpc.getQueryAssignmentMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getQueryAssignmentMethod = MessagingServiceGrpc.getQueryAssignmentMethod) == null) {
          MessagingServiceGrpc.getQueryAssignmentMethod = getQueryAssignmentMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.QueryAssignmentRequest, apache.rocketmq.v1.QueryAssignmentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryAssignment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryAssignmentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryAssignmentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("QueryAssignment"))
              .build();
        }
      }
    }
    return getQueryAssignmentMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest,
      apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReceiveMessage",
      requestType = apache.rocketmq.v1.ReceiveMessageRequest.class,
      responseType = apache.rocketmq.v1.ReceiveMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest,
      apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.ReceiveMessageRequest, apache.rocketmq.v1.ReceiveMessageResponse> getReceiveMessageMethod;
    if ((getReceiveMessageMethod = MessagingServiceGrpc.getReceiveMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getReceiveMessageMethod = MessagingServiceGrpc.getReceiveMessageMethod) == null) {
          MessagingServiceGrpc.getReceiveMessageMethod = getReceiveMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.ReceiveMessageRequest, apache.rocketmq.v1.ReceiveMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReceiveMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ReceiveMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ReceiveMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("ReceiveMessage"))
              .build();
        }
      }
    }
    return getReceiveMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest,
      apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AckMessage",
      requestType = apache.rocketmq.v1.AckMessageRequest.class,
      responseType = apache.rocketmq.v1.AckMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest,
      apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.AckMessageRequest, apache.rocketmq.v1.AckMessageResponse> getAckMessageMethod;
    if ((getAckMessageMethod = MessagingServiceGrpc.getAckMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getAckMessageMethod = MessagingServiceGrpc.getAckMessageMethod) == null) {
          MessagingServiceGrpc.getAckMessageMethod = getAckMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.AckMessageRequest, apache.rocketmq.v1.AckMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AckMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.AckMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.AckMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("AckMessage"))
              .build();
        }
      }
    }
    return getAckMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest,
      apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NackMessage",
      requestType = apache.rocketmq.v1.NackMessageRequest.class,
      responseType = apache.rocketmq.v1.NackMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest,
      apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.NackMessageRequest, apache.rocketmq.v1.NackMessageResponse> getNackMessageMethod;
    if ((getNackMessageMethod = MessagingServiceGrpc.getNackMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getNackMessageMethod = MessagingServiceGrpc.getNackMessageMethod) == null) {
          MessagingServiceGrpc.getNackMessageMethod = getNackMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.NackMessageRequest, apache.rocketmq.v1.NackMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NackMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NackMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NackMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("NackMessage"))
              .build();
        }
      }
    }
    return getNackMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.EndTransactionRequest,
      apache.rocketmq.v1.EndTransactionResponse> getEndTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EndTransaction",
      requestType = apache.rocketmq.v1.EndTransactionRequest.class,
      responseType = apache.rocketmq.v1.EndTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.EndTransactionRequest,
      apache.rocketmq.v1.EndTransactionResponse> getEndTransactionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.EndTransactionRequest, apache.rocketmq.v1.EndTransactionResponse> getEndTransactionMethod;
    if ((getEndTransactionMethod = MessagingServiceGrpc.getEndTransactionMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getEndTransactionMethod = MessagingServiceGrpc.getEndTransactionMethod) == null) {
          MessagingServiceGrpc.getEndTransactionMethod = getEndTransactionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.EndTransactionRequest, apache.rocketmq.v1.EndTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EndTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.EndTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.EndTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("EndTransaction"))
              .build();
        }
      }
    }
    return getEndTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.PollOrphanTransactionRequest,
      apache.rocketmq.v1.PollOrphanTransactionResponse> getPollOrphanTransactionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PollOrphanTransaction",
      requestType = apache.rocketmq.v1.PollOrphanTransactionRequest.class,
      responseType = apache.rocketmq.v1.PollOrphanTransactionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.PollOrphanTransactionRequest,
      apache.rocketmq.v1.PollOrphanTransactionResponse> getPollOrphanTransactionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.PollOrphanTransactionRequest, apache.rocketmq.v1.PollOrphanTransactionResponse> getPollOrphanTransactionMethod;
    if ((getPollOrphanTransactionMethod = MessagingServiceGrpc.getPollOrphanTransactionMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getPollOrphanTransactionMethod = MessagingServiceGrpc.getPollOrphanTransactionMethod) == null) {
          MessagingServiceGrpc.getPollOrphanTransactionMethod = getPollOrphanTransactionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.PollOrphanTransactionRequest, apache.rocketmq.v1.PollOrphanTransactionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PollOrphanTransaction"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PollOrphanTransactionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PollOrphanTransactionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("PollOrphanTransaction"))
              .build();
        }
      }
    }
    return getPollOrphanTransactionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryOffsetRequest,
      apache.rocketmq.v1.QueryOffsetResponse> getQueryOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryOffset",
      requestType = apache.rocketmq.v1.QueryOffsetRequest.class,
      responseType = apache.rocketmq.v1.QueryOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryOffsetRequest,
      apache.rocketmq.v1.QueryOffsetResponse> getQueryOffsetMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.QueryOffsetRequest, apache.rocketmq.v1.QueryOffsetResponse> getQueryOffsetMethod;
    if ((getQueryOffsetMethod = MessagingServiceGrpc.getQueryOffsetMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getQueryOffsetMethod = MessagingServiceGrpc.getQueryOffsetMethod) == null) {
          MessagingServiceGrpc.getQueryOffsetMethod = getQueryOffsetMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.QueryOffsetRequest, apache.rocketmq.v1.QueryOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.QueryOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("QueryOffset"))
              .build();
        }
      }
    }
    return getQueryOffsetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.SeekOffsetRequest,
      apache.rocketmq.v1.SeekOffsetResponse> getSeekOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SeekOffset",
      requestType = apache.rocketmq.v1.SeekOffsetRequest.class,
      responseType = apache.rocketmq.v1.SeekOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.SeekOffsetRequest,
      apache.rocketmq.v1.SeekOffsetResponse> getSeekOffsetMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.SeekOffsetRequest, apache.rocketmq.v1.SeekOffsetResponse> getSeekOffsetMethod;
    if ((getSeekOffsetMethod = MessagingServiceGrpc.getSeekOffsetMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getSeekOffsetMethod = MessagingServiceGrpc.getSeekOffsetMethod) == null) {
          MessagingServiceGrpc.getSeekOffsetMethod = getSeekOffsetMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.SeekOffsetRequest, apache.rocketmq.v1.SeekOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SeekOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SeekOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.SeekOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("SeekOffset"))
              .build();
        }
      }
    }
    return getSeekOffsetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.PollMessageRequest,
      apache.rocketmq.v1.PollMessageResponse> getPollMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PollMessage",
      requestType = apache.rocketmq.v1.PollMessageRequest.class,
      responseType = apache.rocketmq.v1.PollMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.PollMessageRequest,
      apache.rocketmq.v1.PollMessageResponse> getPollMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.PollMessageRequest, apache.rocketmq.v1.PollMessageResponse> getPollMessageMethod;
    if ((getPollMessageMethod = MessagingServiceGrpc.getPollMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getPollMessageMethod = MessagingServiceGrpc.getPollMessageMethod) == null) {
          MessagingServiceGrpc.getPollMessageMethod = getPollMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.PollMessageRequest, apache.rocketmq.v1.PollMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PollMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PollMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PollMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("PollMessage"))
              .build();
        }
      }
    }
    return getPollMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.CommitOffsetRequest,
      apache.rocketmq.v1.CommitOffsetResponse> getCommitOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitOffset",
      requestType = apache.rocketmq.v1.CommitOffsetRequest.class,
      responseType = apache.rocketmq.v1.CommitOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.CommitOffsetRequest,
      apache.rocketmq.v1.CommitOffsetResponse> getCommitOffsetMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.CommitOffsetRequest, apache.rocketmq.v1.CommitOffsetResponse> getCommitOffsetMethod;
    if ((getCommitOffsetMethod = MessagingServiceGrpc.getCommitOffsetMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getCommitOffsetMethod = MessagingServiceGrpc.getCommitOffsetMethod) == null) {
          MessagingServiceGrpc.getCommitOffsetMethod = getCommitOffsetMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.CommitOffsetRequest, apache.rocketmq.v1.CommitOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CommitOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.CommitOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.CommitOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("CommitOffset"))
              .build();
        }
      }
    }
    return getCommitOffsetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MessagingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceStub>() {
        @java.lang.Override
        public MessagingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceStub(channel, callOptions);
        }
      };
    return MessagingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MessagingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceBlockingStub>() {
        @java.lang.Override
        public MessagingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceBlockingStub(channel, callOptions);
        }
      };
    return MessagingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MessagingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MessagingServiceFutureStub>() {
        @java.lang.Override
        public MessagingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MessagingServiceFutureStub(channel, callOptions);
        }
      };
    return MessagingServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class MessagingServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryRouteMethod(), responseObserver);
    }

    /**
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }

    /**
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessageMethod(), responseObserver);
    }

    /**
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryAssignmentMethod(), responseObserver);
    }

    /**
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReceiveMessageMethod(), responseObserver);
    }

    /**
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMessageMethod(), responseObserver);
    }

    /**
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNackMessageMethod(), responseObserver);
    }

    /**
     */
    public void endTransaction(apache.rocketmq.v1.EndTransactionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEndTransactionMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollOrphanTransactionRequest> pollOrphanTransaction(
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollOrphanTransactionResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getPollOrphanTransactionMethod(), responseObserver);
    }

    /**
     */
    public void queryOffset(apache.rocketmq.v1.QueryOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryOffsetMethod(), responseObserver);
    }

    /**
     */
    public void seekOffset(apache.rocketmq.v1.SeekOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SeekOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSeekOffsetMethod(), responseObserver);
    }

    /**
     */
    public void pollMessage(apache.rocketmq.v1.PollMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPollMessageMethod(), responseObserver);
    }

    /**
     */
    public void commitOffset(apache.rocketmq.v1.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitOffsetMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getQueryRouteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryRouteRequest,
                apache.rocketmq.v1.QueryRouteResponse>(
                  this, METHODID_QUERY_ROUTE)))
          .addMethod(
            getHeartbeatMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.HeartbeatRequest,
                apache.rocketmq.v1.HeartbeatResponse>(
                  this, METHODID_HEARTBEAT)))
          .addMethod(
            getHealthCheckMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.HealthCheckRequest,
                apache.rocketmq.v1.HealthCheckResponse>(
                  this, METHODID_HEALTH_CHECK)))
          .addMethod(
            getSendMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.SendMessageRequest,
                apache.rocketmq.v1.SendMessageResponse>(
                  this, METHODID_SEND_MESSAGE)))
          .addMethod(
            getQueryAssignmentMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryAssignmentRequest,
                apache.rocketmq.v1.QueryAssignmentResponse>(
                  this, METHODID_QUERY_ASSIGNMENT)))
          .addMethod(
            getReceiveMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.ReceiveMessageRequest,
                apache.rocketmq.v1.ReceiveMessageResponse>(
                  this, METHODID_RECEIVE_MESSAGE)))
          .addMethod(
            getAckMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.AckMessageRequest,
                apache.rocketmq.v1.AckMessageResponse>(
                  this, METHODID_ACK_MESSAGE)))
          .addMethod(
            getNackMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.NackMessageRequest,
                apache.rocketmq.v1.NackMessageResponse>(
                  this, METHODID_NACK_MESSAGE)))
          .addMethod(
            getEndTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.EndTransactionRequest,
                apache.rocketmq.v1.EndTransactionResponse>(
                  this, METHODID_END_TRANSACTION)))
          .addMethod(
            getPollOrphanTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                apache.rocketmq.v1.PollOrphanTransactionRequest,
                apache.rocketmq.v1.PollOrphanTransactionResponse>(
                  this, METHODID_POLL_ORPHAN_TRANSACTION)))
          .addMethod(
            getQueryOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryOffsetRequest,
                apache.rocketmq.v1.QueryOffsetResponse>(
                  this, METHODID_QUERY_OFFSET)))
          .addMethod(
            getSeekOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.SeekOffsetRequest,
                apache.rocketmq.v1.SeekOffsetResponse>(
                  this, METHODID_SEEK_OFFSET)))
          .addMethod(
            getPollMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.PollMessageRequest,
                apache.rocketmq.v1.PollMessageResponse>(
                  this, METHODID_POLL_MESSAGE)))
          .addMethod(
            getCommitOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.CommitOffsetRequest,
                apache.rocketmq.v1.CommitOffsetResponse>(
                  this, METHODID_COMMIT_OFFSET)))
          .build();
    }
  }

  /**
   */
  public static final class MessagingServiceStub extends io.grpc.stub.AbstractAsyncStub<MessagingServiceStub> {
    private MessagingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceStub(channel, callOptions);
    }

    /**
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void endTransaction(apache.rocketmq.v1.EndTransactionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEndTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollOrphanTransactionRequest> pollOrphanTransaction(
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollOrphanTransactionResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getPollOrphanTransactionMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void queryOffset(apache.rocketmq.v1.QueryOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void seekOffset(apache.rocketmq.v1.SeekOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SeekOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSeekOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pollMessage(apache.rocketmq.v1.PollMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPollMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commitOffset(apache.rocketmq.v1.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class MessagingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MessagingServiceBlockingStub> {
    private MessagingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public apache.rocketmq.v1.QueryRouteResponse queryRoute(apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryRouteMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.HeartbeatResponse heartbeat(apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.HealthCheckResponse healthCheck(apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.SendMessageResponse sendMessage(apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.QueryAssignmentResponse queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryAssignmentMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.ReceiveMessageResponse receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReceiveMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.AckMessageResponse ackMessage(apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.NackMessageResponse nackMessage(apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNackMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.EndTransactionResponse endTransaction(apache.rocketmq.v1.EndTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEndTransactionMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.QueryOffsetResponse queryOffset(apache.rocketmq.v1.QueryOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryOffsetMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.SeekOffsetResponse seekOffset(apache.rocketmq.v1.SeekOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSeekOffsetMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.PollMessageResponse pollMessage(apache.rocketmq.v1.PollMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPollMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.CommitOffsetResponse commitOffset(apache.rocketmq.v1.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitOffsetMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class MessagingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MessagingServiceFutureStub> {
    private MessagingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MessagingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MessagingServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryRouteResponse> queryRoute(
        apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HeartbeatResponse> heartbeat(
        apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HealthCheckResponse> healthCheck(
        apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.SendMessageResponse> sendMessage(
        apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryAssignmentResponse> queryAssignment(
        apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.ReceiveMessageResponse> receiveMessage(
        apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.AckMessageResponse> ackMessage(
        apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.NackMessageResponse> nackMessage(
        apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.EndTransactionResponse> endTransaction(
        apache.rocketmq.v1.EndTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEndTransactionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryOffsetResponse> queryOffset(
        apache.rocketmq.v1.QueryOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryOffsetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.SeekOffsetResponse> seekOffset(
        apache.rocketmq.v1.SeekOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSeekOffsetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.PollMessageResponse> pollMessage(
        apache.rocketmq.v1.PollMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPollMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.CommitOffsetResponse> commitOffset(
        apache.rocketmq.v1.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_QUERY_ROUTE = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_HEALTH_CHECK = 2;
  private static final int METHODID_SEND_MESSAGE = 3;
  private static final int METHODID_QUERY_ASSIGNMENT = 4;
  private static final int METHODID_RECEIVE_MESSAGE = 5;
  private static final int METHODID_ACK_MESSAGE = 6;
  private static final int METHODID_NACK_MESSAGE = 7;
  private static final int METHODID_END_TRANSACTION = 8;
  private static final int METHODID_QUERY_OFFSET = 9;
  private static final int METHODID_SEEK_OFFSET = 10;
  private static final int METHODID_POLL_MESSAGE = 11;
  private static final int METHODID_COMMIT_OFFSET = 12;
  private static final int METHODID_POLL_ORPHAN_TRANSACTION = 13;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MessagingServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MessagingServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_QUERY_ROUTE:
          serviceImpl.queryRoute((apache.rocketmq.v1.QueryRouteRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((apache.rocketmq.v1.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_HEALTH_CHECK:
          serviceImpl.healthCheck((apache.rocketmq.v1.HealthCheckRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse>) responseObserver);
          break;
        case METHODID_SEND_MESSAGE:
          serviceImpl.sendMessage((apache.rocketmq.v1.SendMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse>) responseObserver);
          break;
        case METHODID_QUERY_ASSIGNMENT:
          serviceImpl.queryAssignment((apache.rocketmq.v1.QueryAssignmentRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse>) responseObserver);
          break;
        case METHODID_RECEIVE_MESSAGE:
          serviceImpl.receiveMessage((apache.rocketmq.v1.ReceiveMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse>) responseObserver);
          break;
        case METHODID_ACK_MESSAGE:
          serviceImpl.ackMessage((apache.rocketmq.v1.AckMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse>) responseObserver);
          break;
        case METHODID_NACK_MESSAGE:
          serviceImpl.nackMessage((apache.rocketmq.v1.NackMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse>) responseObserver);
          break;
        case METHODID_END_TRANSACTION:
          serviceImpl.endTransaction((apache.rocketmq.v1.EndTransactionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse>) responseObserver);
          break;
        case METHODID_QUERY_OFFSET:
          serviceImpl.queryOffset((apache.rocketmq.v1.QueryOffsetRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse>) responseObserver);
          break;
        case METHODID_SEEK_OFFSET:
          serviceImpl.seekOffset((apache.rocketmq.v1.SeekOffsetRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.SeekOffsetResponse>) responseObserver);
          break;
        case METHODID_POLL_MESSAGE:
          serviceImpl.pollMessage((apache.rocketmq.v1.PollMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollMessageResponse>) responseObserver);
          break;
        case METHODID_COMMIT_OFFSET:
          serviceImpl.commitOffset((apache.rocketmq.v1.CommitOffsetRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.CommitOffsetResponse>) responseObserver);
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
        case METHODID_POLL_ORPHAN_TRANSACTION:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.pollOrphanTransaction(
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.PollOrphanTransactionResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MessagingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return apache.rocketmq.v1.MQService.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MessagingService");
    }
  }

  private static final class MessagingServiceFileDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier {
    MessagingServiceFileDescriptorSupplier() {}
  }

  private static final class MessagingServiceMethodDescriptorSupplier
      extends MessagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MessagingServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (MessagingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MessagingServiceFileDescriptorSupplier())
              .addMethod(getQueryRouteMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getHealthCheckMethod())
              .addMethod(getSendMessageMethod())
              .addMethod(getQueryAssignmentMethod())
              .addMethod(getReceiveMessageMethod())
              .addMethod(getAckMessageMethod())
              .addMethod(getNackMessageMethod())
              .addMethod(getEndTransactionMethod())
              .addMethod(getPollOrphanTransactionMethod())
              .addMethod(getQueryOffsetMethod())
              .addMethod(getSeekOffsetMethod())
              .addMethod(getPollMessageMethod())
              .addMethod(getCommitOffsetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
