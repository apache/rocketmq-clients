package apache.rocketmq.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * For all the rpcs in MessagingService may return below erros:
 * If the request doesn't have a valid authentication credentials, returns `UNAUTHENTICATED`.
 * If the caller doesn't permission to execute the specified operation, returns `PERMISSION_DENIED`.
 * If the per-user rate quota has been exceeded, returns `RESOURCE_EXHAUSTED`.
 * If any unexpected server-side exception occurs, returns `INTERNAL`.
 * </pre>
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

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest,
      apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> getForwardMessageToDeadLetterQueueMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ForwardMessageToDeadLetterQueue",
      requestType = apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest.class,
      responseType = apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest,
      apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> getForwardMessageToDeadLetterQueueMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest, apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> getForwardMessageToDeadLetterQueueMethod;
    if ((getForwardMessageToDeadLetterQueueMethod = MessagingServiceGrpc.getForwardMessageToDeadLetterQueueMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getForwardMessageToDeadLetterQueueMethod = MessagingServiceGrpc.getForwardMessageToDeadLetterQueueMethod) == null) {
          MessagingServiceGrpc.getForwardMessageToDeadLetterQueueMethod = getForwardMessageToDeadLetterQueueMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest, apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ForwardMessageToDeadLetterQueue"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("ForwardMessageToDeadLetterQueue"))
              .build();
        }
      }
    }
    return getForwardMessageToDeadLetterQueueMethod;
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

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.PullMessageRequest,
      apache.rocketmq.v1.PullMessageResponse> getPullMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PullMessage",
      requestType = apache.rocketmq.v1.PullMessageRequest.class,
      responseType = apache.rocketmq.v1.PullMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.PullMessageRequest,
      apache.rocketmq.v1.PullMessageResponse> getPullMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.PullMessageRequest, apache.rocketmq.v1.PullMessageResponse> getPullMessageMethod;
    if ((getPullMessageMethod = MessagingServiceGrpc.getPullMessageMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getPullMessageMethod = MessagingServiceGrpc.getPullMessageMethod) == null) {
          MessagingServiceGrpc.getPullMessageMethod = getPullMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.PullMessageRequest, apache.rocketmq.v1.PullMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PullMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PullMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.PullMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("PullMessage"))
              .build();
        }
      }
    }
    return getPullMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.MultiplexingRequest,
      apache.rocketmq.v1.MultiplexingResponse> getMultiplexingCallMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MultiplexingCall",
      requestType = apache.rocketmq.v1.MultiplexingRequest.class,
      responseType = apache.rocketmq.v1.MultiplexingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.MultiplexingRequest,
      apache.rocketmq.v1.MultiplexingResponse> getMultiplexingCallMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.MultiplexingRequest, apache.rocketmq.v1.MultiplexingResponse> getMultiplexingCallMethod;
    if ((getMultiplexingCallMethod = MessagingServiceGrpc.getMultiplexingCallMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getMultiplexingCallMethod = MessagingServiceGrpc.getMultiplexingCallMethod) == null) {
          MessagingServiceGrpc.getMultiplexingCallMethod = getMultiplexingCallMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.MultiplexingRequest, apache.rocketmq.v1.MultiplexingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MultiplexingCall"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.MultiplexingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.MultiplexingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("MultiplexingCall"))
              .build();
        }
      }
    }
    return getMultiplexingCallMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v1.NotifyClientTerminationRequest,
      apache.rocketmq.v1.NotifyClientTerminationResponse> getNotifyClientTerminationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NotifyClientTermination",
      requestType = apache.rocketmq.v1.NotifyClientTerminationRequest.class,
      responseType = apache.rocketmq.v1.NotifyClientTerminationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v1.NotifyClientTerminationRequest,
      apache.rocketmq.v1.NotifyClientTerminationResponse> getNotifyClientTerminationMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v1.NotifyClientTerminationRequest, apache.rocketmq.v1.NotifyClientTerminationResponse> getNotifyClientTerminationMethod;
    if ((getNotifyClientTerminationMethod = MessagingServiceGrpc.getNotifyClientTerminationMethod) == null) {
      synchronized (MessagingServiceGrpc.class) {
        if ((getNotifyClientTerminationMethod = MessagingServiceGrpc.getNotifyClientTerminationMethod) == null) {
          MessagingServiceGrpc.getNotifyClientTerminationMethod = getNotifyClientTerminationMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v1.NotifyClientTerminationRequest, apache.rocketmq.v1.NotifyClientTerminationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NotifyClientTermination"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NotifyClientTerminationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v1.NotifyClientTerminationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MessagingServiceMethodDescriptorSupplier("NotifyClientTermination"))
              .build();
        }
      }
    }
    return getNotifyClientTerminationMethod;
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
   * <pre>
   * For all the rpcs in MessagingService may return below erros:
   * If the request doesn't have a valid authentication credentials, returns `UNAUTHENTICATED`.
   * If the caller doesn't permission to execute the specified operation, returns `PERMISSION_DENIED`.
   * If the per-user rate quota has been exceeded, returns `RESOURCE_EXHAUSTED`.
   * If any unexpected server-side exception occurs, returns `INTERNAL`.
   * </pre>
   */
  public static abstract class MessagingServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Querys the route info of a topic from specific endpoints, the server returns a set of partition if success.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryRouteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Producer or consumer sends HeartbeatRequest to server in order to report necessary
     * client-side information, like subscription data of consumer. Returns `OK` if success.
     * If the client language info is invalid, returns `INVALID_ARGUMENT`
     * </pre>
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     * <pre>
     * Checks the health status of message server, returns `OK` if no network issues.
     * Clients could use this RPC to detect the availability of server, and adpot necessary isolation measures. 
     * </pre>
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }

    /**
     * <pre>
     * Sends one message to the specific partition of a topic, returns message id or transaction id with status `OK`.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Querys the assigned partition route info of a topic for current consumer, 
     * the returned assignment result is descided by server-side load balacner.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryAssignmentMethod(), responseObserver);
    }

    /**
     * <pre>
     * Receives messages from the server in batch manner, returns a set of messages if success.
     * The received messages should be acked or uacked after processed.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReceiveMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Acknowledges the message associated with the `receipt_handle` or `offset` in the
     * `AckMessageRequest`, it means the message has been successfully processed.
     * Returns `OK` if the message server remove the relevant message successfully.
     * If the given receipt_handle is illegal or out of date, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAckMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Signals that the message has not been successfully processed. The message server should resend the message
     * follow the retry policy defined at server-side.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNackMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Forwards one message to dead letter queue if the DeadLetterPolicy is triggered by this message at client-side,
     * return `OK` if success.
     * </pre>
     */
    public void forwardMessageToDeadLetterQueue(apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getForwardMessageToDeadLetterQueueMethod(), responseObserver);
    }

    /**
     * <pre>
     * Commits or rollback one transactional message.
     * </pre>
     */
    public void endTransaction(apache.rocketmq.v1.EndTransactionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEndTransactionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Querys the offset of the specific partition, returns the offset with `OK` if success.
     * The message server should maintain a numerical offset for each message in a parition.
     * </pre>
     */
    public void queryOffset(apache.rocketmq.v1.QueryOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryOffsetMethod(), responseObserver);
    }

    /**
     * <pre>
     * Pulls messages from the specific partition, returns a set of messages with next pull offset.
     * The pulled messages can't be acked or nacked, while the client is responsible for manage offesets for consumer,
     * typically update consume offset to local memory or a third-party storage service.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public void pullMessage(apache.rocketmq.v1.PullMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PullMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPullMessageMethod(), responseObserver);
    }

    /**
     */
    public void multiplexingCall(apache.rocketmq.v1.MultiplexingRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.MultiplexingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMultiplexingCallMethod(), responseObserver);
    }

    /**
     */
    public void notifyClientTermination(apache.rocketmq.v1.NotifyClientTerminationRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NotifyClientTerminationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNotifyClientTerminationMethod(), responseObserver);
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
            getForwardMessageToDeadLetterQueueMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest,
                apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse>(
                  this, METHODID_FORWARD_MESSAGE_TO_DEAD_LETTER_QUEUE)))
          .addMethod(
            getEndTransactionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.EndTransactionRequest,
                apache.rocketmq.v1.EndTransactionResponse>(
                  this, METHODID_END_TRANSACTION)))
          .addMethod(
            getQueryOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.QueryOffsetRequest,
                apache.rocketmq.v1.QueryOffsetResponse>(
                  this, METHODID_QUERY_OFFSET)))
          .addMethod(
            getPullMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.PullMessageRequest,
                apache.rocketmq.v1.PullMessageResponse>(
                  this, METHODID_PULL_MESSAGE)))
          .addMethod(
            getMultiplexingCallMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.MultiplexingRequest,
                apache.rocketmq.v1.MultiplexingResponse>(
                  this, METHODID_MULTIPLEXING_CALL)))
          .addMethod(
            getNotifyClientTerminationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v1.NotifyClientTerminationRequest,
                apache.rocketmq.v1.NotifyClientTerminationResponse>(
                  this, METHODID_NOTIFY_CLIENT_TERMINATION)))
          .build();
    }
  }

  /**
   * <pre>
   * For all the rpcs in MessagingService may return below erros:
   * If the request doesn't have a valid authentication credentials, returns `UNAUTHENTICATED`.
   * If the caller doesn't permission to execute the specified operation, returns `PERMISSION_DENIED`.
   * If the per-user rate quota has been exceeded, returns `RESOURCE_EXHAUSTED`.
   * If any unexpected server-side exception occurs, returns `INTERNAL`.
   * </pre>
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
     * <pre>
     * Querys the route info of a topic from specific endpoints, the server returns a set of partition if success.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void queryRoute(apache.rocketmq.v1.QueryRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryRouteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Producer or consumer sends HeartbeatRequest to server in order to report necessary
     * client-side information, like subscription data of consumer. Returns `OK` if success.
     * If the client language info is invalid, returns `INVALID_ARGUMENT`
     * </pre>
     */
    public void heartbeat(apache.rocketmq.v1.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HeartbeatResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Checks the health status of message server, returns `OK` if no network issues.
     * Clients could use this RPC to detect the availability of server, and adpot necessary isolation measures. 
     * </pre>
     */
    public void healthCheck(apache.rocketmq.v1.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Sends one message to the specific partition of a topic, returns message id or transaction id with status `OK`.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public void sendMessage(apache.rocketmq.v1.SendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.SendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Querys the assigned partition route info of a topic for current consumer, 
     * the returned assignment result is descided by server-side load balacner.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryAssignmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Receives messages from the server in batch manner, returns a set of messages if success.
     * The received messages should be acked or uacked after processed.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public void receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ReceiveMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Acknowledges the message associated with the `receipt_handle` or `offset` in the
     * `AckMessageRequest`, it means the message has been successfully processed.
     * Returns `OK` if the message server remove the relevant message successfully.
     * If the given receipt_handle is illegal or out of date, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public void ackMessage(apache.rocketmq.v1.AckMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.AckMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Signals that the message has not been successfully processed. The message server should resend the message
     * follow the retry policy defined at server-side.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public void nackMessage(apache.rocketmq.v1.NackMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NackMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Forwards one message to dead letter queue if the DeadLetterPolicy is triggered by this message at client-side,
     * return `OK` if success.
     * </pre>
     */
    public void forwardMessageToDeadLetterQueue(apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getForwardMessageToDeadLetterQueueMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Commits or rollback one transactional message.
     * </pre>
     */
    public void endTransaction(apache.rocketmq.v1.EndTransactionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEndTransactionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Querys the offset of the specific partition, returns the offset with `OK` if success.
     * The message server should maintain a numerical offset for each message in a parition.
     * </pre>
     */
    public void queryOffset(apache.rocketmq.v1.QueryOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Pulls messages from the specific partition, returns a set of messages with next pull offset.
     * The pulled messages can't be acked or nacked, while the client is responsible for manage offesets for consumer,
     * typically update consume offset to local memory or a third-party storage service.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public void pullMessage(apache.rocketmq.v1.PullMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.PullMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPullMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void multiplexingCall(apache.rocketmq.v1.MultiplexingRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.MultiplexingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMultiplexingCallMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void notifyClientTermination(apache.rocketmq.v1.NotifyClientTerminationRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v1.NotifyClientTerminationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNotifyClientTerminationMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * For all the rpcs in MessagingService may return below erros:
   * If the request doesn't have a valid authentication credentials, returns `UNAUTHENTICATED`.
   * If the caller doesn't permission to execute the specified operation, returns `PERMISSION_DENIED`.
   * If the per-user rate quota has been exceeded, returns `RESOURCE_EXHAUSTED`.
   * If any unexpected server-side exception occurs, returns `INTERNAL`.
   * </pre>
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
     * <pre>
     * Querys the route info of a topic from specific endpoints, the server returns a set of partition if success.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public apache.rocketmq.v1.QueryRouteResponse queryRoute(apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryRouteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Producer or consumer sends HeartbeatRequest to server in order to report necessary
     * client-side information, like subscription data of consumer. Returns `OK` if success.
     * If the client language info is invalid, returns `INVALID_ARGUMENT`
     * </pre>
     */
    public apache.rocketmq.v1.HeartbeatResponse heartbeat(apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Checks the health status of message server, returns `OK` if no network issues.
     * Clients could use this RPC to detect the availability of server, and adpot necessary isolation measures. 
     * </pre>
     */
    public apache.rocketmq.v1.HealthCheckResponse healthCheck(apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Sends one message to the specific partition of a topic, returns message id or transaction id with status `OK`.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public apache.rocketmq.v1.SendMessageResponse sendMessage(apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Querys the assigned partition route info of a topic for current consumer, 
     * the returned assignment result is descided by server-side load balacner.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public apache.rocketmq.v1.QueryAssignmentResponse queryAssignment(apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryAssignmentMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Receives messages from the server in batch manner, returns a set of messages if success.
     * The received messages should be acked or uacked after processed.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public apache.rocketmq.v1.ReceiveMessageResponse receiveMessage(apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReceiveMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Acknowledges the message associated with the `receipt_handle` or `offset` in the
     * `AckMessageRequest`, it means the message has been successfully processed.
     * Returns `OK` if the message server remove the relevant message successfully.
     * If the given receipt_handle is illegal or out of date, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public apache.rocketmq.v1.AckMessageResponse ackMessage(apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAckMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Signals that the message has not been successfully processed. The message server should resend the message
     * follow the retry policy defined at server-side.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public apache.rocketmq.v1.NackMessageResponse nackMessage(apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNackMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Forwards one message to dead letter queue if the DeadLetterPolicy is triggered by this message at client-side,
     * return `OK` if success.
     * </pre>
     */
    public apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse forwardMessageToDeadLetterQueue(apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getForwardMessageToDeadLetterQueueMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Commits or rollback one transactional message.
     * </pre>
     */
    public apache.rocketmq.v1.EndTransactionResponse endTransaction(apache.rocketmq.v1.EndTransactionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEndTransactionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Querys the offset of the specific partition, returns the offset with `OK` if success.
     * The message server should maintain a numerical offset for each message in a parition.
     * </pre>
     */
    public apache.rocketmq.v1.QueryOffsetResponse queryOffset(apache.rocketmq.v1.QueryOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryOffsetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Pulls messages from the specific partition, returns a set of messages with next pull offset.
     * The pulled messages can't be acked or nacked, while the client is responsible for manage offesets for consumer,
     * typically update consume offset to local memory or a third-party storage service.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public apache.rocketmq.v1.PullMessageResponse pullMessage(apache.rocketmq.v1.PullMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPullMessageMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.MultiplexingResponse multiplexingCall(apache.rocketmq.v1.MultiplexingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMultiplexingCallMethod(), getCallOptions(), request);
    }

    /**
     */
    public apache.rocketmq.v1.NotifyClientTerminationResponse notifyClientTermination(apache.rocketmq.v1.NotifyClientTerminationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNotifyClientTerminationMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * For all the rpcs in MessagingService may return below erros:
   * If the request doesn't have a valid authentication credentials, returns `UNAUTHENTICATED`.
   * If the caller doesn't permission to execute the specified operation, returns `PERMISSION_DENIED`.
   * If the per-user rate quota has been exceeded, returns `RESOURCE_EXHAUSTED`.
   * If any unexpected server-side exception occurs, returns `INTERNAL`.
   * </pre>
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
     * <pre>
     * Querys the route info of a topic from specific endpoints, the server returns a set of partition if success.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryRouteResponse> queryRoute(
        apache.rocketmq.v1.QueryRouteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryRouteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Producer or consumer sends HeartbeatRequest to server in order to report necessary
     * client-side information, like subscription data of consumer. Returns `OK` if success.
     * If the client language info is invalid, returns `INVALID_ARGUMENT`
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HeartbeatResponse> heartbeat(
        apache.rocketmq.v1.HeartbeatRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Checks the health status of message server, returns `OK` if no network issues.
     * Clients could use this RPC to detect the availability of server, and adpot necessary isolation measures. 
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.HealthCheckResponse> healthCheck(
        apache.rocketmq.v1.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Sends one message to the specific partition of a topic, returns message id or transaction id with status `OK`.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.SendMessageResponse> sendMessage(
        apache.rocketmq.v1.SendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Querys the assigned partition route info of a topic for current consumer, 
     * the returned assignment result is descided by server-side load balacner.
     * If the corresponding topic doesn't exist, returns `NOT_FOUND`.
     * If the specific endpoints is emtpy, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryAssignmentResponse> queryAssignment(
        apache.rocketmq.v1.QueryAssignmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryAssignmentMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Receives messages from the server in batch manner, returns a set of messages if success.
     * The received messages should be acked or uacked after processed.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.ReceiveMessageResponse> receiveMessage(
        apache.rocketmq.v1.ReceiveMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReceiveMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Acknowledges the message associated with the `receipt_handle` or `offset` in the
     * `AckMessageRequest`, it means the message has been successfully processed.
     * Returns `OK` if the message server remove the relevant message successfully.
     * If the given receipt_handle is illegal or out of date, returns `INVALID_ARGUMENT`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.AckMessageResponse> ackMessage(
        apache.rocketmq.v1.AckMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAckMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Signals that the message has not been successfully processed. The message server should resend the message
     * follow the retry policy defined at server-side.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.NackMessageResponse> nackMessage(
        apache.rocketmq.v1.NackMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNackMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Forwards one message to dead letter queue if the DeadLetterPolicy is triggered by this message at client-side,
     * return `OK` if success.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getForwardMessageToDeadLetterQueueMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Commits or rollback one transactional message.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.EndTransactionResponse> endTransaction(
        apache.rocketmq.v1.EndTransactionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEndTransactionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Querys the offset of the specific partition, returns the offset with `OK` if success.
     * The message server should maintain a numerical offset for each message in a parition.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.QueryOffsetResponse> queryOffset(
        apache.rocketmq.v1.QueryOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryOffsetMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Pulls messages from the specific partition, returns a set of messages with next pull offset.
     * The pulled messages can't be acked or nacked, while the client is responsible for manage offesets for consumer,
     * typically update consume offset to local memory or a third-party storage service.
     * If the pending concurrent receive requests exceed the quota of the given consumer group, returns `UNAVAILABLE`.
     * If the upstream store server hangs, return `DEADLINE_EXCEEDED` in a timely manner.
     * If the corresponding topic or consumer group doesn't exist, returns `NOT_FOUND`.
     * If there is no new message in the specific topic, returns `OK` with an empty message set. Please note that client
     * may suffer from false empty responses.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.PullMessageResponse> pullMessage(
        apache.rocketmq.v1.PullMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPullMessageMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.MultiplexingResponse> multiplexingCall(
        apache.rocketmq.v1.MultiplexingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMultiplexingCallMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v1.NotifyClientTerminationResponse> notifyClientTermination(
        apache.rocketmq.v1.NotifyClientTerminationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNotifyClientTerminationMethod(), getCallOptions()), request);
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
  private static final int METHODID_FORWARD_MESSAGE_TO_DEAD_LETTER_QUEUE = 8;
  private static final int METHODID_END_TRANSACTION = 9;
  private static final int METHODID_QUERY_OFFSET = 10;
  private static final int METHODID_PULL_MESSAGE = 11;
  private static final int METHODID_MULTIPLEXING_CALL = 12;
  private static final int METHODID_NOTIFY_CLIENT_TERMINATION = 13;

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
        case METHODID_FORWARD_MESSAGE_TO_DEAD_LETTER_QUEUE:
          serviceImpl.forwardMessageToDeadLetterQueue((apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse>) responseObserver);
          break;
        case METHODID_END_TRANSACTION:
          serviceImpl.endTransaction((apache.rocketmq.v1.EndTransactionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.EndTransactionResponse>) responseObserver);
          break;
        case METHODID_QUERY_OFFSET:
          serviceImpl.queryOffset((apache.rocketmq.v1.QueryOffsetRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.QueryOffsetResponse>) responseObserver);
          break;
        case METHODID_PULL_MESSAGE:
          serviceImpl.pullMessage((apache.rocketmq.v1.PullMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.PullMessageResponse>) responseObserver);
          break;
        case METHODID_MULTIPLEXING_CALL:
          serviceImpl.multiplexingCall((apache.rocketmq.v1.MultiplexingRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.MultiplexingResponse>) responseObserver);
          break;
        case METHODID_NOTIFY_CLIENT_TERMINATION:
          serviceImpl.notifyClientTermination((apache.rocketmq.v1.NotifyClientTerminationRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v1.NotifyClientTerminationResponse>) responseObserver);
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
              .addMethod(getForwardMessageToDeadLetterQueueMethod())
              .addMethod(getEndTransactionMethod())
              .addMethod(getQueryOffsetMethod())
              .addMethod(getPullMessageMethod())
              .addMethod(getMultiplexingCallMethod())
              .addMethod(getNotifyClientTerminationMethod())
              .build();
        }
      }
    }
    return result;
  }
}
