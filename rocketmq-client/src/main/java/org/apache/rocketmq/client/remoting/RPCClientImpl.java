package org.apache.rocketmq.client.remoting;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proto.AckMessageRequest;
import org.apache.rocketmq.proto.AckMessageResponse;
import org.apache.rocketmq.proto.ChangeInvisibleTimeRequest;
import org.apache.rocketmq.proto.ChangeInvisibleTimeResponse;
import org.apache.rocketmq.proto.HealthCheckRequest;
import org.apache.rocketmq.proto.HealthCheckResponse;
import org.apache.rocketmq.proto.HeartbeatRequest;
import org.apache.rocketmq.proto.HeartbeatResponse;
import org.apache.rocketmq.proto.PopMessageRequest;
import org.apache.rocketmq.proto.PopMessageResponse;
import org.apache.rocketmq.proto.QueryAssignmentRequest;
import org.apache.rocketmq.proto.QueryAssignmentResponse;
import org.apache.rocketmq.proto.RocketMQGrpc;
import org.apache.rocketmq.proto.RocketMQGrpc.RocketMQBlockingStub;
import org.apache.rocketmq.proto.RocketMQGrpc.RocketMQStub;
import org.apache.rocketmq.proto.RouteInfoRequest;
import org.apache.rocketmq.proto.RouteInfoResponse;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;

public class RPCClientImpl implements RPCClient {
  private static final long DEFAULT_TIMEOUT_MILLIS = 3 * 1000;
  /**
   * Usage of {@link RPCClientImpl#fetchTopicRouteInfo(RouteInfoRequest)} are usually invokes the
   * first call of gRPC, which need warm up in most case.
   */
  private static final long FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS = 15 * 1000;

  private final RPCTarget RPCTarget;
  private final ManagedChannel channel;

  private final RocketMQBlockingStub blockingStub;
  private final RocketMQStub asyncStub;
  private final RocketMQGrpc.RocketMQFutureStub futureStub;

  public RPCClientImpl(RPCTarget RPCTarget, ThreadPoolExecutor asyncExecutor) {
    this.RPCTarget = RPCTarget;

    final String target = RPCTarget.getTarget();
    this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

    this.blockingStub = RocketMQGrpc.newBlockingStub(channel);
    this.asyncStub = RocketMQGrpc.newStub(channel).withExecutor(asyncExecutor);
    this.futureStub = RocketMQGrpc.newFutureStub(channel).withExecutor(asyncExecutor);
  }

  @Override
  public void shutdown() {
    if (null != channel) {
      channel.shutdown();
    }
  }

  @Override
  public void setIsolated(boolean isolated) {
    RPCTarget.setIsolated(isolated);
  }

  @Override
  public boolean isIsolated() {
    return RPCTarget.isIsolated();
  }

  @Override
  public RouteInfoResponse getRouteInfo(RouteInfoRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .fetchTopicRouteInfo(request);
  }

  @Override
  public SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit) {
    RocketMQBlockingStub stub = blockingStub.withDeadlineAfter(duration, unit);
    return stub.sendMessage(request);
  }

  @Override
  public void sendMessage(
      SendMessageRequest request,
      InvocationContext<SendMessageResponse> context,
      long duration,
      TimeUnit unit) {
    RocketMQStub stub = asyncStub.withDeadlineAfter(duration, unit);
    StreamObserver<SendMessageResponse> observer =
        new StreamObserver<SendMessageResponse>() {
          @Override
          public void onNext(SendMessageResponse response) {
            context.onSuccess(response);
          }

          @Override
          public void onError(Throwable t) {
            context.onException(t);
          }

          @Override
          public void onCompleted() {}
        };
    stub.sendMessage(request, observer);
  }

  @Override
  public QueryAssignmentResponse queryAssignment(QueryAssignmentRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .queryAssignment(request);
  }

  @Override
  public HealthCheckResponse healthCheck(HealthCheckRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .healthCheck(request);
  }

  @Override
  public void popMessage(PopMessageRequest request, InvocationContext<PopMessageResponse> context) {
    StreamObserver<PopMessageResponse> observer =
        new StreamObserver<PopMessageResponse>() {
          @Override
          public void onNext(PopMessageResponse response) {
            context.onSuccess(response);
          }

          @Override
          public void onError(Throwable t) {
            context.onException(t);
          }

          @Override
          public void onCompleted() {}
        };
    asyncStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .popMessage(request, observer);
  }

  @Override
  public AckMessageResponse ackMessage(AckMessageRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .ackMessage(request);
  }

  @Override
  public void ackMessage(AckMessageRequest request, InvocationContext<AckMessageResponse> context) {
    StreamObserver<AckMessageResponse> observer =
        new StreamObserver<AckMessageResponse>() {
          @Override
          public void onNext(AckMessageResponse response) {
            context.onSuccess(response);
          }

          @Override
          public void onError(Throwable t) {
            context.onException(t);
          }

          @Override
          public void onCompleted() {}
        };
    asyncStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .ackMessage(request, observer);
  }

  @Override
  public ChangeInvisibleTimeResponse changeInvisibleTime(ChangeInvisibleTimeRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .changeInvisibleTime(request);
  }

  @Override
  public HeartbeatResponse heartbeat(HeartbeatRequest request) {
    return blockingStub
        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .heartbeat(request);
  }

  @Override
  public RouteInfoResponse fetchTopicRouteInfo(RouteInfoRequest request) {
    return blockingStub
        .withDeadlineAfter(FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .fetchTopicRouteInfo(request);
  }
}
