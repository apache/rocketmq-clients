package org.apache.rocketmq.client.remoting;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.rocketmq.proto.RocketMQGrpc.RocketMQFutureStub;
import org.apache.rocketmq.proto.RocketMQGrpc.RocketMQStub;
import org.apache.rocketmq.proto.RouteInfoRequest;
import org.apache.rocketmq.proto.RouteInfoResponse;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class RPCClientImpl implements RPCClient {

  private static final long DEFAULT_TIMEOUT_MILLIS = 3 * 1000;

  /**
   * Usage of {@link RPCClientImpl#fetchTopicRouteInfo(RouteInfoRequest)} are usually invokes the
   * first call of gRPC, which need warm up in most case.
   */
  private static final long FETCH_TOPIC_ROUTE_INFO_TIMEOUT_MILLIS = 15 * 1000;

  private final RPCTarget rpcTarget;
  private final ManagedChannel channel;

  private final RocketMQBlockingStub blockingStub;
  private final RocketMQStub asyncStub;
  private final RocketMQFutureStub futureStub;

  private ThreadPoolExecutor sendMessageAsyncExecutor;
  private Semaphore sendMessageAsyncSemaphore;

  private RPCClientImpl(RPCTarget rpcTarget) {
    this.rpcTarget = rpcTarget;

    final String target = rpcTarget.getTarget();
    this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

    this.blockingStub = RocketMQGrpc.newBlockingStub(channel);
    this.asyncStub = RocketMQGrpc.newStub(channel);
    this.futureStub = RocketMQGrpc.newFutureStub(channel);
  }

  public RPCClientImpl(RPCTarget rpcTarget, ThreadPoolExecutor sendMessageAsyncExecutor) {
    this(rpcTarget);

    this.sendMessageAsyncExecutor = sendMessageAsyncExecutor;
    this.sendMessageAsyncSemaphore =
        new Semaphore(getThreadParallelCount(sendMessageAsyncExecutor));
  }

  private int getThreadParallelCount(ThreadPoolExecutor executor) {
    return executor.getMaximumPoolSize() + executor.getQueue().remainingCapacity();
  }

  @Override
  public void shutdown() {
    if (null != channel) {
      channel.shutdown();
    }
  }

  @Override
  public void setIsolated(boolean isolated) {
    rpcTarget.setIsolated(isolated);
  }

  @Override
  public boolean isIsolated() {
    return rpcTarget.isIsolated();
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
    try {
      sendMessageAsyncSemaphore.acquire();
      final RocketMQFutureStub stub =
          futureStub.withExecutor(sendMessageAsyncExecutor).withDeadlineAfter(duration, unit);
      final ListenableFuture<SendMessageResponse> future = stub.sendMessage(request);
      Futures.addCallback(
          future,
          new FutureCallback<SendMessageResponse>() {
            @Override
            public void onSuccess(@Nullable SendMessageResponse response) {
              try {
                context.onSuccess(response);
              } finally {
                sendMessageAsyncSemaphore.release();
              }
            }

            @Override
            public void onFailure(Throwable t) {
              try {
                context.onException(t);
              } finally {
                sendMessageAsyncSemaphore.release();
              }
            }
          },
          MoreExecutors.directExecutor());
    } catch (Throwable t) {
      try {
        context.onException(t);
      } finally {
        sendMessageAsyncSemaphore.release();
      }
    }
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
