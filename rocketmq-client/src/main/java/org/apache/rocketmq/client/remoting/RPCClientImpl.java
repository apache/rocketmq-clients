package org.apache.rocketmq.client.remoting;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.Executor;
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

@Slf4j
public class RPCClientImpl implements RPCClient {

  private final RPCTarget rpcTarget;
  private final ManagedChannel channel;

  private final RocketMQBlockingStub blockingStub;
  private final RocketMQStub asyncStub;
  private final RocketMQFutureStub futureStub;
  /** Default callback executor. */
  private ThreadPoolExecutor callbackExecutor;

  private Semaphore callbackSemaphore;

  private ThreadPoolExecutor sendCallbackExecutor;
  private Semaphore sendCallbackSemaphore;

  private RPCClientImpl(RPCTarget rpcTarget) {
    this.rpcTarget = rpcTarget;
    final String target = rpcTarget.getTarget();
    this.channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

    this.blockingStub = RocketMQGrpc.newBlockingStub(channel);
    this.asyncStub = RocketMQGrpc.newStub(channel);
    this.futureStub = RocketMQGrpc.newFutureStub(channel);
    this.sendCallbackExecutor = null;
    this.sendCallbackSemaphore = null;
  }

  public RPCClientImpl(RPCTarget rpcTarget, ThreadPoolExecutor callbackExecutor) {
    this(rpcTarget);

    //    this.callbackExecutor = callbackExecutor;
    //    this.callbackSemaphore = new Semaphore(getThreadParallelCount(callbackExecutor));
  }

  public void setSendCallbackExecutor(ThreadPoolExecutor sendCallbackExecutor) {
    this.sendCallbackExecutor = sendCallbackExecutor;
    this.sendCallbackSemaphore = new Semaphore(getThreadParallelCount(sendCallbackExecutor));
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
  public SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit) {
    RocketMQBlockingStub stub = blockingStub.withDeadlineAfter(duration, unit);
    return stub.sendMessage(request);
  }

  @Override
  public ListenableFuture<SendMessageResponse> sendMessage(
      SendMessageRequest request, Executor executor, long duration, TimeUnit unit) {
    return futureStub.withExecutor(executor).withDeadlineAfter(duration, unit).sendMessage(request);
  }

  @Override
  public QueryAssignmentResponse queryAssignment(
      QueryAssignmentRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).queryAssignment(request);
  }

  @Override
  public HealthCheckResponse healthCheck(HealthCheckRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).healthCheck(request);
  }

  //  @Override
  //  public void popMessage(PopMessageRequest request,
  // SendMessageResponseCallback<PopMessageResponse> context) {
  //    StreamObserver<PopMessageResponse> observer =
  //        new StreamObserver<PopMessageResponse>() {
  //          @Override
  //          public void onNext(PopMessageResponse response) {
  //            context.onSuccess(response);
  //          }
  //
  //          @Override
  //          public void onError(Throwable t) {
  //            context.onException(t);
  //          }
  //
  //          @Override
  //          public void onCompleted() {}
  //        };
  //    asyncStub
  //        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
  //        .popMessage(request, observer);
  //  }

  @Override
  public AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).ackMessage(request);
  }

  //  @Override
  //  public void ackMessage(AckMessageRequest request,
  // SendMessageResponseCallback<AckMessageResponse> context) {
  //    StreamObserver<AckMessageResponse> observer =
  //        new StreamObserver<AckMessageResponse>() {
  //          @Override
  //          public void onNext(AckMessageResponse response) {
  //            context.onSuccess(response);
  //          }
  //
  //          @Override
  //          public void onError(Throwable t) {
  //            context.onException(t);
  //          }
  //
  //          @Override
  //          public void onCompleted() {}
  //        };
  //    asyncStub
  //        .withDeadlineAfter(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
  //        .ackMessage(request, observer);
  //  }

  @Override
  public ChangeInvisibleTimeResponse changeInvisibleTime(
      ChangeInvisibleTimeRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).changeInvisibleTime(request);
  }

  @Override
  public HeartbeatResponse heartbeat(HeartbeatRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).heartbeat(request);
  }

  @Override
  public RouteInfoResponse fetchTopicRouteInfo(
      RouteInfoRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).fetchTopicRouteInfo(request);
  }
}
