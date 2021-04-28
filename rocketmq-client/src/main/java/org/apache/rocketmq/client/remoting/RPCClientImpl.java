package org.apache.rocketmq.client.remoting;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.Executor;
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

@Slf4j
public class RPCClientImpl implements RPCClient {

  private final RPCTarget rpcTarget;
  private final ManagedChannel channel;

  private final RocketMQBlockingStub blockingStub;
  private final RocketMQStub asyncStub;
  private final RocketMQFutureStub futureStub;

  public RPCClientImpl(RPCTarget rpcTarget) {
    this.rpcTarget = rpcTarget;
    this.channel =
        ManagedChannelBuilder.forTarget(rpcTarget.getTarget())
            .disableRetry()
            .usePlaintext()
            .build();

    this.blockingStub = RocketMQGrpc.newBlockingStub(channel);
    this.asyncStub = RocketMQGrpc.newStub(channel);
    this.futureStub = RocketMQGrpc.newFutureStub(channel);
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
    return blockingStub.withDeadlineAfter(duration, unit).sendMessage(request);
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

  @Override
  public ListenableFuture<PopMessageResponse> popMessage(
      PopMessageRequest request, Executor executor, long duration, TimeUnit unit) {
    return futureStub.withExecutor(executor).withDeadlineAfter(duration, unit).popMessage(request);
  }

  @Override
  public AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit) {
    return blockingStub.withDeadlineAfter(duration, unit).ackMessage(request);
  }

  @Override
  public ListenableFuture<AckMessageResponse> ackMessage(
      AckMessageRequest request, Executor executor, long duration, TimeUnit unit) {
    return futureStub.withExecutor(executor).withDeadlineAfter(duration, unit).ackMessage(request);
  }

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
