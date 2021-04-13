package org.apache.rocketmq.client.remoting;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
import org.apache.rocketmq.proto.RouteInfoRequest;
import org.apache.rocketmq.proto.RouteInfoResponse;
import org.apache.rocketmq.proto.SendMessageRequest;
import org.apache.rocketmq.proto.SendMessageResponse;

public interface RPCClient {
  void shutdown();

  void setIsolated(boolean isolated);

  boolean isIsolated();

  SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit);

  ListenableFuture<SendMessageResponse> sendMessage(
      SendMessageRequest request, Executor executor, long duration, TimeUnit unit);

  QueryAssignmentResponse queryAssignment(
      QueryAssignmentRequest request, long duration, TimeUnit unit);

  HealthCheckResponse healthCheck(HealthCheckRequest request, long duration, TimeUnit unit);

  //  void popMessage(
  //      PopMessageRequest request, SendMessageResponseCallback<PopMessageResponse>
  // sendMessageResponseCallback);

  AckMessageResponse ackMessage(AckMessageRequest request, long duration, TimeUnit unit);

  //  void ackMessage(
  //      AckMessageRequest request, SendMessageResponseCallback<AckMessageResponse>
  // sendMessageResponseCallback);

  ChangeInvisibleTimeResponse changeInvisibleTime(
      ChangeInvisibleTimeRequest request, long duration, TimeUnit unit);

  HeartbeatResponse heartbeat(HeartbeatRequest request, long duration, TimeUnit unit);

  RouteInfoResponse fetchTopicRouteInfo(RouteInfoRequest request, long duration, TimeUnit unit);
}
