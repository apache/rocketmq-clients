package org.apache.rocketmq.client.remoting;

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

  RouteInfoResponse getRouteInfo(RouteInfoRequest request);

  SendMessageResponse sendMessage(SendMessageRequest request, long duration, TimeUnit unit);

  void sendMessage(
      SendMessageRequest request,
      SendMessageResponseCallback callback,
      long duration,
      TimeUnit unit);

  QueryAssignmentResponse queryAssignment(QueryAssignmentRequest request);

  HealthCheckResponse healthCheck(HealthCheckRequest request);

  //  void popMessage(
  //      PopMessageRequest request, SendMessageResponseCallback<PopMessageResponse>
  // sendMessageResponseCallback);

  AckMessageResponse ackMessage(AckMessageRequest request);

  //  void ackMessage(
  //      AckMessageRequest request, SendMessageResponseCallback<AckMessageResponse>
  // sendMessageResponseCallback);

  ChangeInvisibleTimeResponse changeInvisibleTime(ChangeInvisibleTimeRequest request);

  HeartbeatResponse heartbeat(HeartbeatRequest request);

  RouteInfoResponse fetchTopicRouteInfo(RouteInfoRequest request);
}
