/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package golang

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	// "google.golang.org/protobuf/types/known/durationpb"
)

var (
	ErrNoAvailableBrokers = errors.New("rocketmq: no available brokers")
)

type RpcClient interface {
	GracefulStop() error
	HeartBeat(ctx context.Context, request *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error)
	QueryRoute(ctx context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error)
	SendMessage(ctx context.Context, request *v2.SendMessageRequest) (*v2.SendMessageResponse, error)
	Telemetry(ctx context.Context) (v2.MessagingService_TelemetryClient, error)
	EndTransaction(ctx context.Context, request *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error)
	NotifyClientTermination(ctx context.Context, request *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error)
	ReceiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error)
	AckMessage(ctx context.Context, request *v2.AckMessageRequest) (*v2.AckMessageResponse, error)
	ChangeInvisibleDuration(ctx context.Context, request *v2.ChangeInvisibleDurationRequest) (*v2.ChangeInvisibleDurationResponse, error)
	idleDuration() time.Duration
	GetTarget() string
}

var _ = RpcClient(&rpcClient{})

type rpcClient struct {
	opts             rpcClientOptions
	queue            atomic.Value
	mux              sync.Mutex
	conn             ClientConn
	msc              v2.MessagingServiceClient
	target           string
	activityNanoTime time.Time
}

var NewRpcClient = func(target string, opts ...RpcClientOption) (RpcClient, error) {
	rc := &rpcClient{
		target: target,
		opts:   defaultRpcClientOptions,
	}
	for _, opt := range opts {
		opt.apply(&rc.opts)
	}
	conn, err := rc.opts.clientConnFunc(target, rc.opts.connOptions...)
	if err != nil {
		return nil, fmt.Errorf("create grpc conn failed, err=%w", err)
	}
	rc.conn = conn
	rc.msc = v2.NewMessagingServiceClient(conn.Conn())
	rc.activityNanoTime = time.Now()
	sugarBaseLogger.Infof("create rpc client success, target=%v", target)
	return rc, nil
}

func (rc *rpcClient) GetTarget() string {
	return rc.target
}

func (rc *rpcClient) idleDuration() time.Duration {
	return time.Now().Sub(rc.activityNanoTime)
}

func (rc *rpcClient) Close() {}

func (rc *rpcClient) GracefulStop() error {
	sugarBaseLogger.Warnf("close rpc client, target=%s", rc.target)
	return rc.conn.Close()
}

func (rc *rpcClient) QueryRoute(ctx context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.QueryRoute(ctx, request)
	sugarBaseLogger.Debugf("queryRoute request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) SendMessage(ctx context.Context, request *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.SendMessage(ctx, request)
	sugarBaseLogger.Debugf("sendMessage request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) Telemetry(ctx context.Context) (v2.MessagingService_TelemetryClient, error) {
	return rc.msc.Telemetry(ctx)
}

func (rc *rpcClient) EndTransaction(ctx context.Context, request *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.EndTransaction(ctx, request)
	sugarBaseLogger.Debugf("endTransaction request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) HeartBeat(ctx context.Context, request *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.Heartbeat(ctx, request)
	sugarBaseLogger.Debugf("heartBeat request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) NotifyClientTermination(ctx context.Context, request *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.NotifyClientTermination(ctx, request)
	sugarBaseLogger.Debugf("notifyClientTermination request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) ReceiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.ReceiveMessage(ctx, request)
	sugarBaseLogger.Debugf("receiveMessage request: %v, err: %v", request, err)
	return resp, err
}

func (rc *rpcClient) AckMessage(ctx context.Context, request *v2.AckMessageRequest) (*v2.AckMessageResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.AckMessage(ctx, request)
	sugarBaseLogger.Debugf("ackMessage request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) ChangeInvisibleDuration(ctx context.Context, request *v2.ChangeInvisibleDurationRequest) (*v2.ChangeInvisibleDurationResponse, error) {
	rc.activityNanoTime = time.Now()
	resp, err := rc.msc.ChangeInvisibleDuration(ctx, request)
	sugarBaseLogger.Debugf("changeInvisibleDuration request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}
