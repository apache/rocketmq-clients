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
	"time"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	ErrNoAvailableBrokers = errors.New("rocketmq: no available brokers")
)

type RpcClient interface {
	GracefulStop() error
	HeartBeat(ctx context.Context, request *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error)
	QueryRoute(ctx context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error)
	QueryAssignments(ctx context.Context, request *v2.QueryAssignmentRequest) (*v2.QueryAssignmentResponse, error)
	SendMessage(ctx context.Context, request *v2.SendMessageRequest) (*v2.SendMessageResponse, error)
	Telemetry(ctx context.Context) (v2.MessagingService_TelemetryClient, error)
	EndTransaction(ctx context.Context, request *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error)
	NotifyClientTermination(ctx context.Context, request *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error)
	ReceiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error)
	AckMessage(ctx context.Context, request *v2.AckMessageRequest) (*v2.AckMessageResponse, error)
	ChangeInvisibleDuration(ctx context.Context, request *v2.ChangeInvisibleDurationRequest) (*v2.ChangeInvisibleDurationResponse, error)
	ForwardMessageToDeadLetterQueue(ctx context.Context, request *v2.ForwardMessageToDeadLetterQueueRequest) (*v2.ForwardMessageToDeadLetterQueueResponse, error)
	SyncLiteSubscription(ctx context.Context, request *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error)
	RecallMessage(ctx context.Context, request *v2.RecallMessageRequest) (*v2.RecallMessageResponse, error)
	idleDuration() time.Duration
	GetTarget() string
}

var _ = RpcClient(&rpcClient{})

type rpcClient struct {
	// The transport, stub and effective options are immutable after construction.
	opts             rpcClientOptions
	mux              sync.Mutex
	conn             ClientConn
	msc              v2.MessagingServiceClient
	target           string
	activityNanoTime time.Time
	cancel           context.CancelFunc
	closeOnce        sync.Once
	closeErr         error
}

// These optional capabilities deliberately do not extend the public interfaces.
type rpcClientRecreator interface {
	recreate(context.Context) (RpcClient, error)
}

type rpcClientState interface {
	getState() connectivity.State
}

// The manager's dial context is not retained as an original connection option:
// canceling an earlier dial must not poison a later recreation.
type rpcClientContextOption struct {
	ctx context.Context
}

func (o rpcClientContextOption) apply(opts *rpcClientOptions) {
	opts.connOptions = append(opts.connOptions, WithContext(o.ctx))
}

var NewRpcClient = newRpcClient

func newRpcClient(target string, opts ...RpcClientOption) (RpcClient, error) {
	options := defaultRpcClientOptions
	options.connOptions = append([]ConnOption(nil), options.connOptions...)
	ctx := context.Background()
	for _, opt := range opts {
		if internal, ok := opt.(rpcClientContextOption); ok {
			ctx = internal.ctx
		} else {
			opt.apply(&options)
		}
	}

	// Freeze the effective defaults as well as explicit TLS, call and dial options.
	connOpts := defaultConnOptions
	connOpts.DialOptions = append([]grpc.DialOption(nil), connOpts.DialOptions...)
	for _, opt := range options.connOptions {
		opt.apply(&connOpts)
	}
	connOpts.DialOptions = append([]grpc.DialOption(nil), connOpts.DialOptions...)
	options.connOptions = []ConnOption{newFuncConnOption(func(o *connOptions) {
		*o = connOpts
		o.DialOptions = append([]grpc.DialOption(nil), connOpts.DialOptions...)
	})}
	return newRpcClientWithOptions(ctx, target, options)
}

func newRpcClientWithOptions(ctx context.Context, target string, opts rpcClientOptions) (RpcClient, error) {
	connOpts := connOptions{}
	for _, opt := range opts.connOptions {
		opt.apply(&connOpts)
	}
	dialCtx, cancel := context.WithCancel(ctx)
	stopParent := func() bool { return false }
	if connOpts.Context != nil {
		stopParent = context.AfterFunc(connOpts.Context, cancel)
		if connOpts.Context.Err() != nil {
			cancel()
		}
	}
	cleanup := func() {
		stopParent()
		cancel()
	}
	dialOptions := append([]ConnOption(nil), opts.connOptions...)
	dialOptions = append(dialOptions, WithContext(dialCtx))
	conn, err := opts.clientConnFunc(target, dialOptions...)
	if err == nil {
		err = dialCtx.Err()
	}
	if err != nil {
		cleanup()
		if conn != nil {
			_ = conn.Close()
		}
		return nil, fmt.Errorf("create grpc conn failed, err=%w", err)
	}
	rc := &rpcClient{
		target:           target,
		opts:             opts,
		conn:             conn,
		msc:              v2.NewMessagingServiceClient(conn.Conn()),
		activityNanoTime: time.Now(),
		cancel:           cleanup,
	}
	sugarBaseLogger.Infof("create rpc client success, target=%v", target)
	return rc, nil
}

func (rc *rpcClient) recreate(ctx context.Context) (RpcClient, error) {
	return newRpcClientWithOptions(ctx, rc.target, rc.opts)
}

func (rc *rpcClient) getState() connectivity.State {
	return rc.conn.Conn().GetState()
}

func (rc *rpcClient) GetTarget() string {
	return rc.target
}

func (rc *rpcClient) idleDuration() time.Duration {
	rc.mux.Lock()
	duration := time.Since(rc.activityNanoTime)
	rc.mux.Unlock()
	return duration
}

func (rc *rpcClient) Close() {}

func (rc *rpcClient) GracefulStop() error {
	rc.closeOnce.Do(func() {
		if rc.cancel != nil {
			rc.cancel()
		}
		rc.closeErr = rc.conn.Close()
		sugarBaseLogger.Warnf("close rpc client, target=%s", rc.target)
	})
	return rc.closeErr
}

func (rc *rpcClient) QueryRoute(ctx context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.QueryRoute(ctx, request)
	sugarBaseLogger.Debugf("queryRoute request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) QueryAssignments(ctx context.Context, request *v2.QueryAssignmentRequest) (*v2.QueryAssignmentResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.QueryAssignment(ctx, request)
	sugarBaseLogger.Debugf("queryAssignment request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) SendMessage(ctx context.Context, request *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.SendMessage(ctx, request)
	sugarBaseLogger.Debugf("sendMessage request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) Telemetry(ctx context.Context) (v2.MessagingService_TelemetryClient, error) {
	return rc.msc.Telemetry(ctx)
}

func (rc *rpcClient) EndTransaction(ctx context.Context, request *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.EndTransaction(ctx, request)
	sugarBaseLogger.Debugf("endTransaction request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) HeartBeat(ctx context.Context, request *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.Heartbeat(ctx, request)
	sugarBaseLogger.Debugf("heartBeat request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) NotifyClientTermination(ctx context.Context, request *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.NotifyClientTermination(ctx, request)
	sugarBaseLogger.Debugf("notifyClientTermination request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) ReceiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.ReceiveMessage(ctx, request)
	sugarBaseLogger.Debugf("receiveMessage request: %v, err: %v", request, err)
	return resp, err
}

func (rc *rpcClient) AckMessage(ctx context.Context, request *v2.AckMessageRequest) (*v2.AckMessageResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.AckMessage(ctx, request)
	sugarBaseLogger.Debugf("ackMessage request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) ChangeInvisibleDuration(ctx context.Context, request *v2.ChangeInvisibleDurationRequest) (*v2.ChangeInvisibleDurationResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.ChangeInvisibleDuration(ctx, request)
	sugarBaseLogger.Debugf("changeInvisibleDuration request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) ForwardMessageToDeadLetterQueue(ctx context.Context, request *v2.ForwardMessageToDeadLetterQueueRequest) (*v2.ForwardMessageToDeadLetterQueueResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.ForwardMessageToDeadLetterQueue(ctx, request)
	sugarBaseLogger.Debugf("forwardMessageToDeadLetterQueue request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) SyncLiteSubscription(ctx context.Context, request *v2.SyncLiteSubscriptionRequest) (*v2.SyncLiteSubscriptionResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.SyncLiteSubscription(ctx, request)
	sugarBaseLogger.Debugf("SyncLiteSubscription request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}

func (rc *rpcClient) RecallMessage(ctx context.Context, request *v2.RecallMessageRequest) (*v2.RecallMessageResponse, error) {
	rc.mux.Lock()
	rc.activityNanoTime = time.Now()
	rc.mux.Unlock()
	resp, err := rc.msc.RecallMessage(ctx, request)
	sugarBaseLogger.Debugf("recallMessage request: %v, response: %v, err: %v", request, resp, err)
	return resp, err
}
