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
	context "context"
	reflect "reflect"
	time "time"

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	gomock "github.com/golang/mock/gomock"
)

// MockRpcClient is a mock of RpcClient interface.
type MockRpcClient struct {
	ctrl     *gomock.Controller
	recorder *MockRpcClientMockRecorder
}

// MockRpcClientMockRecorder is the mock recorder for MockRpcClient.
type MockRpcClientMockRecorder struct {
	mock *MockRpcClient
}

// NewMockRpcClient creates a new mock instance.
func NewMockRpcClient(ctrl *gomock.Controller) *MockRpcClient {
	mock := &MockRpcClient{ctrl: ctrl}
	mock.recorder = &MockRpcClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRpcClient) EXPECT() *MockRpcClientMockRecorder {
	return m.recorder
}

// AckMessage mocks base method.
func (m *MockRpcClient) AckMessage(ctx context.Context, request *v2.AckMessageRequest) (*v2.AckMessageResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AckMessage", ctx, request)
	ret0, _ := ret[0].(*v2.AckMessageResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AckMessage indicates an expected call of AckMessage.
func (mr *MockRpcClientMockRecorder) AckMessage(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AckMessage", reflect.TypeOf((*MockRpcClient)(nil).AckMessage), ctx, request)
}

// ChangeInvisibleDuration mocks base method.
func (m *MockRpcClient) ChangeInvisibleDuration(ctx context.Context, request *v2.ChangeInvisibleDurationRequest) (*v2.ChangeInvisibleDurationResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeInvisibleDuration", ctx, request)
	ret0, _ := ret[0].(*v2.ChangeInvisibleDurationResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChangeInvisibleDuration indicates an expected call of ChangeInvisibleDuration.
func (mr *MockRpcClientMockRecorder) ChangeInvisibleDuration(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeInvisibleDuration", reflect.TypeOf((*MockRpcClient)(nil).ChangeInvisibleDuration), ctx, request)
}

// EndTransaction mocks base method.
func (m *MockRpcClient) EndTransaction(ctx context.Context, request *v2.EndTransactionRequest) (*v2.EndTransactionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EndTransaction", ctx, request)
	ret0, _ := ret[0].(*v2.EndTransactionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// EndTransaction indicates an expected call of EndTransaction.
func (mr *MockRpcClientMockRecorder) EndTransaction(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EndTransaction", reflect.TypeOf((*MockRpcClient)(nil).EndTransaction), ctx, request)
}

// GetTarget mocks base method.
func (m *MockRpcClient) GetTarget() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTarget")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetTarget indicates an expected call of GetTarget.
func (mr *MockRpcClientMockRecorder) GetTarget() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTarget", reflect.TypeOf((*MockRpcClient)(nil).GetTarget))
}

// GracefulStop mocks base method.
func (m *MockRpcClient) GracefulStop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GracefulStop")
	ret0, _ := ret[0].(error)
	return ret0
}

// GracefulStop indicates an expected call of GracefulStop.
func (mr *MockRpcClientMockRecorder) GracefulStop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GracefulStop", reflect.TypeOf((*MockRpcClient)(nil).GracefulStop))
}

// HeartBeat mocks base method.
func (m *MockRpcClient) HeartBeat(ctx context.Context, request *v2.HeartbeatRequest) (*v2.HeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HeartBeat", ctx, request)
	ret0, _ := ret[0].(*v2.HeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HeartBeat indicates an expected call of HeartBeat.
func (mr *MockRpcClientMockRecorder) HeartBeat(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HeartBeat", reflect.TypeOf((*MockRpcClient)(nil).HeartBeat), ctx, request)
}

// NotifyClientTermination mocks base method.
func (m *MockRpcClient) NotifyClientTermination(ctx context.Context, request *v2.NotifyClientTerminationRequest) (*v2.NotifyClientTerminationResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClientTermination", ctx, request)
	ret0, _ := ret[0].(*v2.NotifyClientTerminationResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NotifyClientTermination indicates an expected call of NotifyClientTermination.
func (mr *MockRpcClientMockRecorder) NotifyClientTermination(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClientTermination", reflect.TypeOf((*MockRpcClient)(nil).NotifyClientTermination), ctx, request)
}

// QueryRoute mocks base method.
func (m *MockRpcClient) QueryRoute(ctx context.Context, request *v2.QueryRouteRequest) (*v2.QueryRouteResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryRoute", ctx, request)
	ret0, _ := ret[0].(*v2.QueryRouteResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryRoute indicates an expected call of QueryRoute.
func (mr *MockRpcClientMockRecorder) QueryRoute(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryRoute", reflect.TypeOf((*MockRpcClient)(nil).QueryRoute), ctx, request)
}

// ReceiveMessage mocks base method.
func (m *MockRpcClient) ReceiveMessage(ctx context.Context, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReceiveMessage", ctx, request)
	ret0, _ := ret[0].(v2.MessagingService_ReceiveMessageClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReceiveMessage indicates an expected call of ReceiveMessage.
func (mr *MockRpcClientMockRecorder) ReceiveMessage(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReceiveMessage", reflect.TypeOf((*MockRpcClient)(nil).ReceiveMessage), ctx, request)
}

// SendMessage mocks base method.
func (m *MockRpcClient) SendMessage(ctx context.Context, request *v2.SendMessageRequest) (*v2.SendMessageResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessage", ctx, request)
	ret0, _ := ret[0].(*v2.SendMessageResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessage indicates an expected call of SendMessage.
func (mr *MockRpcClientMockRecorder) SendMessage(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessage", reflect.TypeOf((*MockRpcClient)(nil).SendMessage), ctx, request)
}

// Telemetry mocks base method.
func (m *MockRpcClient) Telemetry(ctx context.Context) (v2.MessagingService_TelemetryClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Telemetry", ctx)
	ret0, _ := ret[0].(v2.MessagingService_TelemetryClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Telemetry indicates an expected call of Telemetry.
func (mr *MockRpcClientMockRecorder) Telemetry(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Telemetry", reflect.TypeOf((*MockRpcClient)(nil).Telemetry), ctx)
}

// idleDuration mocks base method.
func (m *MockRpcClient) idleDuration() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "idleDuration")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// idleDuration indicates an expected call of idleDuration.
func (mr *MockRpcClientMockRecorder) idleDuration() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "idleDuration", reflect.TypeOf((*MockRpcClient)(nil).idleDuration))
}
