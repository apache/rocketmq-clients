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

// MockClientManager is a mock of ClientManager interface.
type MockClientManager struct {
	ctrl     *gomock.Controller
	recorder *MockClientManagerMockRecorder
}

// MockClientManagerMockRecorder is the mock recorder for MockClientManager.
type MockClientManagerMockRecorder struct {
	mock *MockClientManager
}

// NewMockClientManager creates a new mock instance.
func NewMockClientManager(ctrl *gomock.Controller) *MockClientManager {
	mock := &MockClientManager{ctrl: ctrl}
	mock.recorder = &MockClientManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClientManager) EXPECT() *MockClientManagerMockRecorder {
	return m.recorder
}

// AckMessage mocks base method.
func (m *MockClientManager) AckMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.AckMessageRequest, duration time.Duration) (*v2.AckMessageResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AckMessage", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.AckMessageResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AckMessage indicates an expected call of AckMessage.
func (mr *MockClientManagerMockRecorder) AckMessage(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AckMessage", reflect.TypeOf((*MockClientManager)(nil).AckMessage), ctx, endpoints, request, duration)
}

// ChangeInvisibleDuration mocks base method.
func (m *MockClientManager) ChangeInvisibleDuration(ctx context.Context, endpoints *v2.Endpoints, request *v2.ChangeInvisibleDurationRequest, duration time.Duration) (*v2.ChangeInvisibleDurationResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeInvisibleDuration", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.ChangeInvisibleDurationResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChangeInvisibleDuration indicates an expected call of ChangeInvisibleDuration.
func (mr *MockClientManagerMockRecorder) ChangeInvisibleDuration(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeInvisibleDuration", reflect.TypeOf((*MockClientManager)(nil).ChangeInvisibleDuration), ctx, endpoints, request, duration)
}

// EndTransaction mocks base method.
func (m *MockClientManager) EndTransaction(ctx context.Context, endpoints *v2.Endpoints, request *v2.EndTransactionRequest, duration time.Duration) (*v2.EndTransactionResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EndTransaction", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.EndTransactionResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// EndTransaction indicates an expected call of EndTransaction.
func (mr *MockClientManagerMockRecorder) EndTransaction(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EndTransaction", reflect.TypeOf((*MockClientManager)(nil).EndTransaction), ctx, endpoints, request, duration)
}

// HeartBeat mocks base method.
func (m *MockClientManager) HeartBeat(ctx context.Context, endpoints *v2.Endpoints, request *v2.HeartbeatRequest, duration time.Duration) (*v2.HeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HeartBeat", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.HeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HeartBeat indicates an expected call of HeartBeat.
func (mr *MockClientManagerMockRecorder) HeartBeat(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HeartBeat", reflect.TypeOf((*MockClientManager)(nil).HeartBeat), ctx, endpoints, request, duration)
}

// NotifyClientTermination mocks base method.
func (m *MockClientManager) NotifyClientTermination(ctx context.Context, endpoints *v2.Endpoints, request *v2.NotifyClientTerminationRequest, duration time.Duration) (*v2.NotifyClientTerminationResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClientTermination", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.NotifyClientTerminationResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NotifyClientTermination indicates an expected call of NotifyClientTermination.
func (mr *MockClientManagerMockRecorder) NotifyClientTermination(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClientTermination", reflect.TypeOf((*MockClientManager)(nil).NotifyClientTermination), ctx, endpoints, request, duration)
}

// QueryRoute mocks base method.
func (m *MockClientManager) QueryRoute(ctx context.Context, endpoints *v2.Endpoints, request *v2.QueryRouteRequest, duration time.Duration) (*v2.QueryRouteResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryRoute", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.QueryRouteResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryRoute indicates an expected call of QueryRoute.
func (mr *MockClientManagerMockRecorder) QueryRoute(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryRoute", reflect.TypeOf((*MockClientManager)(nil).QueryRoute), ctx, endpoints, request, duration)
}

// ReceiveMessage mocks base method.
func (m *MockClientManager) ReceiveMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.ReceiveMessageRequest) (v2.MessagingService_ReceiveMessageClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReceiveMessage", ctx, endpoints, request)
	ret0, _ := ret[0].(v2.MessagingService_ReceiveMessageClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReceiveMessage indicates an expected call of ReceiveMessage.
func (mr *MockClientManagerMockRecorder) ReceiveMessage(ctx, endpoints, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReceiveMessage", reflect.TypeOf((*MockClientManager)(nil).ReceiveMessage), ctx, endpoints, request)
}

// RegisterClient mocks base method.
func (m *MockClientManager) RegisterClient(client Client) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterClient", client)
}

// RegisterClient indicates an expected call of RegisterClient.
func (mr *MockClientManagerMockRecorder) RegisterClient(client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterClient", reflect.TypeOf((*MockClientManager)(nil).RegisterClient), client)
}

// SendMessage mocks base method.
func (m *MockClientManager) SendMessage(ctx context.Context, endpoints *v2.Endpoints, request *v2.SendMessageRequest, duration time.Duration) (*v2.SendMessageResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendMessage", ctx, endpoints, request, duration)
	ret0, _ := ret[0].(*v2.SendMessageResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SendMessage indicates an expected call of SendMessage.
func (mr *MockClientManagerMockRecorder) SendMessage(ctx, endpoints, request, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMessage", reflect.TypeOf((*MockClientManager)(nil).SendMessage), ctx, endpoints, request, duration)
}

// Telemetry mocks base method.
func (m *MockClientManager) Telemetry(ctx context.Context, endpoints *v2.Endpoints, duration time.Duration) (v2.MessagingService_TelemetryClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Telemetry", ctx, endpoints, duration)
	ret0, _ := ret[0].(v2.MessagingService_TelemetryClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Telemetry indicates an expected call of Telemetry.
func (mr *MockClientManagerMockRecorder) Telemetry(ctx, endpoints, duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Telemetry", reflect.TypeOf((*MockClientManager)(nil).Telemetry), ctx, endpoints, duration)
}

// UnRegisterClient mocks base method.
func (m *MockClientManager) UnRegisterClient(client Client) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "UnRegisterClient", client)
}

// UnRegisterClient indicates an expected call of UnRegisterClient.
func (mr *MockClientManagerMockRecorder) UnRegisterClient(client interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnRegisterClient", reflect.TypeOf((*MockClientManager)(nil).UnRegisterClient), client)
}
