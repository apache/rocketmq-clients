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

	v2 "github.com/apache/rocketmq-clients/golang/protocol/v2"
	gomock "github.com/golang/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// GetClientID mocks base method.
func (m *MockClient) GetClientID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClientID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetClientID indicates an expected call of GetClientID.
func (mr *MockClientMockRecorder) GetClientID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClientID", reflect.TypeOf((*MockClient)(nil).GetClientID))
}

// GracefulStop mocks base method.
func (m *MockClient) GracefulStop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GracefulStop")
	ret0, _ := ret[0].(error)
	return ret0
}

// GracefulStop indicates an expected call of GracefulStop.
func (mr *MockClientMockRecorder) GracefulStop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GracefulStop", reflect.TypeOf((*MockClient)(nil).GracefulStop))
}

// Sign mocks base method.
func (m *MockClient) Sign(ctx context.Context) context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sign", ctx)
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Sign indicates an expected call of Sign.
func (mr *MockClientMockRecorder) Sign(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sign", reflect.TypeOf((*MockClient)(nil).Sign), ctx)
}

// MockisClient is a mock of isClient interface.
type MockisClient struct {
	ctrl     *gomock.Controller
	recorder *MockisClientMockRecorder
}

// MockisClientMockRecorder is the mock recorder for MockisClient.
type MockisClientMockRecorder struct {
	mock *MockisClient
}

// NewMockisClient creates a new mock instance.
func NewMockisClient(ctrl *gomock.Controller) *MockisClient {
	mock := &MockisClient{ctrl: ctrl}
	mock.recorder = &MockisClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockisClient) EXPECT() *MockisClientMockRecorder {
	return m.recorder
}

// isClient mocks base method.
func (m *MockisClient) isClient() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "isClient")
}

// isClient indicates an expected call of isClient.
func (mr *MockisClientMockRecorder) isClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "isClient", reflect.TypeOf((*MockisClient)(nil).isClient))
}

// onRecoverOrphanedTransactionCommand mocks base method.
func (m *MockisClient) onRecoverOrphanedTransactionCommand(endpoints *v2.Endpoints, command *v2.RecoverOrphanedTransactionCommand) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "onRecoverOrphanedTransactionCommand", endpoints, command)
	ret0, _ := ret[0].(error)
	return ret0
}

// onRecoverOrphanedTransactionCommand indicates an expected call of onRecoverOrphanedTransactionCommand.
func (mr *MockisClientMockRecorder) onRecoverOrphanedTransactionCommand(endpoints, command interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "onRecoverOrphanedTransactionCommand", reflect.TypeOf((*MockisClient)(nil).onRecoverOrphanedTransactionCommand), endpoints, command)
}

// onVerifyMessageCommand mocks base method.
func (m *MockisClient) onVerifyMessageCommand(endpoints *v2.Endpoints, command *v2.VerifyMessageCommand) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "onVerifyMessageCommand", endpoints, command)
	ret0, _ := ret[0].(error)
	return ret0
}

// onVerifyMessageCommand indicates an expected call of onVerifyMessageCommand.
func (mr *MockisClientMockRecorder) onVerifyMessageCommand(endpoints, command interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "onVerifyMessageCommand", reflect.TypeOf((*MockisClient)(nil).onVerifyMessageCommand), endpoints, command)
}

// wrapHeartbeatRequest mocks base method.
func (m *MockisClient) wrapHeartbeatRequest() *v2.HeartbeatRequest {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "wrapHeartbeatRequest")
	ret0, _ := ret[0].(*v2.HeartbeatRequest)
	return ret0
}

// wrapHeartbeatRequest indicates an expected call of wrapHeartbeatRequest.
func (mr *MockisClientMockRecorder) wrapHeartbeatRequest() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "wrapHeartbeatRequest", reflect.TypeOf((*MockisClient)(nil).wrapHeartbeatRequest))
}
