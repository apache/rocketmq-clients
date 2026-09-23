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
	"errors"
	"fmt"

	v2 "github.com/apache/rocketmq-clients/golang/v5/protocol/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrRpcStatus struct {
	Code    int32
	Message string
}

func (err *ErrRpcStatus) GetCode() int32 {
	return err.Code
}

func (err *ErrRpcStatus) GetMessage() string {
	return err.Message
}

func (err *ErrRpcStatus) Error() string {
	codeName, ok := v2.Code_name[err.Code]
	if !ok {
		codeName = string(err.Code)
	}
	return fmt.Sprintf("CODE: %s, MESSAGE: %s", codeName, err.Message)
}

var _ = error(&ErrRpcStatus{})

func AsErrRpcStatus(err error) (*ErrRpcStatus, bool) {
	var target *ErrRpcStatus
	ok := errors.As(err, &target)
	return target, ok
}

// rpcStatusError keeps the transport cause without changing ErrRpcStatus's public layout.
type rpcStatusError struct {
	*ErrRpcStatus
	cause error
}

func (err *rpcStatusError) Unwrap() error {
	return err.cause
}

func (err *rpcStatusError) As(target interface{}) bool {
	if rpcStatus, ok := target.(**ErrRpcStatus); ok {
		*rpcStatus = err.ErrRpcStatus
		return true
	}
	return false
}

func normalizeGrpcError(err error) error {
	if err == nil {
		return nil
	}
	grpcStatus, ok := status.FromError(err)
	if !ok || grpcStatus.Code() != codes.ResourceExhausted {
		return err
	}
	return &rpcStatusError{
		ErrRpcStatus: &ErrRpcStatus{
			Code:    int32(v2.Code_TOO_MANY_REQUESTS),
			Message: grpcStatus.Message(),
		},
		cause: err,
	}
}
